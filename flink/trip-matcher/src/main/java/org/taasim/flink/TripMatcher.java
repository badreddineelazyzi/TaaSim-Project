package org.taasim.flink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.io.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TripMatcher {

    private static final double AVG_SPEED_KMH = 25.0;
    private static final double EARTH_RADIUS_KM = 6371.0;

    public static class Zone implements Serializable {
        public final int id;
        public final double centroidLat;
        public final double centroidLon;
        public final List<Integer> adjacent;

        public Zone(int id, double lat, double lon, List<Integer> adj) {
            this.id = id;
            this.centroidLat = lat;
            this.centroidLon = lon;
            this.adjacent = adj;
        }
    }

    public static class VehicleInfo implements Serializable {
        public String taxiId;
        public double lat;
        public double lon;
        public long lastSeen;
    }

    public static class GpsUpdate implements Serializable {
        public int zoneId;
        public String taxiId;
        public double lat;
        public double lon;
        public long timestamp;
        public String status;
    }

    public static class TripRequest implements Serializable {
        public String tripId;
        public int originZone;
        public int destZone;
        public String riderId;
        public double fare;
        public long timestamp;
    }

    public static class MatchResult implements Serializable {
        public String tripId;
        public String taxiId;
        public int originZone;
        public int destZone;
        public String riderId;
        public double fare;
        public long timestamp;
        public int etaSeconds;
        public boolean matched;
        public int matchedZone;
    }

    public static class ParseGpsUpdate implements MapFunction<String, GpsUpdate> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public GpsUpdate map(String json) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                JsonNode n = mapper.readTree(json);
                GpsUpdate g = new GpsUpdate();
                g.zoneId = n.path("zone_id").asInt(0);
                g.taxiId = n.path("taxi_id").asText("");
                g.lat = n.path("lat").asDouble(0);
                g.lon = n.path("lon").asDouble(0);
                g.timestamp = n.path("timestamp").asLong(0) * 1000;
                g.status = n.path("status").asText("");
                return g;
            } catch (Exception ex) {
                return null;
            }
        }
    }

    public static class ParseTripRequest implements MapFunction<String, TripRequest> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public TripRequest map(String json) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                JsonNode n = mapper.readTree(json);
                TripRequest t = new TripRequest();
                t.tripId = n.path("trip_id").asText("");
                t.originZone = n.path("origin_zone").asInt(0);
                t.destZone = n.path("destination_zone").asInt(0);
                t.riderId = n.path("rider_id").asText("");
                t.fare = n.path("fare").asDouble(0);
                t.timestamp = n.path("requested_at").asLong(0) * 1000;
                return t;
            } catch (Exception ex) {
                return null;
            }
        }
    }

    public static class TripMatcherFn
            extends KeyedCoProcessFunction<String, GpsUpdate, TripRequest, MatchResult> {
        private static final long serialVersionUID = 1L;
        private transient MapState<Integer, Map<String, VehicleInfo>> vehiclesByZone;
        private transient MapState<String, TripRequest> pendingTrips;
        private transient Map<Integer, Zone> zoneMap;
        private final List<Zone> zones;

        public TripMatcherFn(List<Zone> zones) {
            this.zones = zones;
        }

        @Override
        public void open(Configuration params) {
            vehiclesByZone = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("vehiclesByZone",
                            Types.INT, Types.MAP(Types.STRING, Types.POJO(VehicleInfo.class))));

            pendingTrips = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("pendingTrips",
                            Types.STRING, Types.POJO(TripRequest.class)));

            zoneMap = new HashMap<>();
            for (Zone z : zones) {
                zoneMap.put(z.id, z);
            }
        }

        @Override
        public void processElement1(GpsUpdate g, Context ctx, Collector<MatchResult> out) throws Exception {
            if (g == null || g.taxiId.isEmpty() || g.zoneId <= 0) return;

            Map<String, VehicleInfo> zoneVehicles = vehiclesByZone.get(g.zoneId);
            if (zoneVehicles == null) {
                zoneVehicles = new HashMap<>();
            }

            boolean isAvailable = "AVAILABLE".equalsIgnoreCase(g.status)
                    || "FREE".equalsIgnoreCase(g.status)
                    || "IDLE".equalsIgnoreCase(g.status);

            if (isAvailable) {
                VehicleInfo v = new VehicleInfo();
                v.taxiId = g.taxiId;
                v.lat = g.lat;
                v.lon = g.lon;
                v.lastSeen = g.timestamp;
                zoneVehicles.put(g.taxiId, v);
            } else {
                zoneVehicles.remove(g.taxiId);
            }

            vehiclesByZone.put(g.zoneId, zoneVehicles);

            tryMatchPendingTrips(g.zoneId, out);
        }

        @Override
        public void processElement2(TripRequest t, Context ctx, Collector<MatchResult> out) throws Exception {
            if (t == null || t.tripId.isEmpty() || t.originZone <= 0) return;

            MatchResult r = new MatchResult();
            r.tripId = t.tripId;
            r.originZone = t.originZone;
            r.destZone = t.destZone;
            r.riderId = t.riderId;
            r.fare = t.fare;
            r.timestamp = t.timestamp;
            r.matched = false;

            String matchedTaxi = null;
            int matchedZone = t.originZone;

            Set<Integer> searchedZones = new LinkedHashSet<>();
            searchedZones.add(t.originZone);

            Zone originZ = zoneMap.get(t.originZone);
            if (originZ != null && originZ.adjacent != null) {
                searchedZones.addAll(originZ.adjacent);
            }

            for (int zid : searchedZones) {
                matchedTaxi = findVehicleInZone(zid);
                if (matchedTaxi != null) {
                    matchedZone = zid;
                    break;
                }
            }

            if (matchedTaxi != null) {
                r.taxiId = matchedTaxi;
                r.matched = true;
                r.matchedZone = matchedZone;

                VehicleInfo v = getVehicle(matchedZone, matchedTaxi);
                if (v != null) {
                    Zone destZ = zoneMap.get(t.destZone);
                    if (destZ != null) {
                        double dist = haversine(v.lat, v.lon, destZ.centroidLat, destZ.centroidLon);
                        r.etaSeconds = (int) Math.round((dist / AVG_SPEED_KMH) * 3600);
                    }
                }

                Map<String, VehicleInfo> zv = vehiclesByZone.get(matchedZone);
                if (zv != null) {
                    zv.remove(matchedTaxi);
                    vehiclesByZone.put(matchedZone, zv);
                }
            } else {
                pendingTrips.put(t.tripId, t);
                ctx.timerService().registerEventTimeTimer(t.timestamp + 5000);
            }

            out.collect(r);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MatchResult> out) throws Exception {
            List<String> expired = new ArrayList<>();
            for (Map.Entry<String, TripRequest> e : pendingTrips.entries()) {
                TripRequest t = e.getValue();
                if (t.timestamp + 5000 <= timestamp) {
                    expired.add(e.getKey());
                    MatchResult r = new MatchResult();
                    r.tripId = t.tripId;
                    r.originZone = t.originZone;
                    r.destZone = t.destZone;
                    r.riderId = t.riderId;
                    r.fare = t.fare;
                    r.timestamp = t.timestamp;
                    r.matched = false;
                    out.collect(r);
                }
            }
            for (String id : expired) {
                pendingTrips.remove(id);
            }
        }

        private void tryMatchPendingTrips(int zoneId, Collector<MatchResult> out) throws Exception {
            Set<Integer> searchZones = new HashSet<>();
            searchZones.add(zoneId);
            Zone z = zoneMap.get(zoneId);
            if (z != null && z.adjacent != null) {
                searchZones.addAll(z.adjacent);
            }

            List<String> matchedIds = new ArrayList<>();
            for (Map.Entry<String, TripRequest> e : pendingTrips.entries()) {
                TripRequest t = e.getValue();
                if (!searchZones.contains(t.originZone)) continue;

                String taxiId = findVehicleInZone(zoneId);
                if (taxiId == null) continue;

                matchedIds.add(e.getKey());
                MatchResult r = new MatchResult();
                r.tripId = t.tripId;
                r.taxiId = taxiId;
                r.originZone = t.originZone;
                r.destZone = t.destZone;
                r.riderId = t.riderId;
                r.fare = t.fare;
                r.timestamp = t.timestamp;
                r.matched = true;
                r.matchedZone = zoneId;

                VehicleInfo v = getVehicle(zoneId, taxiId);
                if (v != null) {
                    Zone destZ = zoneMap.get(t.destZone);
                    if (destZ != null) {
                        double dist = haversine(v.lat, v.lon, destZ.centroidLat, destZ.centroidLon);
                        r.etaSeconds = (int) Math.round((dist / AVG_SPEED_KMH) * 3600);
                    }
                }

                Map<String, VehicleInfo> zv = vehiclesByZone.get(zoneId);
                if (zv != null) {
                    zv.remove(taxiId);
                    vehiclesByZone.put(zoneId, zv);
                }

                out.collect(r);
            }
            for (String id : matchedIds) {
                pendingTrips.remove(id);
            }
        }

        private String findVehicleInZone(int zoneId) throws Exception {
            Map<String, VehicleInfo> zv = vehiclesByZone.get(zoneId);
            if (zv == null || zv.isEmpty()) return null;

            String oldest = null;
            long oldestTs = Long.MAX_VALUE;
            for (Map.Entry<String, VehicleInfo> e : zv.entrySet()) {
                if (e.getValue().lastSeen < oldestTs) {
                    oldestTs = e.getValue().lastSeen;
                    oldest = e.getKey();
                }
            }
            return oldest;
        }

        private VehicleInfo getVehicle(int zoneId, String taxiId) throws Exception {
            Map<String, VehicleInfo> zv = vehiclesByZone.get(zoneId);
            if (zv == null) return null;
            return zv.get(taxiId);
        }
    }

    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                 * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_KM * c;
    }

    public static class ToMatchJson implements MapFunction<MatchResult, String> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public String map(MatchResult r) {
            if (mapper == null) mapper = new ObjectMapper();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("trip_id", r.tripId);
            m.put("taxi_id", r.taxiId != null ? r.taxiId : "");
            m.put("origin_zone", r.originZone);
            m.put("dest_zone", r.destZone);
            m.put("rider_id", r.riderId);
            m.put("fare", r.fare);
            m.put("eta_seconds", r.etaSeconds);
            m.put("matched", r.matched);
            m.put("matched_zone", r.matchedZone);
            m.put("timestamp", r.timestamp);
            m.put("event_time", Instant.ofEpochMilli(r.timestamp).toString());
            try {
                return mapper.writeValueAsString(m);
            } catch (Exception ex) {
                return "{}";
            }
        }
    }

    public static class CassandraTripWriter extends RichMapFunction<MatchResult, MatchResult> {
        private static final long serialVersionUID = 1L;
        private final String contactPoint;
        private final int port;
        private transient CqlSession session;
        private transient PreparedStatement insertStmt;

        public CassandraTripWriter(String contactPoint, int port) {
            this.contactPoint = contactPoint;
            this.port = port;
        }

        @Override
        public void open(Configuration params) {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(contactPoint, port))
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace("taasim")
                    .build();
            insertStmt = session.prepare(
                "INSERT INTO trips "
              + "(city, date_bucket, created_at, trip_id, taxi_id, origin_zone, dest_zone, "
              + " rider_id, fare, status, eta_seconds) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        }

        @Override
        public MatchResult map(MatchResult r) {
            java.util.UUID tripUuid;
            try {
                tripUuid = java.util.UUID.fromString(r.tripId);
            } catch (Exception e) {
                return r;
            }
            if (r.matched) {
                Instant ts = Instant.ofEpochMilli(r.timestamp);
                String dateBucket = ts.atZone(ZoneId.of("UTC"))
                        .toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
                session.execute(insertStmt.bind(
                        "casablanca",
                        dateBucket,
                        ts,
                        tripUuid,
                        r.taxiId,
                        r.originZone,
                        r.destZone,
                        r.riderId,
                        r.fare,
                        "matched",
                        r.etaSeconds));
            } else {
                Instant ts = Instant.ofEpochMilli(r.timestamp);
                String dateBucket = ts.atZone(ZoneId.of("UTC"))
                        .toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
                session.execute(insertStmt.bind(
                        "casablanca",
                        dateBucket,
                        ts,
                        tripUuid,
                        "",
                        r.originZone,
                        r.destZone,
                        r.riderId,
                        r.fare,
                        "unmatched",
                        0));
            }
            return r;
        }

        @Override
        public void close() {
            if (session != null) session.close();
        }
    }

    private static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        fields.add(cur.toString());
        return fields.toArray(new String[0]);
    }

    private static List<Zone> loadZones() {
        List<Zone> zones = new ArrayList<>();
        try (InputStream is = TripMatcher.class
                .getResourceAsStream("/zone_mapping.csv");
             BufferedReader br = new BufferedReader(
                     new InputStreamReader(Objects.requireNonNull(is)))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = parseCsvLine(line);
                if (p.length >= 7) {
                    int id = Integer.parseInt(p[0].trim());
                    double lat = Double.parseDouble(p[4].trim());
                    double lon = Double.parseDouble(p[5].trim());
                    String adjStr = p[6].trim();
                    adjStr = adjStr.replaceAll("[\\[\\]]", "");
                    List<Integer> adj = new ArrayList<>();
                    for (String s : adjStr.split(",")) {
                        s = s.trim();
                        if (!s.isEmpty()) adj.add(Integer.parseInt(s));
                    }
                    zones.add(new Zone(id, lat, lon, adj));
                }
            }
        } catch (Exception e) {
            System.err.println("[WARN] Could not load zone_mapping.csv: " + e.getMessage());
            e.printStackTrace();
            for (int i = 1; i <= 16; i++) {
                zones.add(new Zone(i, 33.56, -7.60, new ArrayList<>()));
            }
        }
        System.out.println("[TripMatcher] Loaded " + zones.size() + " zones with adjacency");
        return zones;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String kafkaBrokers = "kafka:19092";
        String cassandraHost = "cassandra";
        int cassandraPort = 9042;
        String gpsTopic = "processed.gps";
        String tripsTopic = "raw.trips";
        String matchesTopic = "processed.matches";
        String unmatchedTopic = "processed.unmatched";
        String consumerGroup = "flink-trip-matcher";

        List<Zone> zones = loadZones();

        WatermarkStrategy<GpsUpdate> gpsWatermark = WatermarkStrategy
                .<GpsUpdate>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((g, ts) -> g.timestamp)
                .withIdleness(Duration.ofSeconds(30));

        WatermarkStrategy<TripRequest> tripWatermark = WatermarkStrategy
                .<TripRequest>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((t, ts) -> t.timestamp)
                .withIdleness(Duration.ofSeconds(30));

        KafkaSource<String> gpsSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setGroupId(consumerGroup)
                .setTopics(gpsTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<GpsUpdate> gpsStream = env
                .fromSource(gpsSource, WatermarkStrategy.noWatermarks(), "KafkaGPS")
                .map(new ParseGpsUpdate())
                .filter(g -> g != null && !g.taxiId.isEmpty())
                .assignTimestampsAndWatermarks(gpsWatermark);

        KafkaSource<String> tripsSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setGroupId(consumerGroup)
                .setTopics(tripsTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<TripRequest> tripStream = env
                .fromSource(tripsSource, WatermarkStrategy.noWatermarks(), "KafkaTrips")
                .map(new ParseTripRequest())
                .filter(t -> t != null && !t.tripId.isEmpty())
                .assignTimestampsAndWatermarks(tripWatermark);

        DataStream<MatchResult> matches = gpsStream
                .keyBy(g -> "global")
                .connect(tripStream.keyBy(t -> "global"))
                .process(new TripMatcherFn(zones));

        DataStream<MatchResult> matched = matches.filter(r -> r.matched);
        DataStream<MatchResult> unmatched = matches.filter(r -> !r.matched);

        DataStream<MatchResult> persisted = matched.map(
                new CassandraTripWriter(cassandraHost, cassandraPort));

        KafkaSink<String> matchSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(matchesTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        KafkaSink<String> unmatchedSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(unmatchedTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        persisted.map(new ToMatchJson()).sinkTo(matchSink);
        unmatched.map(new ToMatchJson()).sinkTo(unmatchedSink);

        env.execute("TaaSim Trip Matcher");
    }
}
