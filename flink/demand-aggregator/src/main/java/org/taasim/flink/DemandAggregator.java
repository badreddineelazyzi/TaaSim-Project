package org.taasim.flink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.io.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class DemandAggregator {

    public static class Zone implements Serializable {
        public final int id;
        public final double centroidLat;
        public final double centroidLon;

        public Zone(int id, double lat, double lon) {
            this.id = id;
            this.centroidLat = lat;
            this.centroidLon = lon;
        }
    }

    public static class DemandEvent implements Serializable {
        private static final long serialVersionUID = 1L;
        public int zoneId;
        public long timestamp;
        public boolean isGps;
        public String taxiId;
        public boolean isAvailable;
    }

    public static class TripEvent implements Serializable {
        public int originZone;
        public long timestamp;
    }

    public static class GpsEvent implements Serializable {
        public int zoneId;
        public long timestamp;
        public String taxiId;
        public String status;
    }

    public static class DemandResult implements Serializable {
        public int zoneId;
        public long windowStart;
        public int activeVehicles;
        public int pendingRequests;
        public double ratio;
        public double centroidLat;
        public double centroidLon;
    }

    public static class ParseGps implements MapFunction<String, GpsEvent> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public GpsEvent map(String json) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                JsonNode n = mapper.readTree(json);
                GpsEvent e = new GpsEvent();
                e.zoneId = n.path("zone_id").asInt(0);
                e.timestamp = n.path("timestamp").asLong(0) * 1000;
                e.taxiId = n.path("taxi_id").asText("");
                e.status = n.path("status").asText("");
                return e;
            } catch (Exception ex) {
                return null;
            }
        }
    }

    public static class GpsToDemand implements MapFunction<GpsEvent, DemandEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public DemandEvent map(GpsEvent e) {
            if (e == null) return null;
            DemandEvent d = new DemandEvent();
            d.zoneId = e.zoneId;
            d.timestamp = e.timestamp;
            d.isGps = true;
            d.taxiId = e.taxiId;
            d.isAvailable = "AVAILABLE".equalsIgnoreCase(e.status) || "FREE".equalsIgnoreCase(e.status) || "IDLE".equalsIgnoreCase(e.status);
            return d;
        }
    }

    public static class ParseTrip implements MapFunction<String, DemandEvent> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public DemandEvent map(String json) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                JsonNode n = mapper.readTree(json);
                DemandEvent d = new DemandEvent();
                d.zoneId = n.path("origin_zone").asInt(0);
                d.timestamp = n.path("requested_at").asLong(0) * 1000;
                d.isGps = false;
                d.taxiId = "";
                d.isAvailable = false;
                return d;
            } catch (Exception ex) {
                return null;
            }
        }
    }

    public static class DemandAggregatorFn
            extends ProcessWindowFunction<DemandEvent, DemandResult, Integer, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final Map<Integer, Zone> zoneMap;

        public DemandAggregatorFn(Map<Integer, Zone> zoneMap) {
            this.zoneMap = zoneMap;
        }

        @Override
        public void process(Integer key, Context ctx,
                            Iterable<DemandEvent> elements, Collector<DemandResult> out) {
            Set<String> activeTaxis = new HashSet<>();
            int pendingRequests = 0;
            for (DemandEvent e : elements) {
                if (e.isGps && e.isAvailable) {
                    activeTaxis.add(e.taxiId);
                } else if (!e.isGps) {
                    pendingRequests++;
                }
            }
            int activeCount = activeTaxis.size();
            double ratio = pendingRequests / Math.max(activeCount, 1.0);

            DemandResult r = new DemandResult();
            r.zoneId = key;
            r.windowStart = ctx.window().getStart();
            r.activeVehicles = activeCount;
            r.pendingRequests = pendingRequests;
            r.ratio = ratio;
            Zone z = zoneMap.get(key);
            if (z != null) {
                r.centroidLat = z.centroidLat;
                r.centroidLon = z.centroidLon;
            }
            out.collect(r);
        }
    }

    public static class ToDemandJson implements MapFunction<DemandResult, String> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public String map(DemandResult r) {
            if (mapper == null) mapper = new ObjectMapper();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("zone_id", r.zoneId);
            m.put("window_start", r.windowStart);
            m.put("active_vehicles", r.activeVehicles);
            m.put("pending_requests", r.pendingRequests);
            m.put("ratio", r.ratio);
            m.put("event_time", Instant.ofEpochMilli(r.windowStart).toString());
            try {
                return mapper.writeValueAsString(m);
            } catch (Exception ex) {
                return "{}";
            }
        }
    }

    public static class CassandraDemandWriter extends RichMapFunction<DemandResult, DemandResult> {
        private static final long serialVersionUID = 1L;
        private final String contactPoint;
        private final int port;
        private transient CqlSession session;
        private transient PreparedStatement insertStmt;

        public CassandraDemandWriter(String contactPoint, int port) {
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
                "INSERT INTO demand_zones "
              + "(city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecasted_demand, centroid_lat, centroid_lon) "
              + "VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)");
        }

        @Override
        public DemandResult map(DemandResult r) {
            session.execute(insertStmt.bind(
                    "casablanca",
                    r.zoneId,
                    Instant.ofEpochMilli(r.windowStart),
                    r.activeVehicles,
                    r.pendingRequests,
                    r.ratio,
                    r.centroidLat,
                    r.centroidLon));
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

    private static Map<Integer, Zone> loadZoneMap() {
        Map<Integer, Zone> zoneMap = new HashMap<>();
        try (InputStream is = DemandAggregator.class
                .getResourceAsStream("/zone_mapping.csv");
             BufferedReader br = new BufferedReader(
                     new InputStreamReader(Objects.requireNonNull(is)))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = parseCsvLine(line);
                if (p.length >= 6) {
                    int id = Integer.parseInt(p[0].trim());
                    double lat = Double.parseDouble(p[4].trim());
                    double lon = Double.parseDouble(p[5].trim());
                    zoneMap.put(id, new Zone(id, lat, lon));
                }
            }
        } catch (Exception e) {
            System.err.println("[WARN] Could not load zone_mapping.csv: " + e.getMessage());
        }
        System.out.println("[DemandAggregator] Loaded " + zoneMap.size() + " zone centroids");
        return zoneMap;
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
        String outputTopic = "processed.demand";
        String consumerGroup = "flink-demand-aggregator";

        Map<Integer, Zone> zoneMap = loadZoneMap();

        WatermarkStrategy<DemandEvent> watermarkStrategy = WatermarkStrategy
                .<DemandEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((e, ts) -> e.timestamp)
                .withIdleness(Duration.ofSeconds(30));

        KafkaSource<String> gpsSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setGroupId(consumerGroup)
                .setTopics(gpsTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<DemandEvent> gpsStream = env
                .fromSource(gpsSource, WatermarkStrategy.noWatermarks(), "KafkaGPS")
                .map(new ParseGps())
                .map(new GpsToDemand())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        KafkaSource<String> tripsSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setGroupId(consumerGroup)
                .setTopics(tripsTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<DemandEvent> tripsStream = env
                .fromSource(tripsSource, WatermarkStrategy.noWatermarks(), "KafkaTrips")
                .map(new ParseTrip())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<DemandResult> demand = gpsStream
                .union(tripsStream)
                .filter(e -> e != null && e.zoneId > 0)
                .keyBy(e -> e.zoneId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new DemandAggregatorFn(zoneMap));

        DataStream<DemandResult> persisted = demand
                .map(new CassandraDemandWriter(cassandraHost, cassandraPort));

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        persisted.map(new ToDemandJson()).sinkTo(kafkaSink);

        env.execute("TaaSim Demand Aggregator");
    }
}
