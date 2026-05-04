package org.taasim.flink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.io.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * TaaSim GPS Normalizer — Flink Job 1
 *
 * Pipeline:
 *   raw.gps (Kafka)
 *     → Parse JSON
 *     → Validate (Casablanca bbox, speed ≤ 150 km/h, valid taxi_id)
 *     → Watermark (3-min bounded out-of-orderness)
 *     → Deduplicate (keyed by taxi_id, 2 s window)
 *     → Zone-map (nearest centroid from zone_mapping.csv)
 *     → Anonymize (replace lat/lon with zone centroid)
 *     → Cassandra sink (vehicle_positions)
 *     → Kafka sink  (processed.gps)
 */
public class GpsNormalizer {

    // ── Casablanca bounding box ──────────────────────────────────────────
    private static final double CASA_LAT_MIN = 33.4;
    private static final double CASA_LAT_MAX = 33.7;
    private static final double CASA_LON_MIN = -7.8;
    private static final double CASA_LON_MAX = -7.4;
    private static final double MAX_SPEED     = 150.0;
    private static final long   DEDUP_MS      = 2_000; // 2 seconds

    // ── Data classes ─────────────────────────────────────────────────────

    /** Internal representation of a GPS event flowing through the pipeline. */
    public static class GpsEvent implements Serializable {
        private static final long serialVersionUID = 1L;
        public String  taxiId;
        public String  tripId;
        public long    timestamp;   // epoch-seconds
        public String  eventTime;   // ISO-8601
        public double  lat;
        public double  lon;
        public double  speed;
        public String  status;
        public boolean lateArrival;
        // enriched by zone mapper
        public String  city;
        public int     zoneId;
        public String  zoneName;
    }

    /** A Casablanca arrondissement loaded from zone_mapping.csv. */
    public static class Zone implements Serializable {
        private static final long serialVersionUID = 1L;
        public final int    zoneId;
        public final String zoneName;
        public final double centroidLat;
        public final double centroidLon;

        public Zone(int id, String name, double lat, double lon) {
            this.zoneId      = id;
            this.zoneName    = name;
            this.centroidLat = lat;
            this.centroidLon = lon;
        }
    }

    // ── 1. Parse JSON ────────────────────────────────────────────────────

    public static class ParseJson implements MapFunction<String, GpsEvent> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public GpsEvent map(String json) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                JsonNode n = mapper.readTree(json);
                GpsEvent e = new GpsEvent();
                e.taxiId     = n.path("taxi_id").asText(null);
                e.tripId     = n.path("trip_id").asText(null);
                e.timestamp  = n.path("timestamp").asLong(0);
                e.eventTime  = n.path("event_time").asText("");
                e.lat        = n.path("lat").asDouble(0);
                e.lon        = n.path("lon").asDouble(0);
                e.speed      = n.path("speed").asDouble(0);
                e.status     = n.path("status").asText("UNKNOWN");
                e.lateArrival = n.path("late_arrival").asBoolean(false);
                return e;
            } catch (Exception ex) {
                return null;   // will be filtered downstream
            }
        }
    }

    // ── 2. Validate ──────────────────────────────────────────────────────

    public static class ValidateGps implements FilterFunction<GpsEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(GpsEvent e) {
            if (e == null) return false;
            if (e.taxiId == null || e.taxiId.isEmpty()
                    || "unknown".equalsIgnoreCase(e.taxiId)) return false;
            if (e.timestamp <= 0) return false;
            // Casablanca bounding box
            if (e.lat < CASA_LAT_MIN || e.lat > CASA_LAT_MAX) return false;
            if (e.lon < CASA_LON_MIN || e.lon > CASA_LON_MAX) return false;
            // Speed sanity
            if (e.speed < 0 || e.speed > MAX_SPEED) return false;
            return true;
        }
    }

    // ── 3. Deduplicate (keyed state, 2 s window) ─────────────────────────

    public static class Deduplicate
            extends KeyedProcessFunction<String, GpsEvent, GpsEvent> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> lastTs;

        @Override
        public void open(Configuration params) {
            lastTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastTs", Long.class));
        }

        @Override
        public void processElement(GpsEvent e, Context ctx,
                                   Collector<GpsEvent> out) throws Exception {
            Long prev = lastTs.value();
            long cur  = e.timestamp * 1000;   // → millis
            if (prev == null || Math.abs(cur - prev) >= DEDUP_MS) {
                lastTs.update(cur);
                out.collect(e);
            }
        }
    }

    // ── 4. Zone mapping + anonymisation ──────────────────────────────────

    public static class ZoneMapAndAnonymize
            implements MapFunction<GpsEvent, GpsEvent>, Serializable {
        private static final long serialVersionUID = 1L;
        private final List<Zone> zones;

        public ZoneMapAndAnonymize(List<Zone> zones) {
            this.zones = zones;
        }

        @Override
        public GpsEvent map(GpsEvent e) {
            Zone nearest = findNearest(e.lat, e.lon);
            e.city     = "casablanca";
            e.zoneId   = nearest.zoneId;
            e.zoneName = nearest.zoneName;
            // ── Privacy: replace raw GPS with zone centroid ──
            e.lat = nearest.centroidLat;
            e.lon = nearest.centroidLon;
            return e;
        }

        private Zone findNearest(double lat, double lon) {
            Zone best     = zones.get(0);
            double bestD  = Double.MAX_VALUE;
            for (Zone z : zones) {
                double d = sq(lat - z.centroidLat) + sq(lon - z.centroidLon);
                if (d < bestD) { bestD = d; best = z; }
            }
            return best;
        }
        private static double sq(double v) { return v * v; }
    }

    // ── 5. Cassandra sink (using DataStax driver directly) ───────────────

    public static class CassandraWriter extends RichMapFunction<GpsEvent, GpsEvent> {
        private static final long serialVersionUID = 1L;
        private final String contactPoint;
        private final int    port;
        private transient CqlSession session;
        private transient PreparedStatement insertStmt;

        public CassandraWriter(String contactPoint, int port) {
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
                "INSERT INTO vehicle_positions "
              + "(city, zone_id, event_time, taxi_id, lat, lon, speed, status) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        }

        @Override
        public GpsEvent map(GpsEvent e) {
            session.execute(insertStmt.bind(
                    e.city,
                    e.zoneId,
                    Instant.ofEpochSecond(e.timestamp),
                    e.taxiId,
                    e.lat,
                    e.lon,
                    e.speed,
                    e.status));
            return e;     // pass-through so the stream continues to Kafka sink
        }

        @Override
        public void close() {
            if (session != null) session.close();
        }
    }

    // ── 6. Serialize to JSON for Kafka sink ──────────────────────────────

    public static class ToJson implements MapFunction<GpsEvent, String> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public String map(GpsEvent e) {
            if (mapper == null) mapper = new ObjectMapper();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("taxi_id",      e.taxiId);
            m.put("trip_id",      e.tripId);
            m.put("timestamp",    e.timestamp);
            m.put("event_time",   e.eventTime);
            m.put("lat",          e.lat);
            m.put("lon",          e.lon);
            m.put("speed",        e.speed);
            m.put("status",       e.status);
            m.put("city",         e.city);
            m.put("zone_id",      e.zoneId);
            m.put("zone_name",    e.zoneName);
            m.put("late_arrival", e.lateArrival);
            try {
                return mapper.writeValueAsString(m);
            } catch (Exception ex) {
                return "{}";
            }
        }
    }

    // ── Zone loader ──────────────────────────────────────────────────────

    private static List<Zone> loadZones() {
        List<Zone> zones = new ArrayList<>();
        try (InputStream is = GpsNormalizer.class
                .getResourceAsStream("/zone_mapping.csv");
             BufferedReader br = new BufferedReader(
                     new InputStreamReader(Objects.requireNonNull(is)))) {

            br.readLine();   // skip CSV header
            String line;
            while ((line = br.readLine()) != null) {
                // zone_id,zone_name,zone_type,population_density,centroid_lat,centroid_lon,adjacency_list
                String[] p = line.split(",");
                if (p.length >= 6) {
                    zones.add(new Zone(
                            Integer.parseInt(p[0].trim()),
                            p[1].trim(),
                            Double.parseDouble(p[4].trim()),
                            Double.parseDouble(p[5].trim())));
                }
            }
        } catch (Exception e) {
            System.err.println("[WARN] Could not load zone_mapping.csv from classpath — "
                    + "using hardcoded 16 Casablanca zones. " + e.getMessage());
            zones.clear();
            zones.add(new Zone( 1, "Anfa",            33.593, -7.632));
            zones.add(new Zone( 2, "Maarif",          33.585, -7.640));
            zones.add(new Zone( 3, "Sidi Belyout",    33.592, -7.618));
            zones.add(new Zone( 4, "El Fida",         33.570, -7.608));
            zones.add(new Zone( 5, "Mers Sultan",     33.575, -7.612));
            zones.add(new Zone( 6, "Ain Sebaa",       33.606, -7.540));
            zones.add(new Zone( 7, "Hay Mohammadi",   33.596, -7.570));
            zones.add(new Zone( 8, "Roches Noires",   33.600, -7.585));
            zones.add(new Zone( 9, "Hay Hassani",     33.560, -7.675));
            zones.add(new Zone(10, "Ain Chock",        33.542, -7.614));
            zones.add(new Zone(11, "Sidi Bernoussi",  33.590, -7.490));
            zones.add(new Zone(12, "Sidi Moumen",     33.575, -7.525));
            zones.add(new Zone(13, "Ben M'Sick",       33.555, -7.580));
            zones.add(new Zone(14, "Sbata",           33.550, -7.590));
            zones.add(new Zone(15, "Moulay Rachid",   33.540, -7.550));
            zones.add(new Zone(16, "Sidi Othmane",    33.545, -7.570));
        }
        System.out.println("[GPS-Normalizer] Loaded " + zones.size() + " zones");
        return zones;
    }

    // ══════════════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════════════

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ── Checkpointing ────────────────────────────────────────────────
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ── Config (can be overridden via --key value) ───────────────────
        String kafkaBrokers    = "kafka:19092";
        String cassandraHost   = "cassandra";
        int    cassandraPort   = 9042;
        String sourceTopic     = "raw.gps";
        String sinkTopic       = "processed.gps";
        String consumerGroup   = "flink-gps-normalizer";

        // ── Load zone mapping ────────────────────────────────────────────
        List<Zone> zones = loadZones();

        // ── Kafka Source ─────────────────────────────────────────────────
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setGroupId(consumerGroup)
                .setTopics(sourceTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "KafkaRawGPS");

        // ── Parse ────────────────────────────────────────────────────────
        DataStream<GpsEvent> parsed = raw.map(new ParseJson());

        // ── Validate ─────────────────────────────────────────────────────
        DataStream<GpsEvent> valid = parsed.filter(new ValidateGps());

        // ── Watermark (3-min bounded out-of-orderness) ───────────────────
        DataStream<GpsEvent> watermarked = valid
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<GpsEvent>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(3))
                                .withTimestampAssigner(
                                        (e, ts) -> e.timestamp * 1000));

        // ── Deduplicate (per taxi, 2 s) ──────────────────────────────────
        DataStream<GpsEvent> deduped = watermarked
                .keyBy(e -> e.taxiId)
                .process(new Deduplicate());

        // ── Zone-map + anonymise ─────────────────────────────────────────
        DataStream<GpsEvent> normalized = deduped
                .map(new ZoneMapAndAnonymize(zones));

        // ── Sink 1 → Cassandra (pass-through) ───────────────────────────
        DataStream<GpsEvent> persisted = normalized
                .map(new CassandraWriter(cassandraHost, cassandraPort));

        // ── Sink 2 → Kafka (processed.gps) ──────────────────────────────
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(
                                        new SimpleStringSchema())
                                .build())
                .build();

        persisted.map(new ToJson()).sinkTo(kafkaSink);

        // ── Go! ──────────────────────────────────────────────────────────
        env.execute("TaaSim GPS Normalizer");
    }
}
