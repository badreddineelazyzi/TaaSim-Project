from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import uuid
import json
from datetime import datetime
from cassandra.cluster import Cluster
from aiokafka import AIOKafkaProducer

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

from auth import create_access_token, RoleChecker, TokenData

app = FastAPI(title="TaaSim Demand Forecast API", version="1.0")

allow_all = RoleChecker(["admin", "rider"])
allow_admin_only = RoleChecker(["admin"])

cassandra_session = None
kafka_producer = None
spark = None
demand_model = None


class TripRequest(BaseModel):
    origin_zone: int
    destination_zone: int
    rider_id: str


class ForecastRequest(BaseModel):
    zone_id: int
    datetime: str  # Format: YYYY-MM-DD HH:MM:SS


class LoginRequest(BaseModel):
    username: str
    role: str


FEATURE_COLS = [
    "hour_of_day", "day_of_week", "is_weekend", "is_friday",
    "population_density", "is_residential", "is_commercial", "is_industrial", "is_transit_hub",
    "temperature_2m", "rain", "is_raining", "temp_cold", "temp_hot", "temp_mild",
    "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean", "zone_id"
]


@app.on_event("startup")
async def startup_event():
    global cassandra_session, kafka_producer, spark, demand_model

    try:
        cluster = Cluster(['taasim-cassandra'], port=9042)
        cassandra_session = cluster.connect()
        cassandra_session.set_keyspace('taasim')
    except Exception as e:
        print(f"Warning: Cassandra connection failed - {e}")

    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers='taasim-kafka:19092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await kafka_producer.start()
    except Exception as e:
        print(f"Warning: Kafka connection failed - {e}")

    try:
        spark = SparkSession.builder \
            .appName("FastAPI_Forecast") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
            .getOrCreate()

        model_path = "/app/models/demand_v1"
        demand_model = PipelineModel.load(model_path)
    except Exception as e:
        print(f"Warning: Spark/Model loading failed - {e}")


@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        await kafka_producer.stop()
    if cassandra_session:
        cassandra_session.cluster.shutdown()
    if spark:
        spark.stop()


@app.post("/auth/token", tags=["Authentication"])
async def login(request: LoginRequest):
    if request.role not in ["admin", "rider"]:
        raise HTTPException(status_code=400, detail="Role must be admin or rider")

    access_token = create_access_token(data={"sub": request.username, "role": request.role})
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/api/v1/trips", dependencies=[Depends(allow_all)], tags=["Trips"])
async def create_trip(trip: TripRequest):
    trip_id = str(uuid.uuid4())
    event = {
        "trip_id": trip_id,
        "origin_zone": trip.origin_zone,
        "destination_zone": trip.destination_zone,
        "rider_id": trip.rider_id,
        "event_time": datetime.utcnow().isoformat(),
        "status": "pending"
    }

    if kafka_producer:
        await kafka_producer.send_and_wait("raw.trips", event)

    return {"trip_id": trip_id, "status": "pending"}


@app.get("/api/v1/trips/{trip_id}", dependencies=[Depends(allow_all)], tags=["Trips"])
async def get_trip_status(trip_id: str):
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Database not connected")

    query = "SELECT status, eta FROM trips WHERE trip_id = %s"
    row = cassandra_session.execute(query, (trip_id,)).one()

    if not row:
        raise HTTPException(status_code=404, detail="Trip not found")

    return {"trip_id": trip_id, "status": row.status, "eta": row.eta}


@app.get("/api/v1/vehicles/zone/{zone_id}", dependencies=[Depends(allow_admin_only)], tags=["Admin"])
async def get_vehicles_in_zone(zone_id: int):
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Database not connected")

    query = "SELECT vehicle_id, location, event_time FROM vehicle_positions WHERE zone_id = %s"
    rows = cassandra_session.execute(query, (zone_id,))

    vehicles = []
    now = datetime.utcnow()

    for row in rows:
        if (now - row.event_time).total_seconds() <= 30:
            vehicles.append({
                "vehicle_id": row.vehicle_id,
                "location": row.location
            })

    return {"zone_id": zone_id, "active_vehicles": vehicles}


@app.post("/api/v1/demand/forecast", dependencies=[Depends(allow_admin_only)], tags=["Admin"])
async def predict_demand(req: ForecastRequest):
    if not spark or not demand_model:
        raise HTTPException(status_code=503, detail="ML Model not loaded")

    dt = datetime.strptime(req.datetime, "%Y-%m-%d %H:%M:%S")
    time_slot = dt.strftime("%Y-%m-%d %H:00:00")

    # Load latest features from S3 feature store for the requested zone
    try:
        features_df = spark.read.parquet("s3a://ml-data/features/")
        zone_features = features_df.filter(F.col("zone_id") == req.zone_id)
        latest = zone_features.orderBy(F.col("time_slot_30min").desc()).first()
    except Exception as e:
        print(f"Warning: Could not load features from S3 ({e}), using fallback defaults")
        latest = None

    if latest is not None:
        feature_values = [
            dt.hour,                                     # hour_of_day
            dt.weekday(),                                # day_of_week
            1 if dt.weekday() >= 5 else 0,               # is_weekend
            1 if dt.weekday() == 4 else 0,               # is_friday
            float(latest["population_density"]),          # population_density
            float(latest["is_residential"]),              # is_residential
            float(latest["is_commercial"]),               # is_commercial
            float(latest["is_industrial"]),               # is_industrial
            float(latest["is_transit_hub"]),              # is_transit_hub
            float(latest["temperature_2m"]),              # temperature_2m
            float(latest["rain"]),                        # rain
            float(latest["is_raining"]),                  # is_raining
            float(latest["temp_cold"]),                   # temp_cold
            float(latest["temp_hot"]),                    # temp_hot
            float(latest["temp_mild"]),                   # temp_mild
            float(latest["demand_lag_1d"]),               # demand_lag_1d
            float(latest["demand_lag_7d"]),               # demand_lag_7d
            float(latest["rolling_7d_mean"]),             # rolling_7d_mean
            float(req.zone_id)                            # zone_id
        ]
    else:
        feature_values = [
            dt.hour,
            dt.weekday(),
            1 if dt.weekday() >= 5 else 0,
            1 if dt.weekday() == 4 else 0,
            5000.0,
            1,
            0,
            0,
            0,
            20.0,
            0.0,
            0,
            0,
            0,
            1,
            10.0,
            12.0,
            11.5,
            float(req.zone_id)
        ]

    row_dict = {FEATURE_COLS[i]: feature_values[i] for i in range(len(FEATURE_COLS))}
    pred_row = Row(**row_dict)
    df = spark.createDataFrame([pred_row])
    predictions = demand_model.transform(df)
    predicted_demand = predictions.select("prediction").first()[0]
    predicted_demand = round(float(predicted_demand), 2)

    if cassandra_session:
        try:
            cassandra_session.execute(
                "INSERT INTO demand_zones (city, zone_id, window_start, forecasted_demand, active_vehicles, pending_requests, ratio) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                ("casablanca", req.zone_id, dt, predicted_demand, 0, 0, 0.0)
            )
        except Exception as e:
            print(f"Warning: Failed to insert forecast into Cassandra - {e}")

    return {
        "zone_id": req.zone_id,
        "datetime": req.datetime,
        "predicted_demand": predicted_demand
    }
