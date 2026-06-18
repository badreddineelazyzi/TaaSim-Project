from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import uuid
import json
from datetime import datetime
from cassandra.cluster import Cluster
from aiokafka import AIOKafkaProducer

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml import PipelineModel

# --- Imports dial l'Security (mn fichier auth.py) ---
from auth import create_access_token, RoleChecker, TokenData

app = FastAPI(title="TaaSim Demand Forecast API", version="1.0")

# --- Configuration d l'Roles ---
allow_all = RoleChecker(["admin", "rider"])
allow_admin_only = RoleChecker(["admin"])

# Global variables for connections
cassandra_session = None
kafka_producer = None
spark = None
demand_model = None

# --- Models ---
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

# --- Startup & Shutdown ---
@app.on_event("startup")
async def startup_event():
    global cassandra_session, kafka_producer, spark, demand_model
    
    # 1. Connect to Cassandra
    try:
        cluster = Cluster(['taasim-cassandra'], port=9042)
        cassandra_session = cluster.connect()
        cassandra_session.set_keyspace('taasim')
    except Exception as e:
        print(f"Warning: Cassandra connection failed - {e}")

    # 2. Connect to Kafka
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers='taasim-kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await kafka_producer.start()
    except Exception as e:
        print(f"Warning: Kafka connection failed - {e}")

    # 3. Init PySpark & Load ML Model (Cached in memory)
    try:
        spark = SparkSession.builder \
            .appName("FastAPI_Forecast") \
            .master("local[*]") \
            .getOrCreate()
        
        # Loading from the local volume shared with Spark
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

# --- Security Endpoint (Generation d l'Token) ---

@app.post("/auth/token", tags=["Authentication"])
async def login(request: LoginRequest):
    if request.role not in ["admin", "rider"]:
        raise HTTPException(status_code=400, detail="Role must be admin or rider")
    
    access_token = create_access_token(data={"sub": request.username, "role": request.role})
    return {"access_token": access_token, "token_type": "bearer"}


# --- Protected Endpoints ---

# 1. Endpoint: Rider & Admin yqdro ykriyiw rri7la
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

# 2. Endpoint: Rider & Admin yqdro yshoufo status d rri7la
@app.get("/api/v1/trips/{trip_id}", dependencies=[Depends(allow_all)], tags=["Trips"])
async def get_trip_status(trip_id: str):
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Database not connected")
        
    query = "SELECT status, eta FROM trips WHERE trip_id = %s"
    row = cassandra_session.execute(query, (trip_id,)).one()
    
    if not row:
        raise HTTPException(status_code=404, detail="Trip not found")
        
    return {"trip_id": trip_id, "status": row.status, "eta": row.eta}

# 3. Endpoint: Ghir ADMIN li yqder yshouf l'taxis li mlou7in f l'zone
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

# 4. Endpoint: Ghir ADMIN li yqder ykheddem l'Modèle d'ML
@app.post("/api/v1/demand/forecast", dependencies=[Depends(allow_admin_only)], tags=["Admin"])
async def predict_demand(req: ForecastRequest):
    if not spark or not demand_model:
        raise HTTPException(status_code=503, detail="ML Model not loaded")
        
    dt = datetime.strptime(req.datetime, "%Y-%m-%d %H:%M:%S")
    
    # Mocking real-time features that should normally be pulled from Cache/DB
    features = Row(
        zone_id=req.zone_id,
        hour_of_day=dt.hour,
        day_of_week=dt.weekday(),
        is_weekend=1 if dt.weekday() >= 5 else 0,
        is_friday=1 if dt.weekday() == 4 else 0,
        population_density=5000.0,
        is_residential=1,
        is_commercial=0,
        is_industrial=0,
        is_transit_hub=0,
        temperature_2m=20.0,
        rain=0.0,
        is_raining=0,
        temp_cold=0,
        temp_hot=0,
        temp_mild=1,
        demand_lag_1d=10.0,
        demand_lag_7d=12.0,
        rolling_7d_mean=11.5
    )
    
    df = spark.createDataFrame([features])
    predictions = demand_model.transform(df)
    predicted_demand = predictions.select("prediction").first()[0]
    
    return {
        "zone_id": req.zone_id,
        "datetime": req.datetime,
        "predicted_demand": round(predicted_demand, 2)
    }