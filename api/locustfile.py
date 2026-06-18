from locust import HttpUser, task, between
from datetime import datetime

class FastAPIUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def test_forecast_endpoint(self):
        payload = {
            "zone_id": 1,
            "datetime": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.client.post("/api/v1/demand/forecast", json=payload)
