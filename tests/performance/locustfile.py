"""
Locust Performance Tests — Distributed Sync System
Measures throughput and latency for all API endpoints under load.

Usage:
    locust -f tests/performance/locustfile.py --host=http://localhost:8001
    locust -f tests/performance/locustfile.py --host=http://localhost:8001 \
           --headless -u 50 -r 10 --run-time 60s --csv=results/perf
"""
import random
import string
from locust import HttpUser, task, between, events
from locust.env import Environment


def random_key(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def random_value():
    return random.choice([
        random.randint(1, 10000),
        random_key(16),
        {"nested": random.randint(1, 100)},
    ])


HEADERS = {"X-API-Key": "dev-secret-key-change-in-prod", "Content-Type": "application/json"}


class LockUser(HttpUser):
    """Simulates clients acquiring and releasing distributed locks."""
    wait_time = between(0.1, 0.5)
    weight = 3

    @task(5)
    def acquire_and_release_exclusive(self):
        lock_id = f"resource-{random.randint(1, 10)}"
        client_id = f"client-{self.environment.runner.user_count}"
        with self.client.post(
            "/lock/acquire",
            json={"lock_id": lock_id, "client_id": client_id, "lock_type": "exclusive"},
            headers=HEADERS,
            catch_response=True,
            name="lock/acquire [exclusive]",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") in ("acquired", "queued"):
                    resp.success()
                    # Release it
                    self.client.post(
                        "/lock/release",
                        json={"lock_id": lock_id, "client_id": client_id},
                        headers=HEADERS,
                        name="lock/release",
                    )

    @task(3)
    def acquire_shared(self):
        lock_id = f"shared-res-{random.randint(1, 5)}"
        client_id = f"reader-{random.randint(1, 20)}"
        with self.client.post(
            "/lock/acquire",
            json={"lock_id": lock_id, "client_id": client_id, "lock_type": "shared"},
            headers=HEADERS,
            catch_response=True,
            name="lock/acquire [shared]",
        ) as resp:
            if resp.status_code == 200:
                resp.success()
                self.client.post(
                    "/lock/release",
                    json={"lock_id": lock_id, "client_id": client_id},
                    headers=HEADERS,
                    name="lock/release",
                )

    @task(1)
    def check_lock_status(self):
        self.client.get("/lock/status", headers=HEADERS, name="lock/status")


class QueueUser(HttpUser):
    """Simulates producers and consumers on the distributed queue."""
    wait_time = between(0.05, 0.3)
    weight = 4

    @task(7)
    def produce_message(self):
        queue = f"queue-{random.randint(1, 3)}"
        self.client.post(
            "/queue/produce",
            json={
                "queue": queue,
                "payload": {"data": random_key(20), "value": random.randint(1, 1000)},
                "producer_id": f"producer-{random.randint(1, 5)}",
            },
            headers=HEADERS,
            name="queue/produce",
        )

    @task(5)
    def consume_and_ack(self):
        queue = f"queue-{random.randint(1, 3)}"
        with self.client.post(
            "/queue/consume",
            json={"queue": queue, "consumer_id": f"consumer-{random.randint(1, 5)}"},
            headers=HEADERS,
            catch_response=True,
            name="queue/consume",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok" and data.get("message"):
                    msg_id = data["message"]["message_id"]
                    self.client.post(
                        "/queue/ack",
                        json={"queue": queue, "message_id": msg_id},
                        headers=HEADERS,
                        name="queue/ack",
                    )

    @task(1)
    def check_queue_status(self):
        self.client.get("/queue/status", headers=HEADERS, name="queue/status")


class CacheUser(HttpUser):
    """Simulates cache reads and writes with MESI coherence."""
    wait_time = between(0.02, 0.2)
    weight = 5

    KEYS = [f"cache-key-{i}" for i in range(50)]

    @task(8)
    def read_cache(self):
        key = random.choice(self.KEYS)
        self.client.get(f"/cache/{key}", headers=HEADERS, name="cache/read")

    @task(3)
    def write_cache(self):
        key = random.choice(self.KEYS)
        self.client.put(
            f"/cache/{key}",
            json={"value": random_value()},
            headers=HEADERS,
            name="cache/write",
        )

    @task(1)
    def invalidate_cache(self):
        key = random.choice(self.KEYS)
        self.client.delete(f"/cache/{key}", headers=HEADERS, name="cache/invalidate")

    @task(1)
    def cache_status(self):
        self.client.get("/cache/status", headers=HEADERS, name="cache/status")


class HealthCheckUser(HttpUser):
    """Lightweight health monitoring user."""
    wait_time = between(1, 5)
    weight = 1

    @task
    def health(self):
        self.client.get("/health", name="health")

    @task
    def metrics(self):
        self.client.get("/metrics", headers=HEADERS, name="metrics")
