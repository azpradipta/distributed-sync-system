"""
Benchmark Scenarios — Single-Node vs Distributed Comparison
===========================================================
Measures throughput and latency across all system components.

Usage:
    # Start nodes first, then:
    python benchmarks/load_test_scenarios.py

    # Or run against a specific node:
    python benchmarks/load_test_scenarios.py --host http://localhost:8001
"""
import asyncio
import argparse
import statistics
import time
import random
import string
from typing import List, Tuple

import aiohttp

BASE_URL = "http://localhost:8001"
HEADERS = {"X-API-Key": "dev-secret-key-change-in-prod", "Content-Type": "application/json"}


def random_key(n=8):
    return "".join(random.choices(string.ascii_lowercase, k=n))


# ── Benchmark Helpers ────────────────────────────────────────────────────────

async def timed_request(session: aiohttp.ClientSession, method: str, url: str, **kwargs) -> Tuple[int, float]:
    """Returns (status_code, elapsed_ms)."""
    start = time.perf_counter()
    try:
        async with session.request(method, url, headers=HEADERS, **kwargs) as resp:
            await resp.read()
            elapsed = (time.perf_counter() - start) * 1000
            return resp.status, elapsed
    except Exception:
        return 0, (time.perf_counter() - start) * 1000


def print_results(name: str, latencies: List[float], errors: int, duration: float):
    if not latencies:
        print(f"  {name}: NO RESULTS (all errors)")
        return
    total = len(latencies) + errors
    throughput = len(latencies) / duration
    print(f"\n  📊 {name}")
    print(f"     Requests   : {total} ({errors} errors, {len(latencies)} success)")
    print(f"     Throughput : {throughput:.1f} req/s")
    print(f"     Latency p50: {statistics.median(latencies):.1f} ms")
    print(f"     Latency p95: {sorted(latencies)[int(len(latencies)*0.95)]:.1f} ms")
    print(f"     Latency p99: {sorted(latencies)[int(len(latencies)*0.99)]:.1f} ms")
    print(f"     Min/Max    : {min(latencies):.1f} / {max(latencies):.1f} ms")


# ── Scenario 1: Lock Throughput ───────────────────────────────────────────────

async def bench_lock_throughput(session: aiohttp.ClientSession, n: int = 200):
    print("\n🔒 Scenario 1: Distributed Lock Acquire/Release Throughput")
    latencies, errors = [], 0
    start = time.perf_counter()

    async def single_op():
        nonlocal errors
        lock_id = f"bench-lock-{random.randint(1, 20)}"
        client_id = f"bench-client-{random.randint(1, 50)}"
        status, lat = await timed_request(
            session, "POST", f"{BASE_URL}/lock/acquire",
            json={"lock_id": lock_id, "client_id": client_id, "lock_type": "exclusive"}
        )
        if status == 200:
            latencies.append(lat)
            await timed_request(session, "POST", f"{BASE_URL}/lock/release",
                                json={"lock_id": lock_id, "client_id": client_id})
        else:
            errors += 1

    await asyncio.gather(*[single_op() for _ in range(n)])
    print_results("Lock Acquire+Release", latencies, errors, time.perf_counter() - start)


# ── Scenario 2: Queue Throughput ──────────────────────────────────────────────

async def bench_queue_throughput(session: aiohttp.ClientSession, n: int = 300):
    print("\n📬 Scenario 2: Distributed Queue Produce/Consume Throughput")
    produce_lats, consume_lats, errors = [], [], 0
    start = time.perf_counter()

    # Produce phase
    async def produce():
        nonlocal errors
        status, lat = await timed_request(
            session, "POST", f"{BASE_URL}/queue/produce",
            json={"queue": "bench-q", "payload": {"data": random_key(32)}, "producer_id": "bench"}
        )
        if status == 200:
            produce_lats.append(lat)
        else:
            errors += 1

    await asyncio.gather(*[produce() for _ in range(n)])
    print_results("Queue Produce", produce_lats, errors, time.perf_counter() - start)

    # Consume phase
    errors = 0
    start2 = time.perf_counter()

    async def consume():
        nonlocal errors
        status, lat = await timed_request(
            session, "POST", f"{BASE_URL}/queue/consume",
            json={"queue": "bench-q", "consumer_id": "bench-consumer"}
        )
        if status == 200:
            consume_lats.append(lat)
        else:
            errors += 1

    await asyncio.gather(*[consume() for _ in range(n)])
    print_results("Queue Consume", consume_lats, errors, time.perf_counter() - start2)


# ── Scenario 3: Cache Read/Write ──────────────────────────────────────────────

async def bench_cache(session: aiohttp.ClientSession, n: int = 500):
    print("\n🗄️  Scenario 3: MESI Cache Read/Write Performance")
    write_lats, read_lats, errors = [], [], 0
    keys = [f"bench-{i}" for i in range(50)]

    # Warm up cache
    start = time.perf_counter()
    async def write():
        nonlocal errors
        key = random.choice(keys)
        status, lat = await timed_request(
            session, "PUT", f"{BASE_URL}/cache/{key}",
            json={"value": random.randint(1, 10000)}
        )
        if status == 200:
            write_lats.append(lat)
        else:
            errors += 1

    await asyncio.gather(*[write() for _ in range(n // 2)])
    print_results("Cache Write", write_lats, errors, time.perf_counter() - start)

    errors = 0
    start2 = time.perf_counter()
    async def read():
        nonlocal errors
        key = random.choice(keys)
        status, lat = await timed_request(session, "GET", f"{BASE_URL}/cache/{key}")
        if status == 200:
            read_lats.append(lat)
        else:
            errors += 1

    await asyncio.gather(*[read() for _ in range(n)])
    print_results("Cache Read", read_lats, errors, time.perf_counter() - start2)


# ── Scenario 4: Scalability — Concurrent Users ───────────────────────────────

async def bench_scalability(session: aiohttp.ClientSession):
    print("\n📈 Scenario 4: Scalability — Concurrent User Simulation")
    concurrency_levels = [1, 5, 10, 25, 50]
    print(f"  {'Concurrency':<15} {'Throughput':<15} {'p50 (ms)':<12} {'p99 (ms)'}")
    print(f"  {'-'*55}")

    for concurrency in concurrency_levels:
        lats = []
        start = time.perf_counter()

        async def op():
            key = random_key()
            status, lat = await timed_request(
                session, "PUT", f"{BASE_URL}/cache/{key}",
                json={"value": random.randint(1, 1000)}
            )
            if status == 200:
                lats.append(lat)

        await asyncio.gather(*[op() for _ in range(concurrency * 5)])
        elapsed = time.perf_counter() - start

        if lats:
            p50 = statistics.median(lats)
            p99 = sorted(lats)[int(len(lats)*0.99)] if len(lats) > 1 else lats[0]
            tput = len(lats) / elapsed
            print(f"  {concurrency:<15} {tput:<15.1f} {p50:<12.1f} {p99:.1f}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(host: str):
    global BASE_URL
    BASE_URL = host

    print("=" * 60)
    print("  Distributed Sync System — Performance Benchmark")
    print(f"  Target: {BASE_URL}")
    print("=" * 60)

    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Health check
        status, _ = await timed_request(session, "GET", f"{BASE_URL}/health")
        if status != 200:
            print(f"\n❌ Node unreachable at {BASE_URL}. Start the server first.")
            return

        await bench_lock_throughput(session)
        await bench_queue_throughput(session)
        await bench_cache(session)
        await bench_scalability(session)

    print("\n" + "=" * 60)
    print("  Benchmark complete!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Sync System Benchmark")
    parser.add_argument("--host", default="http://localhost:8001", help="Node base URL")
    args = parser.parse_args()
    asyncio.run(main(args.host))
