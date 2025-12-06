# Gunicorn-autoscaler

**gunicorn-autoscaler** is a lightweight Rust wrapper for Gunicorn that provides **autoscaling** capabilities for FastAPI web applications.

It manages Gunicorn processes and listens to StatsD metrics to dynamicially add or remove workers based on real-time request pressure, without needing a full redeploy or complex orchestrator rules.

## Inspiration & Use Case

This tool was built to solve a specific problem on **Railway** (and similar PaaS providers):

- **Single-node scalability:** I wanted a single service to handle variable load without paying for over-provisioned resources.
- **Cost efficiency:** When traffic drops, the service should contract to minimal resources. When traffic spikes, it should instantly burst up.
- **Simplicity:** No K8s HPA or complex external monitoring hooks—just a binary that watches metrics and manages the process.

> [!WARNING] > **Production Note:** This tool was optimized for a specific single-node PaaS use case. While robust, it makes opinionated choices (like using signals for scaling). If you are on Kubernetes, horizontal pod autoscaling (HPA) is usually the preferred "cloud-native" scaling method. Use this if you need **vertical autoscaling within a single container/node**.

## Features

- **Autoscaling**: scales workers up/down based on RPS and Request Duration (p95).
- **Burst Mode**: instantly adds workers during sudden traffic spikes.
- **Zero-downtime**: uses standard Unix signals (`TTIN`, `TTOU`) to manage workers.
- **Single Binary**: ships as a static Rust binary alongside your Python app.
- **Works with Uvicorn**: supports `uvicorn.workers.UvicornWorker` out of the box.

## Usage

### 1. Install

Download the binary or build from source:

```bash
cargo install gunicorn-autoscaler
```

### 2. Configure (Environment Variables)

| Variable                           | Default          | Description                                |
| ---------------------------------- | ---------------- | ------------------------------------------ |
| `GUNICORN_AUTOSCALER_MIN_WORKERS`  | `2`              | Minimum workers to keep alive              |
| `GUNICORN_AUTOSCALER_MAX_WORKERS`  | `2*cores + 1`    | Maximum workers cap                        |
| `GUNICORN_AUTOSCALER_SLO_P95_MS`   | `300`            | Target p95 latency in ms                   |
| `GUNICORN_AUTOSCALER_IDLE_SECONDS` | `60`             | Seconds of low traffic before scaling down |
| `GUNICORN_AUTOSCALER_STATSD_ADDR`  | `127.0.0.1:9125` | StatsD listener address                    |

### 3. Run

Replace your standard `gunicorn` command with `gunicorn-autoscaler`:

```bash
gunicorn-autoscaler myapp:app --bind 0.0.0.0:8000 --worker-class uvicorn.workers.UvicornWorker
```

### ⚠️ Uvicorn & StatsD Requirement

If you are using `UvicornWorker`, you **must** emit StatsD metrics from your application manually, because Uvicorn bypasses Gunicorn's metric tracking.

Add this middleware to your FastAPI/Starlette app:

```python
import socket
import time
from fastapi import Request

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration_ms = int((time.time() - start) * 1000)

    # Emit metrics in Gunicorn format
    try:
        sock.sendto(b"gunicorn.requests:1|c", ("127.0.0.1", 9125))
        sock.sendto(f"gunicorn.request.duration:{duration_ms}|ms".encode(), ("127.0.0.1", 9125))
    except:
        pass

    return response
```

## Development & Testing

This repository includes a Docker-based integration test suite to verify autoscaling behavior (burst up and idle down).

### Prerequisites

- Docker
- Python 3 + `uv`

### Running Tests

The test runner builds the container, runs both "burst" and "idle" scenarios, and verifies log output.

```bash
# Create venv and install dependency
uv venv
source .venv/bin/activate
uv pip install httpx

# Run full suite
python3 tests/run_tests.py
```
