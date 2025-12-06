import time
import socket
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# UDP socket for StatsD
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
STATSD_ADDR = ("127.0.0.1", 9125)

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    
    # Calculate duration in ms
    duration_ms = int((time.time() - start) * 1000)

    try:
        # Emit metrics in Gunicorn format
        sock.sendto(b"gunicorn.requests:1|c", STATSD_ADDR)
        sock.sendto(f"gunicorn.request.duration:{duration_ms}|ms".encode(), STATSD_ADDR)
    except Exception:
        pass
        
    return response

@app.get("/")
def read_root():
    # Simulate a fast response
    return {"status": "ok"}

@app.get("/slow")
def read_slow():
    # Simulate a slow response for certain load types
    time.sleep(0.5)
    return {"status": "slow"}
