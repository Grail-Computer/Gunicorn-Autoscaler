import subprocess
import time
import sys
import threading
import os
import shutil
import httpx

# Configuration
IMAGE_NAME = "guni-test-image"
CONTAINER_NAME = "guni-test-runner"
PORT = 8010

def log(msg):
    print(f"[TEST] {msg}")

def run_cmd(cmd, check=True):
    log(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=check)

def build_image():
    log("Building Docker image...")
    # Build from root context
    run_cmd(f"docker build -t {IMAGE_NAME} -f tests/Dockerfile .")

def start_container(env_vars=None):
    log("Starting container...")
    env_args = ""
    if env_vars:
        for k, v in env_vars.items():
            env_args += f" -e {k}={v}"
            
    # Always remove old container
    subprocess.run(f"docker rm -f {CONTAINER_NAME}", shell=True, stderr=subprocess.DEVNULL)
    
    cmd = (
        f"docker run -d --name {CONTAINER_NAME} {env_args} "
        f"-p {PORT}:8000 {IMAGE_NAME}"
    )
    run_cmd(cmd)
    
    # Wait for healthy start
    time.sleep(3)
    
def stop_container():
    log("Stopping container...")
    subprocess.run(f"docker rm -f {CONTAINER_NAME}", shell=True, stdout=subprocess.DEVNULL)

def get_logs():
    res = subprocess.run(f"docker logs {CONTAINER_NAME}", shell=True, capture_output=True, text=True)
    return res.stderr + res.stdout

def generate_load(duration_sec, rps_target):
    log(f"Generating load: {rps_target} threads for {duration_sec}s")
    stop_event = threading.Event()
    
    def worker():
        client = httpx.Client()
        while not stop_event.is_set():
            try:
                client.get(f"http://127.0.0.1:{PORT}")
            except:
                pass
            time.sleep(1.0 / rps_target * 10) # rough sleep to throttle

    threads = []
    # Using threads as rough proxies for concurrent users to drive high RPS
    num_threads = rps_target // 5  # heuristic: 1 thread = ~5-10 RPS in tight loop
    if num_threads < 1: num_threads = 1
    
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
        
    time.sleep(duration_sec)
    stop_event.set()
    for t in threads:
        t.join()

def test_scale_up():
    log("\n=== TEST: Scale Up (Burst) ===")
    start_container({
        "GUNICORN_AUTOSCALER_MIN_WORKERS": "2",
        "GUNICORN_AUTOSCALER_MAX_WORKERS": "10",
        "GUNICORN_AUTOSCALER_BURST_ENABLED": "true",
        "RUST_LOG": "info"
    })
    
    try:
        # 1. Check initial state
        initial_logs = get_logs()
        if "Booting worker" not in initial_logs:
            raise Exception("Container failed to start properly")

        # 2. Blast load
        generate_load(duration_sec=10, rps_target=100)
        
        # 3. Check for scale up
        time.sleep(2) # Allow logs to flush
        logs = get_logs()
        
        if "Scale up" in logs or "BURST" in logs:
            log("✅ PASS: 'Scale up' or 'BURST' detected in logs.")
        else:
            print(logs)
            raise Exception("❌ FAIL: No scaling detected under load.")
            
    finally:
        stop_container()

def test_scale_down():
    log("\n=== TEST: Scale Down ===")
    
    # Configure fast downscale for testing
    start_container({
        "GUNICORN_AUTOSCALER_MIN_WORKERS": "2",
        "GUNICORN_AUTOSCALER_MAX_WORKERS": "10",
        "GUNICORN_AUTOSCALER_IDLE_SECONDS": "5",  # idle quickly
        "GUNICORN_AUTOSCALER_DOWN_COOLDOWN_MS": "1000",
        "GUNICORN_AUTOSCALER_NO_DOWNSCALE_MS_AFTER_UP": "1000",
        "RUST_LOG": "info"
    })
    
    try:
        # 1. Burst up first
        log("Bursting up first...")
        generate_load(duration_sec=5, rps_target=100)
        time.sleep(2)
        
        logs_mid = get_logs()
        if "Scale up" not in logs_mid and "BURST" not in logs_mid:
            log("⚠️ Warning: Didn't scale up enough to verify downscale properly, but continuing...")

        # 2. Wait for idle
        log("Waiting for idle (10s)...")
        time.sleep(10)
        
        # 3. Check for scale down
        logs_end = get_logs()
        if "Scale down" in logs_end or "Idle: scale down" in logs_end:
            log("✅ PASS: 'Scale down' detected.")
        else:
            # print(logs_end)
            raise Exception("❌ FAIL: No scale down detected after idle.")
            
    finally:
        stop_container()

if __name__ == "__main__":
    try:
        build_image()
        test_scale_up()
        test_scale_down()
        log("\n✅ ALL TESTS PASSED")
    except Exception as e:
        log(f"\n❌ TEST FAILED: {e}")
        stop_container()
        sys.exit(1)
