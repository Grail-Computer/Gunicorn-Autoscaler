FROM rust:1.83-slim AS builder
WORKDIR /build

# Build the gunicorn-autoscaler binary
# Note: Copying root Cargo.toml/src relative to build context
COPY gunicorn-autoscaler/Cargo.toml gunicorn-autoscaler/
COPY gunicorn-autoscaler/src gunicorn-autoscaler/src
WORKDIR /build/gunicorn-autoscaler
RUN cargo build --release

FROM python:3.12-slim
WORKDIR /app

# Install dependencies
RUN pip install fastapi uvicorn gunicorn "uvicorn[standard]"

# Copy binary
COPY --from=builder /build/gunicorn-autoscaler/target/release/gunicorn-autoscaler /usr/local/bin/gunicorn-autoscaler

# Copy test app
COPY tests/app/main.py .

# Expose port
EXPOSE 8000

# Default CMD (can be overridden by test runner)
CMD ["gunicorn-autoscaler", "main:app", "--bind", "0.0.0.0:8000", "--worker-class", "uvicorn.workers.UvicornWorker", "--log-level", "info"]
