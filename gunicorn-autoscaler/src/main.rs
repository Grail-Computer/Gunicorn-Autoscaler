use anyhow::{anyhow, Context, Result};
use hdrhistogram::Histogram;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag as signal_flag;
use std::collections::VecDeque;
use std::env;
use std::fs;
use std::net::UdpSocket;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

static PIDFILE: &str = "/tmp/gunicorn.pid";

#[derive(Clone, Debug)]
struct Config {
    // statsd
    statsd_addr: String,    // 127.0.0.1:9125
    statsd_prefix: String,  // gunicorn

    // scaling
    min_workers: i32,
    max_workers: i32,
    window_seconds: u64,
    tick_ms: u64,
    idle_seconds: u64,
    up_cooldown_ms: u64,
    down_cooldown_ms: u64,
    max_step_up: i32,
    target_conc_per_worker: f64,
    slo_p95_ms: f64,
    no_downscale_for_ms_after_up: u64,

    // burst mode
    burst_enabled: bool,
    burst_window_seconds: u64,
    burst_rps_jump: f64,
    burst_add_workers: i32,
    burst_cooldown_ms: u64,
    burst_min_rps: f64,

    // UX / compat
    map_uvicorn_deprecated: bool,
    auto_set_graceful_timeout: bool,
}

impl Config {
    fn from_env() -> Config {
        let cores = effective_cores().unwrap_or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(|n| n.get() as f64)
                .unwrap_or(2.0)
        });
        let default_max = (2.0 * cores + 1.0).round() as i32;

        Config {
            statsd_addr: env_var("GUNICORN_AUTOSCALER_STATSD_ADDR", "127.0.0.1:9125"),
            statsd_prefix: env_var("GUNICORN_AUTOSCALER_STATSD_PREFIX", "gunicorn"),

            min_workers: env_var_i32("GUNICORN_AUTOSCALER_MIN_WORKERS", 2),
            max_workers: env_var_i32("GUNICORN_AUTOSCALER_MAX_WORKERS", default_max),
            window_seconds: env_var_u64("GUNICORN_AUTOSCALER_WINDOW_SECONDS", 20),
            tick_ms: env_var_u64("GUNICORN_AUTOSCALER_TICK_MS", 1000),
            idle_seconds: env_var_u64("GUNICORN_AUTOSCALER_IDLE_SECONDS", 60),
            up_cooldown_ms: env_var_u64("GUNICORN_AUTOSCALER_UP_COOLDOWN_MS", 2000),
            down_cooldown_ms: env_var_u64("GUNICORN_AUTOSCALER_DOWN_COOLDOWN_MS", 15000),
            max_step_up: env_var_i32("GUNICORN_AUTOSCALER_MAX_STEP_UP", 16),
            target_conc_per_worker: env_var_f64("GUNICORN_AUTOSCALER_TARGET_CONC_PER_WORKER", 25.0),
            slo_p95_ms: env_var_f64("GUNICORN_AUTOSCALER_SLO_P95_MS", 300.0),
            no_downscale_for_ms_after_up: env_var_u64("GUNICORN_AUTOSCALER_NO_DOWNSCALE_MS_AFTER_UP", 60000),

            burst_enabled: env_var_bool("GUNICORN_AUTOSCALER_BURST_ENABLED", true),
            burst_window_seconds: env_var_u64("GUNICORN_AUTOSCALER_BURST_WINDOW_SECONDS", 3),
            burst_rps_jump: env_var_f64("GUNICORN_AUTOSCALER_BURST_RPS_JUMP", 5.0),
            burst_add_workers: env_var_i32("GUNICORN_AUTOSCALER_BURST_ADD_WORKERS", 8),
            burst_cooldown_ms: env_var_u64("GUNICORN_AUTOSCALER_BURST_COOLDOWN_MS", 5000),
            burst_min_rps: env_var_f64("GUNICORN_AUTOSCALER_BURST_MIN_RPS", 3.0),

            map_uvicorn_deprecated: env_var_bool("GUNICORN_AUTOSCALER_MAP_UVICORN_DEPRECATED", false),
            auto_set_graceful_timeout: env_var_bool("GUNICORN_AUTOSCALER_AUTO_SET_GRACEFUL_TIMEOUT", true),
        }
    }
}

fn env_var(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_var_i32(key: &str, default: i32) -> i32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_var_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_var_f64(key: &str, default: f64) -> f64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_var_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

/// Effective cores from cgroups (v2 preferred), else None.
fn effective_cores() -> Option<f64> {
    if let Ok(s) = fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() == 2 && parts[0] != "max" {
            let quota: f64 = parts[0].parse().ok()?;
            let period: f64 = parts[1].parse().ok()?;
            if period > 0.0 {
                return Some(quota / period);
            }
        }
    }
    let quota = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()?;
    let period = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()?;
    if quota > 0.0 && period > 0.0 {
        Some(quota / period)
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct Snapshot {
    rps: f64,
    p95_ms: f64,
    workers_gauge: Option<i32>,
    short_rps: f64,
}

#[derive(Debug)]
struct Bucket {
    sec: u64,
    req_count: f64,
    hist: Histogram<u64>,
}

#[derive(Debug)]
struct State {
    start: Instant,
    window_seconds: u64,
    buckets: VecDeque<Bucket>,
    workers_gauge: Option<i32>,
}

impl State {
    fn new(window_seconds: u64) -> Result<Self> {
        Ok(Self {
            start: Instant::now(),
            window_seconds,
            buckets: VecDeque::new(),
            workers_gauge: None,
        })
    }

    fn now_sec(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    fn ensure_bucket(&mut self, now_sec: u64) -> Result<()> {
        if self.buckets.back().map(|b| b.sec) == Some(now_sec) {
            return Ok(());
        }
        if self.buckets.is_empty() {
            self.buckets.push_back(Bucket {
                sec: now_sec,
                req_count: 0.0,
                hist: Histogram::new_with_bounds(1, 1_000_000, 3)
                    .map_err(|e| anyhow!("hist init: {e}"))?,
            });
        } else {
            let mut sec = self.buckets.back().unwrap().sec + 1;
            while sec <= now_sec {
                self.buckets.push_back(Bucket {
                    sec,
                    req_count: 0.0,
                    hist: Histogram::new_with_bounds(1, 1_000_000, 3)
                        .map_err(|e| anyhow!("hist init: {e}"))?,
                });
                sec += 1;
            }
        }
        let min_keep = now_sec.saturating_sub(self.window_seconds.saturating_sub(1));
        while self.buckets.front().map(|b| b.sec < min_keep).unwrap_or(false) {
            self.buckets.pop_front();
        }
        Ok(())
    }

    fn ingest_request(&mut self, count: f64) -> Result<()> {
        let now = self.now_sec();
        self.ensure_bucket(now)?;
        if let Some(b) = self.buckets.back_mut() {
            b.req_count += count;
        }
        Ok(())
    }

    fn ingest_duration_ms(&mut self, ms: u64) -> Result<()> {
        let now = self.now_sec();
        self.ensure_bucket(now)?;
        if let Some(b) = self.buckets.back_mut() {
            let ms = ms.clamp(1, 1_000_000);
            let _ = b.hist.record(ms);
        }
        Ok(())
    }

    fn snapshot(&mut self, short_window_s: u64) -> Result<Snapshot> {
        let now = self.now_sec();
        self.ensure_bucket(now)?;

        let mut total = 0.0;
        let mut merged = Histogram::new_with_bounds(1, 1_000_000, 3)
            .map_err(|e| anyhow!("hist init: {e}"))?;

        for b in self.buckets.iter() {
            total += b.req_count;
            let _ = merged.add(&b.hist);
        }

        let rps = total / (self.window_seconds as f64).max(1.0);
        let p95_ms = if merged.len() == 0 {
            0.0
        } else {
            merged.value_at_quantile(0.95) as f64
        };

        // short RPS over last N seconds
        let n = short_window_s.max(1);
        let min_sec = now.saturating_sub(n.saturating_sub(1));
        let mut short_total = 0.0;
        for b in self.buckets.iter().rev() {
            if b.sec < min_sec { break; }
            short_total += b.req_count;
        }
        let short_rps = short_total / (n as f64);

        Ok(Snapshot {
            rps,
            p95_ms,
            workers_gauge: self.workers_gauge,
            short_rps,
        })
    }
}

/// StatsD: accept
/// - gunicorn.requests:<v>|c(|@rate)
/// - gunicorn.request.duration:<ms>|ms
/// - gunicorn.workers:<n>|g
fn statsd_server(addr: String, state: Arc<Mutex<State>>, prefix: String, stop: Arc<AtomicBool>) -> Result<()> {
    let sock = UdpSocket::bind(&addr).with_context(|| format!("bind UDP {addr}"))?;
    sock.set_read_timeout(Some(Duration::from_millis(500)))?;
    info!("StatsD listening on udp://{}", addr);

    let mut buf = [0u8; 65535];
    while !stop.load(Ordering::Relaxed) {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => {
                let s = String::from_utf8_lossy(&buf[..n]);
                // tracing::info!("Received StatsD packet: {}", s); 
                for line in s.lines() {
                    if let Err(e) = parse_statsd_line(line, &state, &prefix) {
                        tracing::debug!("statsd parse err: {}", e);
                    }
                }
            }
            Err(_) => continue,
        }
    }
    Ok(())
}

fn parse_statsd_line(line: &str, state: &Arc<Mutex<State>>, prefix: &str) -> Result<()> {
    let (name, rest) = line.split_once(':').ok_or_else(|| anyhow!("no ':'"))?;
    let mut parts = rest.split('|');
    let value_str = parts.next().ok_or_else(|| anyhow!("no value"))?;
    let typ = parts.next().ok_or_else(|| anyhow!("no type"))?;

    let mut rate = 1.0_f64;
    for p in parts {
        if let Some(r) = p.strip_prefix('@') {
            rate = r.parse::<f64>().unwrap_or(1.0);
        }
    }
    if rate <= 0.0 { rate = 1.0; }

    let full = name.trim();
    let want_req = full.ends_with(".requests") || full == format!("{prefix}.requests");
    let want_dur = full.ends_with(".request.duration") || full == format!("{prefix}.request.duration");
    let want_workers = full.ends_with(".workers") || full == format!("{prefix}.workers");

    let mut g = state.lock().unwrap();

    match typ {
        "c" if want_req => {
            let v: f64 = value_str.parse().unwrap_or(0.0);
            g.ingest_request(v / rate)?;
        }
        "ms" | "h" if want_dur => {
            let v: f64 = value_str.parse().unwrap_or(0.0);
            let ms = v.round().max(1.0) as u64;
            g.ingest_duration_ms(ms)?;
        }
        "g" if want_workers => {
            let v: f64 = value_str.parse().unwrap_or(-1.0);
            if v >= 0.0 {
                g.workers_gauge = Some(v.round() as i32);
            }
        }
        _ => {}
    }
    Ok(())
}

fn strip_managed_flags(args: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];

        if a.starts_with("--workers=")
            || a.starts_with("--threads=")
            || a.starts_with("--worker-connections=")
            || a.starts_with("--statsd-host=")
            || a.starts_with("--statsd-prefix=")
            || a.starts_with("--pid=")
        {
            i += 1;
            continue;
        }

        let is_managed = matches!(
            a.as_str(),
            "--workers" | "-w" |
            "--threads" |
            "--worker-connections" |
            "--statsd-host" |
            "--statsd-prefix" |
            "--pid"
        );

        if is_managed {
            i += 2;
            continue;
        }

        out.push(a.clone());
        i += 1;
    }
    out
}

fn map_worker_class(args: Vec<String>, enabled: bool) -> Vec<String> {
    if !enabled { return args; }
    let mut out = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if (a == "--worker-class" || a == "-k") && i + 1 < args.len() {
            let mut wc = args[i + 1].clone();
            if wc == "uvicorn.workers.UvicornWorker" {
                wc = "uvicorn_worker.UvicornWorker".to_string();
            }
            out.push(a.clone());
            out.push(wc);
            i += 2;
            continue;
        }
        out.push(a.clone());
        i += 1;
    }
    out
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag || a.starts_with(&format!("{flag}=")))
}

fn extract_timeout(args: &[String]) -> Option<u64> {
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == "--timeout" || a == "-t" {
            if i + 1 < args.len() {
                return args[i + 1].parse().ok();
            }
        }
        if let Some(v) = a.strip_prefix("--timeout=") {
            return v.parse().ok();
        }
        i += 1;
    }
    None
}

fn maybe_add_graceful_timeout(mut args: Vec<String>, cfg: &Config) -> Vec<String> {
    if !cfg.auto_set_graceful_timeout {
        return args;
    }
    if has_flag(&args, "--graceful-timeout") {
        return args;
    }
    if let Some(t) = extract_timeout(&args) {
        // protective default: match timeout so scale-down doesn't SIGKILL after 30s default.
        args.push("--graceful-timeout".to_string());
        args.push(t.to_string());
    }
    args
}

fn decide(snapshot: &Snapshot, current_workers: i32, cfg: &Config) -> i32 {
    let p95_s = snapshot.p95_ms / 1000.0;
    let conc_est = snapshot.rps * p95_s;
    let mut desired = (conc_est / cfg.target_conc_per_worker).ceil() as i32;

    if snapshot.rps > 0.1 && snapshot.p95_ms > cfg.slo_p95_ms {
        desired = desired.max(current_workers + 1);
    }
    desired.clamp(cfg.min_workers, cfg.max_workers)
}

fn kill_signal(pid: u32, sig: i32) -> Result<()> {
    let rc = unsafe { libc::kill(pid as i32, sig) };
    if rc != 0 {
        return Err(anyhow!(
            "kill({}, {}) failed: {}",
            pid,
            sig,
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

fn read_pidfile() -> Option<u32> {
    fs::read_to_string(PIDFILE).ok()?.trim().parse::<u32>().ok()
}

fn spawn_gunicorn(app: &str, gunicorn_args: &[String], cfg: &Config) -> Result<Child> {
    let _ = fs::remove_file(PIDFILE);

    let mut cmd = Command::new("gunicorn");
    cmd.arg(app)
        .arg("--pid").arg(PIDFILE)
        .arg("--workers").arg(cfg.min_workers.to_string())
        .arg("--statsd-host").arg(&cfg.statsd_addr)
        .arg("--statsd-prefix").arg(&cfg.statsd_prefix)
        .args(gunicorn_args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    info!("Launching: {:?}", cmd);
    cmd.spawn().context("failed to spawn gunicorn")
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_env();
    if cfg.min_workers < 1 {
        return Err(anyhow!("GUNICORN_AUTOSCALER_MIN_WORKERS must be >= 1"));
    }
    if cfg.max_workers < cfg.min_workers {
        return Err(anyhow!("GUNICORN_AUTOSCALER_MAX_WORKERS must be >= MIN_WORKERS"));
    }

    let argv: Vec<String> = env::args().collect();
    if argv.len() < 2 {
        return Err(anyhow!("usage: gunicorn-autoscaler APP [gunicorn args...]"));
    }

    let app = argv[1].clone();
    let passthrough = argv[2..].to_vec();
    let passthrough = strip_managed_flags(passthrough);
    let passthrough = map_worker_class(passthrough, cfg.map_uvicorn_deprecated);
    let passthrough = maybe_add_graceful_timeout(passthrough, &cfg);

    info!("Config: {:?}", cfg);

    let stop = Arc::new(AtomicBool::new(false));
    signal_flag::register(SIGTERM, stop.clone())?;
    signal_flag::register(SIGINT, stop.clone())?;

    let state = Arc::new(Mutex::new(State::new(cfg.window_seconds)?));

    // statsd listener
    {
        let state = state.clone();
        let addr = cfg.statsd_addr.clone();
        let prefix = cfg.statsd_prefix.clone();
        let stop2 = stop.clone();
        thread::spawn(move || {
            if let Err(e) = statsd_server(addr, state, prefix, stop2) {
                error!("statsd thread failed: {}", e);
            }
        });
    }

    let mut child = spawn_gunicorn(&app, &passthrough, &cfg)?;
    let mut master_pid: u32 = read_pidfile().unwrap_or_else(|| child.id());
    info!("Gunicorn master PID: {}", master_pid);

    // controller state
    let mut current_workers = cfg.min_workers;
    let mut last_up = Instant::now() - Duration::from_millis(cfg.up_cooldown_ms);
    let mut last_down = Instant::now() - Duration::from_millis(cfg.down_cooldown_ms);
    let mut last_burst = Instant::now() - Duration::from_millis(cfg.burst_cooldown_ms);
    let mut idle_since = Instant::now();
    let mut down_streak = 0u32;
    let mut prev_short_rps = 0.0_f64;

    while !stop.load(Ordering::Relaxed) {
        if let Some(status) = child.try_wait()? {
            return Err(anyhow!("gunicorn exited: {}", status));
        }

        // master PID might change in edge cases; trust pidfile if present
        if let Some(p) = read_pidfile() {
            master_pid = p;
        }

        let snap = {
            let mut g = state.lock().unwrap();
            g.snapshot(cfg.burst_window_seconds)?
        };

        // sync current_workers from gunicorn.workers gauge if available (avoids drift)
        if let Some(w) = snap.workers_gauge {
            if w >= cfg.min_workers && w <= cfg.max_workers {
                current_workers = w;
            }
        }

        // BURST MODE: preemptive scale-up on sudden RPS jump
        if cfg.burst_enabled {
            let delta = snap.short_rps - prev_short_rps;
            if snap.short_rps >= cfg.burst_min_rps
                && delta >= cfg.burst_rps_jump
                && last_burst.elapsed() >= Duration::from_millis(cfg.burst_cooldown_ms)
                && current_workers < cfg.max_workers
            {
                let add = (cfg.max_workers - current_workers).min(cfg.burst_add_workers);
                info!(
                    "BURST: short_rps={:.2} prev={:.2} delta={:.2} -> add {} workers",
                    snap.short_rps, prev_short_rps, delta, add
                );
                for _ in 0..add {
                    kill_signal(master_pid, libc::SIGTTIN)?;
                    current_workers += 1;
                }
                last_up = Instant::now();
                last_burst = Instant::now();
                down_streak = 0;
            }
            prev_short_rps = snap.short_rps;
        }

        // idle detection (for cost saving)
        if snap.rps < 0.01 {
            // effectively idle
            // don't downscale too soon after scaling up
            if idle_since.elapsed().as_secs() >= cfg.idle_seconds
                && current_workers > cfg.min_workers
                && last_down.elapsed() >= Duration::from_millis(cfg.down_cooldown_ms)
                && last_up.elapsed() >= Duration::from_millis(cfg.no_downscale_for_ms_after_up)
            {
                info!("Idle: scale down by 1 ({} -> {})", current_workers, current_workers - 1);
                kill_signal(master_pid, libc::SIGTTOU)?;
                current_workers -= 1;
                last_down = Instant::now();
            }
        } else {
            idle_since = Instant::now();
        }

        // normal controller
        let desired = decide(&snap, current_workers, &cfg);

        if desired > current_workers && last_up.elapsed() >= Duration::from_millis(cfg.up_cooldown_ms) {
            let gap = desired - current_workers;
            let step = cfg.max_step_up.min(std::cmp::max(1, current_workers / 2));
            let n = gap.min(step);
            info!(
                "Scale up: rps={:.2} p95={:.0}ms desired={} current={} +{}",
                snap.rps, snap.p95_ms, desired, current_workers, n
            );
            for _ in 0..n {
                kill_signal(master_pid, libc::SIGTTIN)?;
                current_workers += 1;
            }
            last_up = Instant::now();
            down_streak = 0;
        }

        if desired < current_workers
            && last_down.elapsed() >= Duration::from_millis(cfg.down_cooldown_ms)
            && last_up.elapsed() >= Duration::from_millis(cfg.no_downscale_for_ms_after_up)
            && snap.p95_ms <= cfg.slo_p95_ms
        {
            down_streak += 1;
            if down_streak >= 3 {
                info!(
                    "Scale down: rps={:.2} p95={:.0}ms desired={} current={} -1",
                    snap.rps, snap.p95_ms, desired, current_workers
                );
                kill_signal(master_pid, libc::SIGTTOU)?;
                current_workers -= 1;
                last_down = Instant::now();
                down_streak = 0;
            }
        } else if desired >= current_workers {
            down_streak = 0;
        }

        thread::sleep(Duration::from_millis(cfg.tick_ms));
    }

    warn!("Stopping: forwarding SIGTERM to gunicorn...");
    let _ = kill_signal(master_pid, libc::SIGTERM);
    let _ = child.wait();
    Ok(())
}
