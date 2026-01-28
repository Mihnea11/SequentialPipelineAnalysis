import time
import streamlit as st

from ui.runner import start_background_loop, stop_background_loop, RunnerState
from ui.engine_bridge import run_engine_for_ui, EngineConfig


# ---------------- Helpers ----------------

def ensure_state():
    if "runner" not in st.session_state:
        st.session_state.runner = RunnerState()
    if "buffer" not in st.session_state:
        st.session_state.buffer = []
    if "last_drained_at" not in st.session_state:
        st.session_state.last_drained_at = 0.0
    if "metrics_history" not in st.session_state:
        st.session_state.metrics_history = []
    if "last_engine_cfg" not in st.session_state:
        st.session_state.last_engine_cfg = None


def is_running(state: RunnerState) -> bool:
    return state.thread is not None and state.thread.is_alive()


def drain_queue(state: RunnerState, limit: int = 300):
    if state.out_queue is None:
        return 0

    drained = 0
    while drained < limit:
        try:
            item = state.out_queue.get_nowait()
        except Exception:
            break
        st.session_state.buffer.append(item)
        drained += 1

    st.session_state.last_drained_at = time.time()
    return drained


def trim_buffer(max_items: int):
    if len(st.session_state.buffer) > max_items:
        st.session_state.buffer = st.session_state.buffer[-max_items:]


def split_buffer(items):
    events = [x for x in items if x.get("type") == "event"]
    aggs = [x for x in items if x.get("type") == "agg"]
    metrics = [x for x in items if x.get("type") == "metrics"]
    return events, aggs, metrics


def latest_metrics(metrics_items):
    if not metrics_items:
        return None
    return metrics_items[-1].get("data")


def push_metrics_history(snap: dict | None):
    if not snap:
        return

    cur_id = (snap.get("ingested_total"), snap.get("processed_total"), snap.get("aggregated_total"))
    last_id = st.session_state.metrics_history[-1]["_id"] if st.session_state.metrics_history else None

    if cur_id != last_id:
        st.session_state.metrics_history.append({"_id": cur_id, "ts": time.time(), "snap": snap})
        st.session_state.metrics_history = st.session_state.metrics_history[-200:]


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)


# ---------------- UI ----------------

st.set_page_config(page_title="PFN Streaming Dashboard", page_icon="📡", layout="wide")
ensure_state()

st.sidebar.title("📡 Control Panel")

refresh_ms = st.sidebar.slider("Refresh interval (ms)", 100, 2000, 300, 50)
max_buffer = st.sidebar.slider("Max buffer size", 200, 8000, 2000, 200)
drain_limit = st.sidebar.slider("Drain per tick", 50, 1000, 300, 50)

st.sidebar.divider()

st.sidebar.subheader("🔥 Stress mode")

stress_mode = st.sidebar.toggle("Enable stress mode", value=False)

artificial_delay_ms = st.sidebar.slider("Artificial delay per window (ms)", 0, 200, 0, 5, disabled=not stress_mode)

per_source_queue_size = st.sidebar.slider("Per-source queue size", 1, 20, 10, 1, disabled=not stress_mode)
merged_queue_size = st.sidebar.slider("Merged queue size", 1, 100, 30, 1, disabled=not stress_mode)

st.sidebar.caption("Tip: in stress mode, small queues + delay make drops visible (drop_ratio > 0).")

st.sidebar.divider()

cfg = EngineConfig(
    stress_mode=stress_mode,
    artificial_delay_ms=float(artificial_delay_ms),
    per_source_queue_size=int(per_source_queue_size),
    merged_queue_size=int(merged_queue_size),
)

if st.session_state.last_engine_cfg != cfg:
    st.session_state.metrics_history = []
    st.session_state.last_engine_cfg = cfg

c1, c2 = st.sidebar.columns(2)

with c1:
    if st.sidebar.button("▶ Start", type="primary", use_container_width=True):
        if not is_running(st.session_state.runner):
            st.session_state.runner = start_background_loop(run_engine_for_ui, cfg)

with c2:
    if st.sidebar.button("⏹ Stop", use_container_width=True):
        stop_background_loop(st.session_state.runner)

if st.sidebar.button("🧹 Clear buffer", use_container_width=True):
    st.session_state.buffer = []

if st.sidebar.button("🧹 Clear charts history", use_container_width=True):
    st.session_state.metrics_history = []

running = is_running(st.session_state.runner)
st.sidebar.success("RUNNING ✅" if running else "STOPPED ⏹️")
st.sidebar.caption(f"Last drain: {time.strftime('%H:%M:%S', time.localtime(st.session_state.last_drained_at))}")

st.title("PFN – Streaming Dashboard")
if stress_mode:
    st.error("🔥 STRESS MODE ACTIVE — drops/backpressure expected")
else:
    st.success("✅ Normal mode — stable operation expected")
st.caption("Asyncio streaming pipeline • map/filter/window/reduce • tumbling windows • live metrics")

drained_now = drain_queue(st.session_state.runner, limit=drain_limit)
trim_buffer(max_buffer)

items = st.session_state.buffer
events, aggs, metrics_items = split_buffer(items)
snap = latest_metrics(metrics_items)

push_metrics_history(snap)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Queue drained (this tick)", drained_now)
k2.metric("Buffered updates", len(items))
k3.metric("Agg windows seen", len(aggs))
k4.metric("Metrics snapshots", len(metrics_items))

st.divider()

# ======================
# Row 1: Raw + Aggregated
# ======================

col_raw, col_agg = st.columns([1.2, 1.0], gap="large")

with col_raw:
    with st.expander("🧾 Raw data generated (unprocessed)", expanded=True):
        st.caption("Events produced by sources before windowing/aggregation.")
        raw_payloads = [x.get("data") for x in events[-25:]]
        if not raw_payloads:
            st.info("No raw events yet. Press Start.")
        else:
            st.json(raw_payloads)

with col_agg:
    with st.expander("🪟 Aggregated windows", expanded=True):
        st.caption("Tumbling-window outputs (aggregated events).")
        agg_payloads = [x.get("data") for x in aggs[-15:]]
        if not agg_payloads:
            st.info("No aggregations yet. Wait ~5 seconds after Start.")
        else:
            st.json(agg_payloads)

st.divider()

# ======================
# Row 2: KPI section
# ======================

with st.expander("✅ KPIs (derived from metrics snapshot)", expanded=True):
    if snap is None:
        st.info("No metrics snapshot yet. Wait ~2 seconds after Start.")
    else:
        rates = snap.get("rates_eps", {}) or {}
        lat = snap.get("event_processing_latency_ms", {}) or {}

        dropped = snap.get("dropped_total", 0) or 0
        processed = snap.get("processed_total", 0) or 0
        drop_ratio = (dropped / processed) if processed else 0.0

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Ingest (eps)", f"{safe_float(rates.get('ingest')):.2f}")
        r2.metric("Process (eps)", f"{safe_float(rates.get('process')):.2f}")
        r3.metric("Aggregate (eps)", f"{safe_float(rates.get('aggregate')):.2f}")
        r4.metric("Drop ratio", f"{drop_ratio:.2%}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latency avg (ms)", f"{safe_float(lat.get('avg_ms')):.3f}")
        c2.metric("Latency p50 (ms)", f"{safe_float(lat.get('p50_ms')):.3f}")
        c3.metric("Latency p95 (ms)", f"{safe_float(lat.get('p95_ms')):.3f}")
        c4.metric("Dropped total", int(dropped))

        with st.expander("Details (raw snapshot JSON)", expanded=False):
            st.json(snap)

st.divider()

# ======================
# Row 3: Performance graphs
# ======================

with st.expander("📊 Performance graphs", expanded=True):
    hist = st.session_state.metrics_history

    if not hist:
        st.info("No history yet. Start the engine and wait a few seconds.")
    else:
        eps_series = []
        lat_series = []
        drop_series = []

        for p in hist:
            s = p["snap"]
            rates = s.get("rates_eps", {}) or {}
            lat = s.get("event_processing_latency_ms", {}) or {}
            dropped = s.get("dropped_total", 0) or 0
            processed = s.get("processed_total", 0) or 0
            ratio = (dropped / processed) if processed else 0.0

            eps_series.append({
                "ingest_eps": safe_float(rates.get("ingest")),
                "process_eps": safe_float(rates.get("process")),
                "aggregate_eps": safe_float(rates.get("aggregate")),
            })
            lat_series.append({
                "lat_avg_ms": safe_float(lat.get("avg_ms")),
                "lat_p50_ms": safe_float(lat.get("p50_ms")),
                "lat_p95_ms": safe_float(lat.get("p95_ms")),
            })
            drop_series.append({
                "drop_ratio": safe_float(ratio),
                "dropped_total": safe_float(dropped),
            })

        g1, g2 = st.columns(2)
        with g1:
            st.subheader("EPS (events/sec)")
            st.line_chart(eps_series)

        with g2:
            st.subheader("Latency (ms)")
            st.line_chart(lat_series)

        st.subheader("Drops")
        st.line_chart(drop_series)

if running:
    time.sleep(refresh_ms / 1000.0)
    st.rerun()
