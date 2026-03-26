import streamlit as st
import json
import pandas as pd
import time
import re
from snowflake.snowpark.context import get_active_session

# ==========================================
# CONFIGURATION
# ==========================================
ENGINE_DB = "SYSTEM_DQ_DB"
ENGINE_SCHEMA = "CONFIG"
CORTEX_MODEL = "claude-opus-4-6" 

st.set_page_config(page_title="AutoHeal.ai OS", layout="wide", page_icon="⚡")
session = get_active_session()
current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]

# ==========================================
# 🎵 HIDDEN AMBIENT SCI-FI BACKGROUND MUSIC (BULLETPROOF VOLUME)
# ==========================================
# We wrap the audio and script inside an invisible iframe so Streamlit cannot strip the JavaScript.
audio_html = """
<iframe 
    srcdoc="
        <audio id='bg-audio' autoplay loop>
            <source src='https://cdn.pixabay.com/download/audio/2022/01/18/audio_d0a13f69d2.mp3?filename=sci-fi-ambient-116345.mp3' type='audio/mpeg'>
        </audio>
        <script>
            var audio = document.getElementById('bg-audio');
            audio.volume = 0.01; /* 5% Volume - very quiet! */
        </script>
    " 
    width="0" 
    height="0" 
    style="display:none;" 
    allow="autoplay">
</iframe>
"""
st.markdown(audio_html, unsafe_allow_html=True)
# ==========================================
# 🎬 THE JARVIS BOOT SEQUENCE (RUNS ONCE)
# ==========================================
if "jarvis_booted" not in st.session_state:
    boot_html = f"""
    <style>
        .jarvis-overlay {{
            position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
            background-color: #030508; z-index: 9999999;
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            font-family: 'Share Tech Mono', monospace;
            animation: bootFade 4.5s cubic-bezier(0.8, 0, 0.2, 1) forwards; pointer-events: none; 
        }}
        .arc-reactor {{ position: relative; width: 250px; height: 250px; margin-bottom: 40px; }}
        .ring {{ position: absolute; border-radius: 50%; border: 2px solid transparent; top: 0; left: 0; right: 0; bottom: 0; }}
        .ring.r1 {{ border-top: 4px solid #00f0ff; border-left: 4px solid #00f0ff; animation: spin 1.5s linear infinite; box-shadow: 0 0 15px #00f0ff inset; }}
        .ring.r2 {{ border-bottom: 4px solid #ff0055; border-right: 4px solid #ff0055; animation: spinRev 2.5s linear infinite; margin: 20px; box-shadow: 0 0 15px #ff0055; }}
        .ring.r3 {{ border-top: 4px solid #39ff14; border-bottom: 4px solid #39ff14; animation: spin 1s linear infinite; margin: 45px; }}
        .ring.r4 {{ border: 2px dashed #a0c0d0; animation: spinRev 8s linear infinite; margin: 65px; }}
        .core {{ position: absolute; top: 95px; left: 95px; right: 95px; bottom: 95px; background: #00f0ff; border-radius: 50%; box-shadow: 0 0 40px #00f0ff, 0 0 80px #00f0ff; animation: pulseCore 1s ease-in-out infinite alternate; }}
        .terminal-log {{ color: #00f0ff; font-size: 18px; text-align: left; width: 450px; text-shadow: 0 0 5px #00f0ff; }}
        .log-line {{ opacity: 0; margin: 8px 0; }}
        .log-1 {{ animation: typeLine 0s 0.5s forwards; }}
        .log-2 {{ animation: typeLine 0s 1.2s forwards; }}
        .log-3 {{ animation: typeLine 0s 1.9s forwards; }}
        .log-4 {{ animation: typeLine 0s 2.6s forwards; color: #ff0055; text-shadow: 0 0 8px #ff0055; }}
        .log-5 {{ animation: typeLine 0s 3.3s forwards; color: #39ff14; text-shadow: 0 0 10px #39ff14; font-weight: bold; font-size: 24px;}}
        @keyframes spin {{ 100% {{ transform: rotate(360deg); }} }}
        @keyframes spinRev {{ 100% {{ transform: rotate(-360deg); }} }}
        @keyframes pulseCore {{ 0% {{ transform: scale(0.8); opacity: 0.6; }} 100% {{ transform: scale(1.3); opacity: 1; }} }}
        @keyframes typeLine {{ to {{ opacity: 1; }} }}
        @keyframes bootFade {{ 0%, 85% {{ opacity: 1; visibility: visible; }} 100% {{ opacity: 0; visibility: hidden; display: none; }} }}
    </style>
    <div class="jarvis-overlay">
        <div class="arc-reactor"><div class="ring r1"></div><div class="ring r2"></div><div class="ring r3"></div><div class="ring r4"></div><div class="core"></div></div>
        <div class="terminal-log">
            <div class="log-line log-1">> SYSTEM.BOOT (AUTOHEAL.AI)</div>
            <div class="log-line log-2">> BYPASSING MAINFRAME PROTOCOLS...</div>
            <div class="log-line log-3">> ESTABLISHING CORTEX NEURAL LINK...</div>
            <div class="log-line log-4">> OVERRIDING SECURITY RESTRICTIONS...</div>
            <div class="log-line log-5">> HUD ONLINE. WELCOME, {current_user}.</div>
        </div>
    </div>
    """
    st.markdown(boot_html, unsafe_allow_html=True)
    st.session_state["jarvis_booted"] = True

# ==========================================
# 🚀 FUTURISTIC NEON CSS + MOTION BACKGROUND
# ==========================================
THEME_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
    [data-testid="stAppViewContainer"], .stApp { font-family: 'Share Tech Mono', monospace !important; background: linear-gradient(270deg, #07090f, #0d1b2a, #1a0b1c, #07090f) !important; background-size: 400% 400% !important; animation: aiThinking 12s ease infinite !important; color: #00f0ff !important; }
    [data-testid="stHeader"] { background: transparent !important; }
    @keyframes aiThinking { 0% { background-position: 0% 50%; } 50% { background-position: 100% 50%; } 100% { background-position: 0% 50%; } }
    h1, h2, h3, h4, h5, h6 { color: #00f0ff !important; text-transform: uppercase; letter-spacing: 2px; text-shadow: 0 0 10px rgba(0, 240, 255, 0.5); }
    div[data-testid="metric-container"] { background-color: rgba(10, 15, 25, 0.6) !important; backdrop-filter: blur(10px); border: 1px solid #00f0ff; border-radius: 4px; padding: 15px; box-shadow: 0 0 15px rgba(0, 240, 255, 0.1) inset, 0 0 10px rgba(0, 240, 255, 0.2); }
    [data-testid="stMetricValue"] { color: #39ff14 !important; font-weight: bold !important; text-shadow: 0 0 8px rgba(57, 255, 20, 0.6); }
    [data-testid="stMetricLabel"] { color: #a0c0d0 !important; text-transform: uppercase; font-size: 12px; }
    [data-testid="stExpander"] { background-color: rgba(10, 15, 25, 0.6) !important; backdrop-filter: blur(10px); border: 1px solid #ff0055 !important; border-radius: 4px; box-shadow: 0 0 10px rgba(255, 0, 85, 0.1) inset; }
    .stButton > button { background: transparent !important; border: 1px solid #00f0ff !important; color: #00f0ff !important; border-radius: 0px !important; font-weight: bold !important; text-transform: uppercase; box-shadow: 0 0 10px rgba(0, 240, 255, 0.2) inset; transition: all 0.3s ease-in-out; }
    .stButton > button:hover { background: #00f0ff !important; color: #000 !important; box-shadow: 0 0 20px rgba(0, 240, 255, 0.8); }
    button[kind="primary"] { border: 1px solid #ff0055 !important; color: #ff0055 !important; box-shadow: 0 0 10px rgba(255, 0, 85, 0.2) inset !important; }
    button[kind="primary"]:hover { background: #ff0055 !important; color: #fff !important; box-shadow: 0 0 20px rgba(255, 0, 85, 0.8) !important; }
    hr { border-top: 1px solid rgba(0, 240, 255, 0.3) !important; }
</style>
"""
st.markdown(THEME_CSS, unsafe_allow_html=True)
st.markdown("""<div style="position: fixed; top: 50%; left: 50%; width: 100vw; height: 100vh; background: radial-gradient(circle, rgba(0, 240, 255, 0.05) 0%, rgba(255, 0, 85, 0.03) 35%, rgba(0,0,0,0) 65%); transform: translate(-50%, -50%); z-index: -1; animation: orbPulse 6s ease-in-out infinite alternate; pointer-events: none;"></div><style>@keyframes orbPulse { 0% { transform: translate(-50%, -50%) scale(0.8); opacity: 0.5; } 100% { transform: translate(-50%, -50%) scale(1.2); opacity: 1; } }</style>""", unsafe_allow_html=True)

col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.markdown("<h1>⚡ AutoHeal.ai : Neural Command</h1>", unsafe_allow_html=True)
    st.markdown("`SYSTEM STATUS: ONLINE | AGENTIC HEALING: ENABLED | CORTEX UPLINK: SECURE`")
with col_refresh:
    st.write("") 
    if st.button("REBOOT HUD", use_container_width=True): 
        st.session_state.clear()
        st.rerun()

# ==========================================
# SIDEBAR: COMMAND UPLINK
# ==========================================
with st.sidebar:
    st.markdown("### 📡 TARGET UPLINK")
    dbs = [row["name"] for row in session.sql("SHOW DATABASES").collect()]
    selected_db = st.selectbox("DB_TARGET", dbs)
    schemas = [row["name"] for row in session.sql(f"SHOW SCHEMAS IN DATABASE {selected_db}").collect()]
    selected_schema = st.selectbox("SCHEMA_TARGET", schemas)
    st.divider()
    st.markdown("### ⚙️ EXECUTION PROTOCOLS")
    
    if st.button("INITIATE NEURAL SCAN", type="primary", use_container_width=True):
        session.sql(f"CALL {ENGINE_DB}.{ENGINE_SCHEMA}.run_background_dq_profiler('{selected_db}', '{selected_schema}')").collect_nowait()
        start_time = time.time()
        
        # 🚀 THE FLOATING JARVIS PROFILER WIDGET
        jarvis_tracker = st.empty()
        
        with st.status("🔗 ESTABLISHING CORTEX UPLINK...", expanded=True) as status:
            st.write(f"> Deploying {CORTEX_MODEL} probes...")
            progress_bar = st.empty()
            
            while True:
                time.sleep(3) 
                try:
                    track_df = session.sql(f"SELECT * FROM {ENGINE_DB}.{ENGINE_SCHEMA}.PROFILER_TRACKING WHERE DATABASE_NAME='{selected_db}' AND SCHEMA_NAME='{selected_schema}'").collect()
                    if track_df:
                        track = track_df[0]
                        total, done, status_msg = track["TOTAL_TABLES"], track["PROFILED_TABLES"], track["STATUS"]
                        
                        if total > 0:
                            pct = done / total
                            progress_bar.progress(pct, text=f"SCANNING: {done}/{total} NODES ({int(pct*100)}%)")
                            
                            jarvis_tracker.markdown(f"""
                            <div style="position: fixed; bottom: 30px; right: 30px; width: 240px; background: rgba(5, 8, 15, 0.9); border: 1px solid #00f0ff; border-radius: 8px; padding: 15px; box-shadow: 0 0 20px rgba(0, 240, 255, 0.4); z-index: 99999; text-align: center; font-family: 'Share Tech Mono', monospace;">
                                <div style="position: relative; width: 60px; height: 60px; margin: 0 auto 10px;">
                                    <div style="position: absolute; top:0; left:0; right:0; bottom:0; border-radius: 50%; border: 2px solid transparent; border-top-color: #00f0ff; border-bottom-color: #ff0055; animation: spin 1s linear infinite;"></div>
                                    <div style="position: absolute; top:10px; left:10px; right:10px; bottom:10px; border-radius: 50%; border: 2px dashed #39ff14; animation: spinRev 2s linear infinite;"></div>
                                    <div style="position: absolute; top:20px; left:20px; right:20px; bottom:20px; background: #00f0ff; border-radius: 50%; box-shadow: 0 0 10px #00f0ff; animation: pulseCore 1s infinite alternate;"></div>
                                </div>
                                <div style="color: #a0c0d0; font-size: 10px; letter-spacing: 1px;">NEURAL PROFILING</div>
                                <div style="color: #39ff14; font-size: 14px; font-weight: bold; margin-top: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{track['CURRENT_TABLE']}</div>
                                <div style="color: #00f0ff; font-size: 10px; margin-top: 5px;">{int(pct*100)}% COMPLETE</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                        if status_msg == "COMPLETED":
                            progress_bar.progress(1.0, text="NEURAL SCAN COMPLETE.")
                            status.update(label="✅ SCAN SUCCESSFUL", state="complete", expanded=False)
                            jarvis_tracker.empty()
                            break
                        elif "ERROR" in status_msg:
                            status.update(label="❌ UPLINK FAILED", state="error", expanded=False)
                            st.error(f"Execution Error: {status_msg}")
                            jarvis_tracker.empty()
                            break
                except Exception: pass 
                    
    st.divider()
    if st.button("EXECUTE GUARDIAN MONITOR", use_container_width=True):
        with st.spinner("EXECUTING DMFs AND AGENTIC HEALING PROTOCOLS..."):
            try:
                session.sql(f"CALL {ENGINE_DB}.{ENGINE_SCHEMA}.run_dq_monitor('{selected_db}', '{selected_schema}')").collect()
                st.success("MONITOR SEQUENCE COMPLETE. REFRESHING HUD.")
            except Exception as e: st.error(f"EXECUTION FAILED: {e}")

# ==========================================
# TABS SETUP
# ==========================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "💠 SYSTEM HEALTH", "🚨 INCIDENT ALERTS", "⏱️ TEMPORAL SYNC", 
    "🛡️ REMEDIATION QUEUE", "🧠 NEURAL RULE ENGINE", "🔋 RESOURCE TELEMETRY"
])

with tab1:
    st.markdown(f"### 💠 VITAL SIGNS: `{selected_db}.{selected_schema}`")
    try:
        metrics_df = session.sql(f"SELECT COUNT(*) as TOTAL_RULES, SUM(CASE WHEN IS_RESOLVED = TRUE THEN 1 ELSE 0 END) as PASSING, SUM(CASE WHEN IS_RESOLVED = FALSE THEN 1 ELSE 0 END) as FAILING FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_RULE_RESULTS WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}'").collect()
        alerts_count = session.sql(f"SELECT COUNT(*) AS CNT FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_ALERTS WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND IS_RESOLVED = FALSE").collect()[0]["CNT"]
        vol_df = session.sql(f"SELECT SUM(TOTAL_ROWS) as TOT FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_FRESHNESS_RESULTS WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}'").collect()
        total_volume = vol_df[0]["TOT"] if vol_df and vol_df[0]["TOT"] else 0
        heals_df = session.sql(f"SELECT COUNT(*) as TOT FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_REMEDIATION_LOG WHERE STATUS = 'APPLIED'").collect()
        total_heals = heals_df[0]["TOT"] if heals_df else 0

        if metrics_df and metrics_df[0]["TOTAL_RULES"] > 0:
            metrics = metrics_df[0]
            total_rules = metrics["TOTAL_RULES"]
            passing = metrics["PASSING"] or 0
            failing = metrics["FAILING"] or 0
            health_score = int((passing / total_rules) * 100) if total_rules > 0 else 0

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Data Nodes Scanned", f"{total_volume:,}")
            c2.metric("Active Rules", total_rules)
            c3.metric("Failing Integrity", failing)
            c4.metric("Active Anomalies", alerts_count)
            c5.metric("Agentic Heals Executed", total_heals)
            st.write("")
            
            col_gauge, col_table = st.columns([1, 2])
            with col_gauge:
                st.markdown(f"""
                <div style="background-color: rgba(10, 15, 25, 0.6); backdrop-filter: blur(10px); border: 1px solid #00f0ff; border-radius: 4px; padding: 30px 20px; text-align: center; box-shadow: 0 0 15px rgba(0, 240, 255, 0.1) inset, 0 0 10px rgba(0, 240, 255, 0.2); height: 100%;">
                    <h3 style="margin-top: 0; color: #a0c0d0; font-size: 16px; letter-spacing: 2px;">SYSTEM INTEGRITY</h3>
                    <h1 style="color: #39ff14; font-size: 64px; margin: 20px 0; text-shadow: 0 0 15px rgba(57, 255, 20, 0.6);">{health_score}%</h1>
                    <div style="width: 100%; background-color: #07090f; border: 1px solid #333; height: 24px; border-radius: 12px; overflow: hidden; margin-top: 20px;">
                        <div style="width: {health_score}%; background-color: #39ff14; height: 100%; box-shadow: 0 0 10px #39ff14;"></div>
                    </div>
                    <p style="color: #ff0055; font-size: 12px; margin-top: 15px; letter-spacing: 1px;">► {failing} ANOMALIES DETECTED</p>
                </div>
                """, unsafe_allow_html=True)
            with col_table:
                st.markdown("#### 📉 INTEGRITY TELEMETRY")
                results_df = session.sql(f"SELECT TABLE_NAME, RULE_TYPE, RULE_NAME, VIOLATION_COUNT, CASE WHEN IS_RESOLVED = TRUE THEN '✅ NOMINAL' ELSE '❌ ANOMALY' END AS STATUS FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_RULE_RESULTS WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' ORDER BY IS_RESOLVED ASC, LAST_SEEN_AT DESC").collect()
                st.dataframe(results_df, use_container_width=True)
        else: st.info("> NO TELEMETRY DATA FOUND. INITIATE SCAN.")
    except Exception: st.info("> UPLINK OFFLINE.")

with tab2:
    st.markdown("### 🚨 INCIDENT ALERTS & DISPATCH")
    try:
        alerts_raw = session.sql(f"SELECT SEVERITY, ALERT_TYPE, TABLE_NAME, RULE_NAME, MESSAGE, LAST_SEEN_AT FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_ALERTS WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND IS_RESOLVED = FALSE ORDER BY CASE WHEN SEVERITY = 'CRITICAL' THEN 1 ELSE 2 END, LAST_SEEN_AT DESC").collect()
        alerts_pd = pd.DataFrame([r.as_dict() for r in alerts_raw])
        if not alerts_pd.empty:
            for _, row in alerts_pd.iterrows():
                if row["SEVERITY"] == "CRITICAL": st.error(f"**[CRITICAL ANOMALY] {row['ALERT_TYPE']} on {row['TABLE_NAME']}** | {row['MESSAGE']}")
                else: st.warning(f"**[SUB-OPTIMAL] {row['ALERT_TYPE']} on {row['TABLE_NAME']}** | {row['MESSAGE']}")
            st.divider()
            col_down, col_email = st.columns(2)
            with col_down:
                raw_table = alerts_pd.to_html(index=False, escape=False)
                styled_table = raw_table.replace('<table border="1" class="dataframe">', '<table style="width: 100%; border-collapse: collapse; font-family: Arial, sans-serif; font-size: 14px;">')
                styled_table = styled_table.replace('<th>', '<th style="background-color: #f4f6f8; color: #333; padding: 12px; text-align: left; border-bottom: 2px solid #ddd;">')
                styled_table = styled_table.replace('<td>', '<td style="padding: 10px; border-bottom: 1px solid #ddd; color: #555;">')
                styled_table = styled_table.replace('<td>CRITICAL</td>', '<td style="padding: 10px; border-bottom: 1px solid #ddd; color: #d9534f; font-weight: bold;">🚨 CRITICAL</td>')
                styled_table = styled_table.replace('<td>WARNING</td>', '<td style="padding: 10px; border-bottom: 1px solid #ddd; color: #f0ad4e; font-weight: bold;">⚠️ WARNING</td>')
                rich_html_email = f"""
                <html>
                <body style="font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f7f6;">
                    <div style="max-width: 900px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.05); border: 1px solid #e0e0e0;">
                        <div style="background-color: #ff4b4b; color: #ffffff; padding: 20px; text-align: center;"><h2 style="margin: 0; font-size: 24px;">🚨 AutoHeal.ai Operations</h2><p style="margin: 5px 0 0 0; font-size: 14px; opacity: 0.9;">Daily Data Quality Incident Report</p></div>
                        <div style="padding: 30px;"><p style="font-size: 16px; color: #333;">The following data quality anomalies have been detected:</p>{styled_table}</div>
                    </div>
                </body>
                </html>
                """
                st.download_button(label="📥 EXPORT INCIDENT REPORT", data=rich_html_email, file_name="autoheal_dispatch.html", mime="text/html", use_container_width=True)
            with col_email:
                with st.popover("📧 TRANSMIT TO EXTERNAL COMM", use_container_width=True):
                    email_address = st.text_input("ENTER RECIPIENT ID:")
                    if st.button("TRANSMIT"):
                        try:
                            safe_html_email = rich_html_email.replace("'", "''")
                            session.sql(f"CALL SYSTEM$SEND_EMAIL('coco_email_int', '{email_address}', 'AUTOHEAL.AI SYSTEM ALERTS', '{safe_html_email}', 'text/html')").collect()
                            st.success("> TRANSMISSION SUCCESSFUL")
                        except Exception as e: st.error(f"> TRANSMISSION FAILED: Integration Offline. {e}")
            st.dataframe(alerts_pd, use_container_width=True)
        else: st.success("> NO ANOMALIES DETECTED. SYSTEM NOMINAL.")
    except Exception: pass

with tab3:
    st.markdown("### ⏱️ TEMPORAL SYNC (FRESHNESS)")
    try:
        fresh_df = session.sql(f"SELECT TABLE_NAME, TIMESTAMP_COLUMN, TOTAL_ROWS, STALE_ROWS, FRESHNESS_PCT || '%' AS FRESHNESS_PCT, STATUS, NEWEST_RECORD, LAST_SEEN_AT FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_FRESHNESS_RESULTS WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' ORDER BY FRESHNESS_PCT ASC").collect()
        if fresh_df: st.dataframe(fresh_df, use_container_width=True)
        else: st.info("> TEMPORAL SYNC DATA UNAVAILABLE.")
    except Exception: pass

with tab4:
    st.markdown("### 🛡️ AGENTIC REMEDIATION QUEUE")
    try:
        st.markdown("#### ⚠️ PENDING OVERRIDES")
        pending_df = session.sql(f"SELECT LOG_ID, TABLE_NAME, COLUMN_NAME, REMEDIATION_TYPE, ROWS_AFFECTED, SQL_QUERY FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_REMEDIATION_LOG WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND STATUS = 'PENDING' ORDER BY RUN_TIMESTAMP DESC").collect()
        pending_pd = pd.DataFrame([r.as_dict() for r in pending_df])
        if not pending_pd.empty:
            st.dataframe(pending_pd, use_container_width=True)
            selected_ids = st.multiselect("SELECT LOG_ID FOR AGENTIC EXECUTION:", pending_pd['LOG_ID'].tolist())
            if st.button("✅ AUTHORIZE OVERRIDE", type="primary"):
                for log_id in selected_ids:
                    query_to_run = pending_pd[pending_pd['LOG_ID'] == log_id]['SQL_QUERY'].values[0]
                    try:
                        session.sql(query_to_run).collect()
                        session.sql(f"UPDATE {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_REMEDIATION_LOG SET STATUS = 'APPLIED', EXECUTED_BY = '{current_user}', RUN_TIMESTAMP = CURRENT_TIMESTAMP() WHERE LOG_ID = '{log_id}'").collect()
                        st.toast(f"> OVERRIDE EXECUTED: {log_id}")
                    except Exception as e: st.error(f"> EXECUTION FAILED: {e}")
                time.sleep(1)
                st.rerun()
        else: st.info("> QUEUE EMPTY.")
        st.divider()
        st.markdown("#### 📜 OVERRIDE HISTORY")
        remedy_df = session.sql(f"SELECT RUN_TIMESTAMP, TABLE_NAME, COLUMN_NAME, REMEDIATION_TYPE, ROWS_AFFECTED, STATUS, EXECUTED_BY, SQL_QUERY FROM {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_REMEDIATION_LOG WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND STATUS = 'APPLIED' ORDER BY RUN_TIMESTAMP DESC").collect()
        if remedy_df: st.dataframe(remedy_df, use_container_width=True)
    except Exception: pass

with tab5:
    st.markdown("### 🧠 NEURAL RULE ENGINE")
    rules_df = session.sql(f"SELECT TABLE_NAME, CORTEX_JSON FROM {ENGINE_DB}.{ENGINE_SCHEMA}.AUTO_RULES_JSON WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND CORTEX_JSON IS NOT NULL").collect()

    if not rules_df: st.info("> NO NEURAL RULES LOADED.")
    else:
        try:
            tables_meta = session.sql(f"SHOW TABLES IN SCHEMA {selected_db}.{selected_schema}").collect()
            table_stats_map = {t["name"]: {"rows": t["rows"], "bytes": t["bytes"]} for t in tables_meta}
        except Exception: table_stats_map = {}

        for row in rules_df:
            table_name = row["TABLE_NAME"]
            stats = table_stats_map.get(table_name, {"rows": 0, "bytes": 0})
            try: 
                ai_data = json.loads(row["CORTEX_JSON"])
                if isinstance(ai_data, list) and len(ai_data) > 0: ai_data = ai_data[0]
                if not isinstance(ai_data, dict): ai_data = {}
            except Exception: continue
            
            with st.expander(f"🗄️ NODE: {table_name}  |  📊 Rows: {stats.get('rows', 0):,}  |  💾 Size: {round(stats.get('bytes', 0) / (1024 * 1024), 2)} MB"):
                col_prompt, col_btn = st.columns([4, 1])
                with col_prompt: custom_prompt = st.text_input("INJECT CUSTOM DIRECTIVE:", key=f"prompt_{table_name}", label_visibility="collapsed")
                with col_btn:
                    if st.button("INJECT ✨", key=f"add_{table_name}", use_container_width=True):
                        if custom_prompt:
                            with st.spinner("COMPILING..."):
                                try:
                                    dq = chr(36) * 2
                                    json_prompt = f"Translate to SQL. Table: {selected_db}.{selected_schema}.{table_name}. Return ONLY valid JSON: {{\"rule_name\": \"...\", \"sql_check\": \"SELECT * FROM ... WHERE ...\", \"description\": \"{custom_prompt}\", \"is_active\": false}}"
                                    new_rule_str = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_MODEL}', {dq}{json_prompt}{dq})").collect()[0][0]
                                    s_idx, e_idx = new_rule_str.find('{'), new_rule_str.rfind('}')
                                    if s_idx != -1 and e_idx != -1: 
                                        new_rule_dict = json.loads(new_rule_str[s_idx:e_idx+1].replace('\n', ' '), strict=False)
                                        ai_data.setdefault('business_rules', []).append(new_rule_dict)
                                        session.sql(f"UPDATE {ENGINE_DB}.{ENGINE_SCHEMA}.AUTO_RULES_JSON SET CORTEX_JSON = PARSE_JSON({dq}{json.dumps(ai_data)}{dq}) WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND TABLE_NAME = '{table_name}'").collect()
                                        st.rerun() 
                                except Exception as e: st.error(f"> COMPILATION FAILED: {e}")

                st.divider()
                dq_rules = ai_data.get('dq_rules', [])
                biz_rules = ai_data.get('business_rules', [])
                for idx, r in enumerate(dq_rules): r.update({'ui_name': f"{r.get('dmf_name')} ➔ ({r.get('column_name')})", 'badge': '❄️ DMF', 'list_type': 'dq_rules', 'orig_idx': idx})
                for idx, r in enumerate(biz_rules): r.update({'ui_name': r.get('rule_name', 'Rule'), 'badge': '🏢 SQL', 'list_type': 'business_rules', 'orig_idx': idx})
                
                for i, rule in enumerate(dq_rules + biz_rules):
                    if not isinstance(rule, dict): continue
                    
                    with st.container(border=True):
                        col_title, col_toggle = st.columns([3, 1])
                        with col_title: st.markdown(f"**{rule['ui_name']}** `{rule['badge']}`")
                        with col_toggle: 
                            cur_state = rule.get("is_active", False)
                            new_state = st.toggle("ENGAGE", value=cur_state, key=f"tgl_{table_name}_{i}")
                            if new_state != cur_state:
                                rule["is_active"] = new_state
                                dq = chr(36) * 2
                                session.sql(f"UPDATE {ENGINE_DB}.{ENGINE_SCHEMA}.AUTO_RULES_JSON SET CORTEX_JSON = PARSE_JSON({dq}{json.dumps(ai_data)}{dq}) WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND TABLE_NAME = '{table_name}'").collect()
                                st.rerun()
                        
                        col_action, col_delete, col_sql = st.columns([3, 1, 2])
                        with col_action:
                            state_key = f"heal_state_{table_name}_{i}"
                            if state_key not in st.session_state: st.session_state[state_key] = "init"
                            if f"agent_feedback_{table_name}_{i}" not in st.session_state: st.session_state[f"agent_feedback_{table_name}_{i}"] = ""

                            if st.session_state[state_key] == "init":
                                if st.button(f"▶️ RUN AGENTIC HEAL", key=f"btn_test_{table_name}_{i}", type="secondary", use_container_width=True):
                                    st.session_state[state_key] = "processing"; st.rerun()

                            elif st.session_state[state_key] == "processing":
                                try:
                                    if rule['list_type'] == 'dq_rules':
                                        d_nm, c_nm = rule.get('dmf_name', ''), rule['column_name']
                                        if "NULL" in d_nm: bad_data = session.sql(f"SELECT * FROM {selected_db}.{selected_schema}.{table_name} WHERE {c_nm} IS NULL LIMIT 5").collect()
                                        elif "BLANK" in d_nm: bad_data = session.sql(f"SELECT * FROM {selected_db}.{selected_schema}.{table_name} WHERE TRIM({c_nm}::STRING) = '' LIMIT 5").collect()
                                        elif "UNIQUE" in d_nm or "DUPLICATE" in d_nm: bad_data = session.sql(f"SELECT * FROM {selected_db}.{selected_schema}.{table_name} WHERE {c_nm} IN (SELECT {c_nm} FROM {selected_db}.{selected_schema}.{table_name} GROUP BY {c_nm} HAVING COUNT(*) > 1) LIMIT 5").collect()
                                        else: bad_data = session.sql(f"SELECT * FROM {selected_db}.{selected_schema}.{table_name} WHERE {c_nm} IS NULL LIMIT 5").collect()
                                        f_ctx = f"DMF {d_nm} failed on {c_nm}."
                                    else:
                                        c_sql = re.sub(r'(?i)USE\s+(DATABASE|SCHEMA)\s+[a-zA-Z0-9_]+;', '', str(rule.get('sql_check', '')))
                                        bad_data = session.sql(c_sql + " LIMIT 5").collect()
                                        f_ctx = "SQL Check failed."
                                        
                                    if bad_data:
                                        att, succ, dq = 0, False, chr(36) * 2
                                        with st.status("🧠 INITIALIZING CORTEX AGENT...", expanded=True):
                                            while att < 3 and not succ:
                                                att += 1
                                                a_prompt = f"Heal data. Table: {selected_db}.{selected_schema}.{table_name}. Context: {f_ctx}. Rows: {str([r.as_dict() for r in bad_data])}. Feedback: {st.session_state[f'agent_feedback_{table_name}_{i}']}. Return STRICT JSON: 'thought_process', 'sql_action' (UPDATE query), and 'preview_data' (Array of fixed rows)."
                                                try:
                                                    raw_agent = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_MODEL}', {dq}{a_prompt}{dq})").collect()[0][0]
                                                    s_idx, e_idx = raw_agent.find('{'), raw_agent.rfind('}')
                                                    cln = json.loads(raw_agent[s_idx:e_idx+1], strict=False) if s_idx != -1 else {}
                                                    sql_exec = cln.get('sql_action', '')
                                                    session.sql("BEGIN").collect()
                                                    session.sql(sql_exec).collect()
                                                    session.sql("ROLLBACK").collect()
                                                    st.session_state[f"bad_data_{table_name}_{i}"] = [r.as_dict() for r in bad_data]
                                                    st.session_state[f"sql_fix_{table_name}_{i}"] = sql_exec
                                                    st.session_state[f"preview_{table_name}_{i}"] = cln.get('preview_data', [])
                                                    st.session_state[f"agent_feedback_{table_name}_{i}"] = "" 
                                                    st.session_state[state_key] = "review"
                                                    succ = True
                                                except Exception as ex:
                                                    session.sql("ROLLBACK").collect()
                                                    st.session_state[f"agent_feedback_{table_name}_{i}"] = f"SQL ERROR: '{str(ex)[:200]}'."
                                            if succ: st.rerun()
                                            else: 
                                                st.error("> AGENTIC SEQUENCE FAILED.")
                                                if st.button("ABORT", key=f"btn_back_{table_name}_{i}"): st.session_state[state_key] = "init"; st.rerun()
                                    else: 
                                        st.success("> NO ANOMALIES DETECTED.")
                                        time.sleep(1); st.session_state[state_key] = "init"; st.rerun()
                                except Exception as e: 
                                    st.error(f"Error: {e}")
                                    if st.button("🔙 ABORT", key=f"btn_err_exec_{table_name}_{i}"): st.session_state[state_key] = "init"; st.rerun()

                            elif st.session_state[state_key] == "review":
                                st.error("⚠️ ANOMALIES FOUND. REVIEW AGENT OVERRIDE:")
                                st.write("> **BEFORE (CORRUPTED):**")
                                st.dataframe(st.session_state[f"bad_data_{table_name}_{i}"], use_container_width=True)
                                st.write("> **AGENT SQL PROTOCOL:**")
                                st.code(st.session_state[f"sql_fix_{table_name}_{i}"], language="sql")
                                st.write("> **AFTER (PREVIEW):**")
                                st.dataframe(st.session_state[f"preview_{table_name}_{i}"], use_container_width=True)
                                
                                user_feedback = st.text_input("NLP ADJUSTMENT:", placeholder="e.g. Set to 'Unknown' instead of NULL", key=f"fb_{table_name}_{i}")
                                if st.button("🔄 RECALCULATE", key=f"btn_regen_{table_name}_{i}"):
                                    if user_feedback:
                                        st.session_state[f"agent_feedback_{table_name}_{i}"] = f"USER DIRECTIVE: {user_feedback}."
                                        st.session_state[state_key] = "processing"; st.rerun()
                                
                                col_btn1, col_btn2 = st.columns(2)
                                with col_btn1:
                                    if st.button("✅ AUTHORIZE", key=f"btn_confirm_{table_name}_{i}", type="primary", use_container_width=True):
                                        try:
                                            session.sql(st.session_state[f"sql_fix_{table_name}_{i}"]).collect()
                                            session.sql(f"INSERT INTO {ENGINE_DB}.{ENGINE_SCHEMA}.DQ_REMEDIATION_LOG (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, REMEDIATION_TYPE, ROWS_AFFECTED, STATUS, SQL_QUERY, EXECUTED_BY) VALUES ('{selected_db}', '{selected_schema}', '{table_name}', 'AGENTIC_AUTO_HEAL', -1, 'APPLIED', '{st.session_state[f'sql_fix_{table_name}_{i}'].replace(chr(39), chr(39)*2)}', '{current_user}')").collect()
                                            st.session_state[state_key] = "init"; st.rerun()
                                        except Exception as e: st.error(f"Error: {e}")
                                with col_btn2:
                                    if st.button("❌ CANCEL", key=f"btn_cancel_{table_name}_{i}", use_container_width=True):
                                        st.session_state[state_key] = "init"; st.rerun()

                        with col_delete:
                            if st.button(f"🗑️ PURGE", key=f"btn_del_{table_name}_{i}", type="primary", use_container_width=True):
                                ai_data[rule['list_type']].pop(rule['orig_idx'])
                                dq = chr(36) * 2
                                session.sql(f"UPDATE {ENGINE_DB}.{ENGINE_SCHEMA}.AUTO_RULES_JSON SET CORTEX_JSON = PARSE_JSON({dq}{json.dumps(ai_data)}{dq}) WHERE DATABASE_NAME = '{selected_db}' AND SCHEMA_NAME = '{selected_schema}' AND TABLE_NAME = '{table_name}'").collect()
                                st.rerun()
                        with col_sql:
                            with st.expander("🔍 View Definition"): 
                                if rule['list_type'] == 'dq_rules': st.code(f"ALTER TABLE {table_name} ADD DATA METRIC FUNCTION {rule['dmf_name']} ON ({rule['column_name']});", language="sql")
                                else: st.code(rule.get('sql_check', ''), language="sql")

with tab6:
    st.markdown("### 🔋 RESOURCE TELEMETRY (COST MONITOR)")
    try:
        c_df = session.sql("SELECT START_TIME::DATE AS USAGE_DATE, SUM(CREDITS_USED) AS TOTAL_CREDITS FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()))) GROUP BY 1 ORDER BY 1 DESC").collect()
        if c_df: st.bar_chart(pd.DataFrame(c_df).set_index("USAGE_DATE"))
    except Exception as e: st.error(f"Telemetry offline: {e}")