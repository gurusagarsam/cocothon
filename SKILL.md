# 🛠️ SYSTEM BLUEPRINT & COMPETENCY MATRIX: AutoHeal.ai (SKILL.md)

## 📌 1. EXECUTIVE SUMMARY & SYSTEM OVERVIEW
**Project Name:** AutoHeal.ai
**Platform:** Snowflake (Snowpark Python, Streamlit in Snowflake, Cortex AI)
**Objective:** An autonomous Data Quality (DQ) and Active Healing platform. It shifts the paradigm from passive alerting to active healing by using Snowflake Cortex to profile schemas, enforce data metric functions (DMFs) via a Virtual Engine, and execute Agentic ReAct workflows to auto-heal corrupted data with Human-in-the-Loop (HITL) approval.

**Our Philosophy:** *"The Brain (Cortex) Suggests, The Muscle (Snowpark) Executes, The Human Approves."*

---

## 🗄️ 2. BACKEND INFRASTRUCTURE (`setup.sql`)
The backend relies on stateful tracking tables stored in `SYSTEM_DQ_DB.CONFIG`. Any AI assistant or developer extending this code must strictly adhere to these schemas when generating SQL queries.

### 2.1 Core Tracking Tables
* **`AUTO_RULES_JSON`**: Stores AI-generated rules. `(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CORTEX_JSON, CREATED_AT)`
* **`DQ_RULE_RESULTS`**: Tracks pass/fail state of executed rules. `(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, RULE_TYPE, RULE_NAME, DESCRIPTION, VIOLATION_COUNT, SQL_CHECK, IS_RESOLVED, FIRST_SEEN_AT, LAST_SEEN_AT)`
* **`DQ_FRESHNESS_RESULTS`**: Tracks temporal staleness of data. `(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TIMESTAMP_COLUMN, TOTAL_ROWS, STALE_ROWS, FRESHNESS_PCT, OLDEST_RECORD, NEWEST_RECORD, STALENESS_THRESHOLD_DAYS, STATUS)`
* **`DQ_ALERTS`**: Active incident dispatch queue. `(SEVERITY, ALERT_TYPE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, RULE_NAME, MESSAGE, METRIC_VALUE, IS_RESOLVED)`
* **`PROFILER_TRACKING`**: UI state tracking for the background scanner. `(DATABASE_NAME, SCHEMA_NAME, TOTAL_TABLES, PROFILED_TABLES, CURRENT_TABLE, STATUS)`
* **`DQ_REMEDIATION_LOG`**: Immutable audit log and pending queue for AI fixes. `(LOG_ID, RUN_TIMESTAMP, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, REMEDIATION_TYPE, ROWS_AFFECTED, STATUS, SQL_QUERY, EXECUTED_BY)`

### 2.2 Python Stored Procedures (Snowpark)
* **`run_background_dq_profiler`**: Iterates through `SHOW TABLES`, extracts schema/DDL, and calls `SNOWFLAKE.CORTEX.COMPLETE` to generate JSON payloads containing structural DQ rules.
* **`run_dq_monitor`**: The "Virtual Engine". Evaluates freshness, translates native DMF names (e.g., `SNOWFLAKE.CORE.NULL_COUNT`) into standard SQL aggregates for Standard Edition compatibility, evaluates business rules, and updates the Alerts tables.

---

## 🎨 3. FRONTEND UI/UX ARCHITECTURE (Streamlit)
The frontend (`app.py`) uses custom HTML/CSS to bypass Streamlit limitations and Content Security Policy (CSP) blocks, ensuring a cinematic experience.

### 3.1 Cinematic Features (Must Preserve in Edits)
* **JARVIS Boot Sequence:** A full-screen `fixed` HTML overlay with spinning CSS arc-reactors, typing terminal text, and a native browser JS Text-to-Speech (`window.speechSynthesis`) welcoming the `CURRENT_USER()`. Controlled via `st.session_state["jarvis_booted"]`.
* **Ambient Audio:** An invisible iframe injecting a 5% volume looping MP3 track to bypass browser autoplay/CSP restrictions.
* **Breathing Gradient Background:** `.stApp` and `[data-testid="stAppViewContainer"]` overridden with a 12s animated CSS gradient.
* **Floating Profiler Widget:** An absolute-positioned HTML widget displaying real-time scan progress in the bottom right corner.

### 3.2 Application Tabs
1. **📊 Health Dashboard:** Custom HTML CSS dial gauge showing System Integrity %. Real-time metrics for monitored rows, rules, and anomalies.
2. **🚨 Active Alerts & Reports:** Displays unresolved anomalies. Includes a Python-to-HTML converter to dispatch styled incident reports via `SYSTEM$SEND_EMAIL`.
3. **⏱️ Data Freshness:** DataFrame view of `DQ_FRESHNESS_RESULTS`.
4. **🧹 Auto-Remediation Log & Queue:** Displays `PENDING` queries for execution approval, and `APPLIED` queries for historical auditing.
5. **🧠 Agentic Rule Engine:** The core ReAct interface. Allows toggling rules, injecting custom prompts, and executing the Auto-Heal sandbox.
6. **💰 Resource & Cost Telemetry:** Tracks `WAREHOUSE_METERING_HISTORY` and filters `ACCOUNT_USAGE.METERING_HISTORY` for `SERVICE_TYPE = 'CORTEX_LLM'` to isolate AI API costs.

---

## ☁️ 4. TECHNICAL COMPETENCY MATRIX (SKILLS APPLIED)

### Cloud & Data Warehousing (Snowflake)
* **Native Application Development:** Building self-contained, secure applications using Streamlit in Snowflake (SiS).
* **Snowpark Python:** Leveraging Snowpark dataframes and execution logic for high-performance, server-side data processing.
* **Stateful Architectures:** Designing robust metadata tracking tables, `MERGE INTO` logic, and schema-agnostic dynamic SQL execution.
* **Security & RBAC:** Utilizing `CURRENT_USER()` for audit logging and Snowflake Notification Integrations for secure outbound communication.

### Artificial Intelligence & LLM Orchestration
* **Snowflake Cortex AI:** Direct integration with cutting-edge models (`claude-opus-4-6`) via `SNOWFLAKE.CORTEX.COMPLETE`.
* **Agentic ReAct Framework:** Engineering multi-step "Reason and Act" workflows where the LLM writes code, evaluates execution feedback, and self-corrects.
* **Zero-Shot Prompt Engineering:** Designing highly constrained prompts that force the LLM to analyze raw DDL/schema metadata and return strictly structured JSON payloads.
* **Transactional Sandboxing:** Using `BEGIN...ROLLBACK` boundaries to safely execute and validate AI-generated SQL inside the data warehouse.

### Software Engineering & Frontend
* **Dynamic Code Generation:** Using Python string manipulation and regex to dynamically build and sanitize complex SQL statements.
* **State Machine Implementation:** Managing complex UI/UX flows (Init -> Processing -> Review) within Streamlit's top-to-bottom execution model using `st.session_state`.
* **Advanced CSS3 & DOM Manipulation:** Bypassing standard Streamlit limitations using pure CSS keyframes, pseudo-elements, and invisible HTML wrappers.

---

## 🤖 5. CORTEX AI PROMPT ENGINEERING PLAYBOOK
The application relies on highly constrained Zero-Shot prompting. Any modifications made by AI assistants must maintain strict JSON enforcement.

### 5.1 Schema Profiler Prompt
**Objective:** Force the LLM to read raw table structures and output machine-readable Data Quality rules without hallucinating unsupported functions.

```text
You are an expert Data Architect. Analyze the following table: {target_db}.{target_schema}.{table_name}
Schema: {schema_str} | DDL: {ddl_str} | Sample data: {sample_str}

Generate exactly 3 Data Quality rules using native Snowflake DMFs and 2 Business Logic checks using standard SQL.
CRITICAL 1: 'dq_rules' MUST use ONLY: SNOWFLAKE.CORE.NULL_COUNT, SNOWFLAKE.CORE.UNIQUE_COUNT, SNOWFLAKE.CORE.BLANK_COUNT, SNOWFLAKE.CORE.DUPLICATE_COUNT.
CRITICAL 2: 'business_rules' MUST be a single SELECT returning violating rows.
Return ONLY valid JSON: {{"table": "...", "summary": "...", "dq_rules": [...], "business_rules": [...]}}```

### 5.2 Agentic Auto-Healer Prompt (ReAct)
**Objective:**  The AI must act as a Data Engineer, reason about the failure, write a SQL fix, and provide a preview of the corrected data.

```text
You are an autonomous Data Quality Agent. Heal broken data.
Table: {target_db}.{target_schema}.{table_name} | Failed Context: {f_ctx} | Sample Bad Rows: {sample_bad_rows}
Feedback from User: {agent_feedback}

Return STRICT JSON:
{{
    "thought_process": "Explain reasoning.",
    "sql_action": "The exact UPDATE or DELETE statement. No markdown.",
    "preview_data": [ {{ "COL1": "Fixed" }} ]
}}```

### 5.3 State Machine for Agentic ReAct
The UI manages the Agentic flow using `st.session_state[f"heal_state_{table_name}_{i}"]`:

1. **`init`**: Shows "Test Rule & Auto-Heal" button.
2. **`processing`**: Calls Cortex with Prompt 5.2. Tests `sql_action` inside `BEGIN; ... ROLLBACK;`. If compilation fails, catches Exception and auto-retries (Max 3).
3. **`review`**: Renders Before, SQL, and After preview. Accepts Human NLP feedback.
* If User Input -> State goes to `processing` with `{agent_feedback}` injected.
* If Confirmed -> Executes `sql_action`, logs to `DQ_REMEDIATION_LOG`, State returns to `init`.