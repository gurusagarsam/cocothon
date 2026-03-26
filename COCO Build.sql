-- ====================================================================
-- AUTOHEAL.AI: MASTER BACKEND SETUP (STANDARD EDITION SAFE)
-- ====================================================================

CREATE DATABASE IF NOT EXISTS SYSTEM_DQ_DB;
CREATE SCHEMA IF NOT EXISTS SYSTEM_DQ_DB.CONFIG;

-- 1. TRACKING & METADATA TABLES
CREATE TABLE IF NOT EXISTS SYSTEM_DQ_DB.CONFIG.AUTO_RULES_JSON (
    DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, CORTEX_JSON VARIANT, CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SYSTEM_DQ_DB.CONFIG.DQ_RULE_RESULTS (
    DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, RULE_TYPE VARCHAR, RULE_NAME VARCHAR, DESCRIPTION VARCHAR, VIOLATION_COUNT INT, SQL_CHECK VARCHAR, 
    IS_RESOLVED BOOLEAN DEFAULT FALSE, FIRST_SEEN_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), LAST_SEEN_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SYSTEM_DQ_DB.CONFIG.DQ_FRESHNESS_RESULTS (
    DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, TIMESTAMP_COLUMN VARCHAR, TOTAL_ROWS INT, STALE_ROWS INT, FRESHNESS_PCT FLOAT, OLDEST_RECORD TIMESTAMP, NEWEST_RECORD TIMESTAMP, STALENESS_THRESHOLD_DAYS INT, STATUS VARCHAR,
    FIRST_SEEN_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), LAST_SEEN_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SYSTEM_DQ_DB.CONFIG.DQ_ALERTS (
    SEVERITY VARCHAR, ALERT_TYPE VARCHAR, DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, RULE_NAME VARCHAR, MESSAGE VARCHAR, METRIC_VALUE FLOAT,
    IS_RESOLVED BOOLEAN DEFAULT FALSE, FIRST_SEEN_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), LAST_SEEN_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SYSTEM_DQ_DB.CONFIG.PROFILER_TRACKING (
    DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TOTAL_TABLES INT, PROFILED_TABLES INT, CURRENT_TABLE VARCHAR, STATUS VARCHAR, START_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), END_TIME TIMESTAMP
);

-- 🚀 AUDITABLE REMEDIATION LOG (Pending Queue & History)
CREATE OR REPLACE TABLE SYSTEM_DQ_DB.CONFIG.DQ_REMEDIATION_LOG (
    LOG_ID VARCHAR DEFAULT UUID_STRING(),
    RUN_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, COLUMN_NAME VARCHAR, 
    REMEDIATION_TYPE VARCHAR, ROWS_AFFECTED INT, 
    STATUS VARCHAR, -- 'PENDING' or 'APPLIED'
    SQL_QUERY VARCHAR, 
    EXECUTED_BY VARCHAR
);

-- ====================================================================
-- 2. HACKATHON DEMO ENVIRONMENT
-- ====================================================================
CREATE DATABASE IF NOT EXISTS HACKATHON_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS HACKATHON_DEMO_DB.HR;
CREATE OR REPLACE TABLE HACKATHON_DEMO_DB.HR.EMPLOYEES (
    EMP_ID INT, FIRST_NAME VARCHAR, LAST_NAME VARCHAR, EMAIL VARCHAR, SALARY NUMBER(10,2), HIRE_DATE DATE, DEPARTMENT VARCHAR, LAST_MODIFIED TIMESTAMP
);

TRUNCATE TABLE HACKATHON_DEMO_DB.HR.EMPLOYEES;
INSERT INTO HACKATHON_DEMO_DB.HR.EMPLOYEES VALUES
(101, 'John', 'Doe', 'john.doe@company.com', 85000, '2023-01-15', 'IT', CURRENT_TIMESTAMP()),
(102, 'Jane', 'Smith', '  JANE.smith@COMPANY.com  ', -50000, '2022-06-01', 'HR', DATEADD('day', -90, CURRENT_TIMESTAMP())),
(NULL, 'Bob', 'Jones', 'bob.jones@company.com', 75000, '2025-12-01', 'Sales', DATEADD('day', -45, CURRENT_TIMESTAMP())),
(104, 'Alice ', NULL, 'alice@company.com', 95000, '2021-03-20', 'IT', DATEADD('day', -3, CURRENT_TIMESTAMP())),
(105, 'Charlie', 'Brown', 'charlie.b@company.com', NULL, '2023-08-10', 'Marketing', DATEADD('day', -60, CURRENT_TIMESTAMP()));

-- ====================================================================
-- 3. BULLETPROOF AI PROFILER
-- ====================================================================
CREATE OR REPLACE PROCEDURE SYSTEM_DQ_DB.CONFIG.run_background_dq_profiler(TARGET_DB STRING, TARGET_SCHEMA STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION = '3.10' PACKAGES = ('snowflake-snowpark-python') HANDLER = 'main'
AS $$
import snowflake.snowpark.functions as F
import json

def main(session, target_db, target_schema):
    tables_df = session.sql(f"SHOW TABLES IN SCHEMA {target_db}.{target_schema}").collect()
    tables = [row["name"] for row in tables_df if row["name"] != 'INFORMATION_SCHEMA']
    total_tables = len(tables)
    
    if total_tables == 0: 
        return "No tables found."

    session.sql(f"""
        MERGE INTO SYSTEM_DQ_DB.CONFIG.PROFILER_TRACKING t 
        USING (SELECT '{target_db}' AS db, '{target_schema}' AS sch) s 
        ON t.DATABASE_NAME = s.db AND t.SCHEMA_NAME = s.sch
        WHEN MATCHED THEN UPDATE SET TOTAL_TABLES={total_tables}, PROFILED_TABLES=0, STATUS='RUNNING', CURRENT_TABLE='Initializing...', START_TIME=CURRENT_TIMESTAMP(), END_TIME=NULL
        WHEN NOT MATCHED THEN INSERT (DATABASE_NAME, SCHEMA_NAME, TOTAL_TABLES, PROFILED_TABLES, CURRENT_TABLE, STATUS) 
        VALUES ('{target_db}', '{target_schema}', {total_tables}, 0, 'Initializing...', 'RUNNING')
    """).collect()

    for i, table_name in enumerate(tables):
        session.sql(f"UPDATE SYSTEM_DQ_DB.CONFIG.PROFILER_TRACKING SET CURRENT_TABLE='{table_name}' WHERE DATABASE_NAME='{target_db}' AND SCHEMA_NAME='{target_schema}'").collect()
        
        cols = session.sql(f"DESCRIBE TABLE {target_db}.{target_schema}.{table_name}").collect()
        schema_str = ", ".join([f"{r['name']} ({r['type']})" for r in cols])
        
        try: 
            ddl_str = session.sql(f"SELECT GET_DDL('TABLE', '{target_db}.{target_schema}.{table_name}') AS DDL").collect()[0]["DDL"].replace("'", "''")
        except Exception: 
            ddl_str = "No specific DDL available."
            
        sample_str = str([row.as_dict() for row in session.sql(f"SELECT * FROM {target_db}.{target_schema}.{table_name} LIMIT 3").collect()]).replace("'", "''") 
        
        prompt = f"""
        You are an expert Data Architect. Analyze the following table: {target_db}.{target_schema}.{table_name}
        Schema: {schema_str}
        DDL / Lineage: {ddl_str}
        Sample data: {sample_str}
        
        Generate exactly 3 Data Quality rules using native Snowflake Data Metric Functions (DMFs) and 2 Business Logic checks using standard SQL. 
        CRITICAL 1: For 'dq_rules', you MUST use ONLY these allowed Snowflake native DMFs: SNOWFLAKE.CORE.NULL_COUNT, SNOWFLAKE.CORE.UNIQUE_COUNT, SNOWFLAKE.CORE.BLANK_COUNT, SNOWFLAKE.CORE.DUPLICATE_COUNT.
        CRITICAL 2: For 'dq_rules', you MUST specify the exact 'column_name' from the schema that the DMF applies to.
        CRITICAL 3: For 'business_rules', your 'sql_check' MUST be a single SELECT statement returning violating rows using the fully qualified name {target_db}.{target_schema}.{table_name}.
        
        Return ONLY a valid JSON object matching exactly this format (note is_active is false):
        {{"table": "{table_name}", "summary": "...", "dq_rules": [{{"dmf_name": "SNOWFLAKE.CORE.NULL_COUNT", "column_name": "EMP_ID", "description": "...", "is_active": false}}], "business_rules": [{{"rule_name": "...", "sql_check": "SELECT * FROM {target_db}.{target_schema}.{table_name} WHERE ...", "description": "...", "is_active": false}}]}}
        """
        
        try:
            dq = chr(36) * 2
            ai_response = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-6', {dq}{prompt}{dq}) AS AI_REC").collect()[0]["AI_REC"]
            
            start_idx, end_idx = ai_response.find('{'), ai_response.rfind('}')
            if start_idx != -1 and end_idx != -1: 
                ai_response = ai_response[start_idx:end_idx+1]
                
            session.sql(f"DELETE FROM SYSTEM_DQ_DB.CONFIG.AUTO_RULES_JSON WHERE DATABASE_NAME = '{target_db}' AND SCHEMA_NAME = '{target_schema}' AND TABLE_NAME = '{table_name}'").collect()
            session.sql(f"INSERT INTO SYSTEM_DQ_DB.CONFIG.AUTO_RULES_JSON (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CORTEX_JSON) SELECT '{target_db}', '{target_schema}', '{table_name}', TRY_PARSE_JSON({dq}{ai_response}{dq})").collect()
        
        except Exception as e:
            session.sql(f"UPDATE SYSTEM_DQ_DB.CONFIG.PROFILER_TRACKING SET STATUS='ERROR on {table_name}: {str(e).replace(chr(39), chr(39)*2)[:150]}' WHERE DATABASE_NAME='{target_db}' AND SCHEMA_NAME='{target_schema}'").collect()
            continue
            
        session.sql(f"UPDATE SYSTEM_DQ_DB.CONFIG.PROFILER_TRACKING SET PROFILED_TABLES={i+1} WHERE DATABASE_NAME='{target_db}' AND SCHEMA_NAME='{target_schema}'").collect()
        
    session.sql(f"UPDATE SYSTEM_DQ_DB.CONFIG.PROFILER_TRACKING SET STATUS='COMPLETED', CURRENT_TABLE='None', END_TIME=CURRENT_TIMESTAMP() WHERE DATABASE_NAME='{target_db}' AND SCHEMA_NAME='{target_schema}'").collect()
    return "Profiling complete"
$$;

-- ====================================================================
-- 4. STATEFUL CONTINUOUS MONITOR (VIRTUAL ENGINE & QUEUE)
-- ====================================================================
CREATE OR REPLACE PROCEDURE SYSTEM_DQ_DB.CONFIG.run_dq_monitor(TARGET_DB STRING, TARGET_SCHEMA STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION = '3.10' PACKAGES = ('snowflake-snowpark-python') HANDLER = 'main'
AS $$
def main(session, target_db, target_schema):
    import re
    dq = chr(36) * 2
    STALENESS_THRESHOLD_DAYS, FRESHNESS_CRITICAL_PCT, FRESHNESS_WARNING_PCT, VIOLATION_CRITICAL_THRESHOLD = 30, 70, 90, 5

    def fire_alert(severity, alert_type, db, schema, table, rule_name, message, metric_value):
        safe_msg = str(message).replace("'", "''")
        safe_rule = str(rule_name).replace("'", "''")
        session.sql(f"""
            MERGE INTO SYSTEM_DQ_DB.CONFIG.DQ_ALERTS a 
            USING (SELECT '{severity}' AS SEV, '{alert_type}' AS ATYPE, '{db}' AS DB, '{schema}' AS SCH, '{table}' AS TAB, '{safe_rule}' AS RNAME, '{safe_msg}' AS MSG, {metric_value} AS MVAL) s
            ON a.DATABASE_NAME = s.DB AND a.SCHEMA_NAME = s.SCH AND a.TABLE_NAME = s.TAB AND a.RULE_NAME = s.RNAME AND a.ALERT_TYPE = s.ATYPE
            WHEN MATCHED THEN UPDATE SET MESSAGE = s.MSG, METRIC_VALUE = s.MVAL, LAST_SEEN_AT = CURRENT_TIMESTAMP(), IS_RESOLVED = FALSE, SEVERITY = s.SEV
            WHEN NOT MATCHED THEN INSERT (SEVERITY, ALERT_TYPE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, RULE_NAME, MESSAGE, METRIC_VALUE, IS_RESOLVED, FIRST_SEEN_AT, LAST_SEEN_AT) 
            VALUES (s.SEV, s.ATYPE, s.DB, s.SCH, s.TAB, s.RNAME, s.MSG, s.MVAL, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """).collect()

    # 1. DATA FRESHNESS LOGIC
    try:
        ts_cols_df = session.sql(f"SELECT TABLE_NAME, COLUMN_NAME FROM {target_db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{target_schema}' AND (DATA_TYPE LIKE '%TIMESTAMP%' OR DATA_TYPE LIKE '%DATE%')").collect()
        for row in ts_cols_df:
            table = row["TABLE_NAME"]
            ts_col = row["COLUMN_NAME"]
            fqn = f"{target_db}.{target_schema}.{table}"
            try:
                query = f"SELECT COUNT({ts_col}) AS TOTAL_VAL, MIN({ts_col}) AS OLD_VAL, MAX({ts_col}) AS NEW_VAL, COUNT(CASE WHEN DATEDIFF('day', {ts_col}::TIMESTAMP_NTZ, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) > {STALENESS_THRESHOLD_DAYS} THEN 1 END) AS STALE_VAL FROM {fqn}"
                runtime_stats = session.sql(query).collect()[0]
                
                total = runtime_stats["TOTAL_VAL"] or 0
                stale = runtime_stats["STALE_VAL"] or 0
                pct = round(((total - stale) / total * 100), 2) if total > 0 else 0.0
                f_status = "FRESH" if stale == 0 else ("STALE" if pct < 50 else "WARNING")
                
                old_val = runtime_stats["OLD_VAL"]
                new_val = runtime_stats["NEW_VAL"]
                old_str = f"'{old_val}'::TIMESTAMP" if old_val else "NULL"
                new_str = f"'{new_val}'::TIMESTAMP" if new_val else "NULL"
                
                session.sql(f"""
                    MERGE INTO SYSTEM_DQ_DB.CONFIG.DQ_FRESHNESS_RESULTS f 
                    USING (SELECT '{target_db}' AS DB, '{target_schema}' AS SCH, '{table}' AS TAB, '{ts_col}' AS COL, {total} AS TOT, {stale} AS STL, {pct} AS PCT, {old_str} AS OLD, {new_str} AS NEW, '{f_status}' AS STAT) s
                    ON f.DATABASE_NAME = s.DB AND f.SCHEMA_NAME = s.SCH AND f.TABLE_NAME = s.TAB AND f.TIMESTAMP_COLUMN = s.COL
                    WHEN MATCHED THEN UPDATE SET TOTAL_ROWS=s.TOT, STALE_ROWS=s.STL, FRESHNESS_PCT=s.PCT, OLDEST_RECORD=s.OLD, NEWEST_RECORD=s.NEW, STATUS=s.STAT, LAST_SEEN_AT=CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TIMESTAMP_COLUMN, TOTAL_ROWS, STALE_ROWS, FRESHNESS_PCT, OLDEST_RECORD, NEWEST_RECORD, STALENESS_THRESHOLD_DAYS, STATUS, FIRST_SEEN_AT, LAST_SEEN_AT) 
                    VALUES (s.DB, s.SCH, s.TAB, s.COL, s.TOT, s.STL, s.PCT, s.OLD, s.NEW, {STALENESS_THRESHOLD_DAYS}, s.STAT, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """).collect()
                
                if pct < FRESHNESS_CRITICAL_PCT: 
                    fire_alert("CRITICAL", "FRESHNESS", target_db, target_schema, table, ts_col, f"Freshness at {pct}% on {fqn}.{ts_col}", pct)
                elif pct < FRESHNESS_WARNING_PCT: 
                    fire_alert("WARNING", "FRESHNESS", target_db, target_schema, table, ts_col, f"Freshness at {pct}% on {fqn}.{ts_col}", pct)
                else: 
                    session.sql(f"UPDATE SYSTEM_DQ_DB.CONFIG.DQ_ALERTS SET IS_RESOLVED = TRUE, LAST_SEEN_AT = CURRENT_TIMESTAMP() WHERE DATABASE_NAME='{target_db}' AND SCHEMA_NAME='{target_schema}' AND TABLE_NAME='{table}' AND RULE_NAME='{ts_col}' AND ALERT_TYPE='FRESHNESS'").collect()
            
            except Exception as e: 
                fire_alert("WARNING", "FRESHNESS_ERROR", target_db, target_schema, table, ts_col, f"Freshness Check Failed: {str(e)[:150]}", -1)
    except Exception as e: pass

    # 2. PENDING REMEDIATION QUEUE LOGIC
    try:
        tables = session.sql(f"SELECT DISTINCT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME FROM SYSTEM_DQ_DB.CONFIG.AUTO_RULES_JSON WHERE DATABASE_NAME='{target_db}' AND SCHEMA_NAME='{target_schema}' AND CORTEX_JSON IS NOT NULL").collect()
        for tbl in tables:
            db = tbl["DATABASE_NAME"]
            schema = tbl["SCHEMA_NAME"]
            table = tbl["TABLE_NAME"]
            fqn = f"{db}.{schema}.{table}"
            
            try: 
                cols = session.sql(f"DESCRIBE TABLE {fqn}").collect()
            except Exception: 
                continue
                
            for col in [c["name"] for c in cols if "VARCHAR" in c["type"].upper() or "STRING" in c["type"].upper()]:
                try:
                    cnt = session.sql(f"SELECT COUNT(*) AS CNT FROM {fqn} WHERE {col} != TRIM({col})").collect()[0]["CNT"]
                    if cnt > 0:
                        sql_query = f"UPDATE {fqn} SET {col} = TRIM({col}) WHERE {col} != TRIM({col})"
                        exists = session.sql(f"SELECT COUNT(*) as C FROM SYSTEM_DQ_DB.CONFIG.DQ_REMEDIATION_LOG WHERE TABLE_NAME='{table}' AND COLUMN_NAME='{col}' AND STATUS='PENDING' AND REMEDIATION_TYPE='TRIM_WHITESPACE'").collect()[0]["C"]
                        if exists == 0:
                            session.sql(f"INSERT INTO SYSTEM_DQ_DB.CONFIG.DQ_REMEDIATION_LOG (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, REMEDIATION_TYPE, ROWS_AFFECTED, STATUS, SQL_QUERY, EXECUTED_BY) VALUES ('{db}', '{schema}', '{table}', '{col}', 'TRIM_WHITESPACE', {cnt}, 'PENDING', '{sql_query}', 'SYSTEM')").collect()
                except Exception: pass 
                
            for col in [c["name"] for c in cols if "EMAIL" in c["name"].upper()]:
                try:
                    cnt = session.sql(f"SELECT COUNT(*) AS CNT FROM {fqn} WHERE {col} != LOWER(TRIM({col}))").collect()[0]["CNT"]
                    if cnt > 0:
                        sql_query = f"UPDATE {fqn} SET {col} = LOWER(TRIM({col})) WHERE {col} != LOWER(TRIM({col}))"
                        exists = session.sql(f"SELECT COUNT(*) as C FROM SYSTEM_DQ_DB.CONFIG.DQ_REMEDIATION_LOG WHERE TABLE_NAME='{table}' AND COLUMN_NAME='{col}' AND STATUS='PENDING' AND REMEDIATION_TYPE='LOWERCASE_EMAIL'").collect()[0]["C"]
                        if exists == 0:
                            session.sql(f"INSERT INTO SYSTEM_DQ_DB.CONFIG.DQ_REMEDIATION_LOG (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, REMEDIATION_TYPE, ROWS_AFFECTED, STATUS, SQL_QUERY, EXECUTED_BY) VALUES ('{db}', '{schema}', '{table}', '{col}', 'LOWERCASE_EMAIL', {cnt}, 'PENDING', '{sql_query}', 'SYSTEM')").collect()
                except Exception: pass
    except Exception: pass

    # 3. RULE EVALUATION (VIRTUAL ENGINE)
    try:
        raw_rules = session.sql(f"""
            SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, f.value:dmf_name::STRING AS dmf_name, f.value:column_name::STRING AS column_name, f.value:description::STRING AS description, 'DQ_DMF' AS rule_type, NULL AS sql_check 
            FROM SYSTEM_DQ_DB.CONFIG.AUTO_RULES_JSON, LATERAL FLATTEN(input => CORTEX_JSON:dq_rules) f 
            WHERE DATABASE_NAME = '{target_db}' AND SCHEMA_NAME = '{target_schema}' AND COALESCE(f.value:is_active::BOOLEAN, FALSE) = TRUE 
            UNION ALL 
            SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, f.value:rule_name::STRING AS dmf_name, NULL AS column_name, f.value:description::STRING AS description, 'BUSINESS' AS rule_type, f.value:sql_check::STRING AS sql_check 
            FROM SYSTEM_DQ_DB.CONFIG.AUTO_RULES_JSON, LATERAL FLATTEN(input => CORTEX_JSON:business_rules) f 
            WHERE DATABASE_NAME = '{target_db}' AND SCHEMA_NAME = '{target_schema}' AND COALESCE(f.value:is_active::BOOLEAN, FALSE) = TRUE
        """).collect()
        
        for raw_row in raw_rules:
            rule = raw_row.as_dict()
            try: 
                if rule['RULE_TYPE'] == 'DQ_DMF':
                    dmf_nm = str(rule.get('DMF_NAME') or '').strip().upper()
                    col_nm = str(rule.get('COLUMN_NAME') or '').strip()
                    fqn = f"{rule['DATABASE_NAME']}.{rule['SCHEMA_NAME']}.{rule['TABLE_NAME']}"
                    
                    if "NULL_COUNT" in dmf_nm: 
                        cnt = session.sql(f"SELECT COUNT(CASE WHEN {col_nm} IS NULL THEN 1 END) AS CNT FROM {fqn}").collect()[0]["CNT"]
                    elif "UNIQUE_COUNT" in dmf_nm or "DUPLICATE_COUNT" in dmf_nm: 
                        cnt = session.sql(f"SELECT COUNT({col_nm}) - COUNT(DISTINCT {col_nm}) AS CNT FROM {fqn}").collect()[0]["CNT"]
                    elif "BLANK_COUNT" in dmf_nm: 
                        cnt = session.sql(f"SELECT COUNT(CASE WHEN TRIM({col_nm}::STRING) = '' THEN 1 END) AS CNT FROM {fqn}").collect()[0]["CNT"]
                    else: 
                        cnt = session.sql(f"SELECT COUNT(CASE WHEN {col_nm} IS NULL THEN 1 END) AS CNT FROM {fqn}").collect()[0]["CNT"]
                    
                    safe_chk = f"Translated Engine: {dmf_nm} on column {col_nm}"
                    safe_rule_name = f"{dmf_nm} ({col_nm})"
                else:
                    raw_sql = str(rule.get('SQL_CHECK') or '')
                    if not raw_sql.strip(): 
                        raise ValueError("AI generated empty SQL")
                    clean_sql = re.sub(r'(?i)USE\s+(DATABASE|SCHEMA)\s+[a-zA-Z0-9_]+;', '', raw_sql).strip().rstrip(';')
                    cnt = session.sql(f"SELECT COUNT(*) AS CNT FROM ({clean_sql})").collect()[0]["CNT"]
                    safe_chk = raw_sql.replace("'", "''")
                    safe_rule_name = str(rule.get('DMF_NAME') or 'Unknown Rule').replace("'", "''")
                    
            except Exception as e: 
                cnt = -1
                fallback_name = str(rule.get('DMF_NAME') or 'Unknown Rule').replace("'", "''")
                fire_alert("CRITICAL", "RULE_ERROR", rule["DATABASE_NAME"], rule["SCHEMA_NAME"], rule["TABLE_NAME"], fallback_name, f"Check Failed: {str(e)[:150]}", -1)
                continue
                
            is_res = 'TRUE' if cnt == 0 else 'FALSE'
            safe_desc = str(rule.get('DESCRIPTION') or '-- Missing Description').replace("'", "''")
            
            session.sql(f"""
                MERGE INTO SYSTEM_DQ_DB.CONFIG.DQ_RULE_RESULTS r 
                USING (SELECT '{rule['DATABASE_NAME']}' AS DB, '{rule['SCHEMA_NAME']}' AS SCH, '{rule['TABLE_NAME']}' AS TAB, '{rule['RULE_TYPE']}' AS RTYPE, '{safe_rule_name}' AS RNAME, '{safe_desc}' AS DESCR, {cnt} AS CNT, '{safe_chk}' AS CHK, {is_res} AS RES) s 
                ON r.DATABASE_NAME = s.DB AND r.SCHEMA_NAME = s.SCH AND r.TABLE_NAME = s.TAB AND r.RULE_NAME = s.RNAME 
                WHEN MATCHED THEN UPDATE SET VIOLATION_COUNT = s.CNT, IS_RESOLVED = s.RES, LAST_SEEN_AT = CURRENT_TIMESTAMP(), SQL_CHECK = s.CHK 
                WHEN NOT MATCHED THEN INSERT (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, RULE_TYPE, RULE_NAME, DESCRIPTION, VIOLATION_COUNT, SQL_CHECK, IS_RESOLVED, FIRST_SEEN_AT, LAST_SEEN_AT) 
                VALUES (s.DB, s.SCH, s.TAB, s.RTYPE, s.RNAME, s.DESCR, s.CNT, s.CHK, s.RES, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """).collect()
            
            if cnt > VIOLATION_CRITICAL_THRESHOLD: 
                fire_alert("CRITICAL", "VIOLATION", rule["DATABASE_NAME"], rule["SCHEMA_NAME"], rule["TABLE_NAME"], safe_rule_name, f"{cnt} violations for {safe_rule_name}", cnt)
            elif cnt > 0: 
                fire_alert("WARNING", "VIOLATION", rule["DATABASE_NAME"], rule["SCHEMA_NAME"], rule["TABLE_NAME"], safe_rule_name, f"{cnt} violations for {safe_rule_name}", cnt)
            else: 
                session.sql(f"UPDATE SYSTEM_DQ_DB.CONFIG.DQ_ALERTS SET IS_RESOLVED = TRUE, LAST_SEEN_AT = CURRENT_TIMESTAMP() WHERE DATABASE_NAME='{rule['DATABASE_NAME']}' AND SCHEMA_NAME='{rule['SCHEMA_NAME']}' AND TABLE_NAME='{rule['TABLE_NAME']}' AND RULE_NAME='{safe_rule_name}' AND ALERT_TYPE IN ('VIOLATION', 'RULE_ERROR')").collect()
    except Exception: pass
    
    return "Stateful DQ Monitor Run Complete."
$$;