"""
Day close v1: single run for daily schedule. No loop.
Read scan_cpjr and scan_mcm from MySQL, aggregate in Python, write scan_merge and scan_deptmerge to MySQL.
Uses scan_connector_last_run.last_processed_end_date to process all dates since last run (catch-up). Run once and exit.
"""
import json
import os
import traceback
from datetime import datetime, timedelta

import pytz

from db_config import get_mysql_connection

# --- Configurable toggles (yes = enable, no = disable) ---
#   SCAN_CONNECTOR_LOGS=yes|no   - Write to scan_connector_logs (default: yes)
#   SCAN_CONNECTOR_STATUS=yes|no - Write to scan_connector_status (default: yes)
#   DEBUG_PRINT=yes|no          - Print debug to console (default: no)
#   LOG_SAVING=yes|no           - Save debug output to log file when DEBUG_PRINT is on (default: yes)
def _is_enabled(env_key, default="no"):
    v = os.environ.get(env_key, default).lower()
    return v in ("yes", "1", "true", "on")

SCAN_CONNECTOR_LOGS = _is_enabled("SCAN_CONNECTOR_LOGS", "no")
SCAN_CONNECTOR_STATUS = _is_enabled("SCAN_CONNECTOR_STATUS", "yes")
DEBUG_PRINT = _is_enabled("DEBUG_PRINT", "no")
LOG_SAVING = _is_enabled("LOG_SAVING", "no")

# Log file (when DEBUG_PRINT and LOG_SAVING are on). Set DAY_CLOSE_V1_DEBUG_LOG for custom path.
_script_dir = os.path.dirname(os.path.abspath(__file__))
_debug_log_path = os.environ.get("DAY_CLOSE_V1_DEBUG_LOG", os.path.join(_script_dir, "day_close_v1_debug.log"))
_debug_log_file = None


def _open_debug_log():
    """Open debug log file (append mode) when DEBUG_PRINT and LOG_SAVING are enabled."""
    global _debug_log_file
    if _debug_log_file is None and DEBUG_PRINT and LOG_SAVING:
        try:
            _debug_log_file = open(_debug_log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[day_close_v1] Warning: could not open debug log {_debug_log_path}: {e}")


def _close_debug_log():
    """Close debug log file."""
    global _debug_log_file
    if _debug_log_file is not None:
        try:
            _debug_log_file.close()
        except OSError:
            pass
        _debug_log_file = None


def debug_print(*args, **kwargs):
    """Print when DEBUG_PRINT is on; append to log file when DEBUG_PRINT and LOG_SAVING are on."""
    if DEBUG_PRINT:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [day_close_v1] " + " ".join(str(a) for a in args)
        print(line, **kwargs)
        if LOG_SAVING:
            try:
                _open_debug_log()
                if _debug_log_file is not None:
                    _debug_log_file.write(line + "\n")
                    _debug_log_file.flush()
            except OSError:
                pass


corpid = os.environ.get("CORPID", "65361d1bc436047c00231e45")
storeid = os.environ.get("STOREID", "65d83ff360d8fb8e5b10b00d")
local_tz = pytz.timezone("America/New_York")
CONNECTOR_NAME = "Day Close Connector"

# Since last run: fallback when no previous run (process last N days). Env DAY_CLOSE_CATCHUP_DAYS, default 7.
DAY_CLOSE_CATCHUP_DAYS = int(os.environ.get("DAY_CLOSE_CATCHUP_DAYS", "7"))


def get_last_processed_end_date():
    """Return last processed EndDate (YYYY-MM-DD) from scan_connector_last_run for Day Close, or None."""
    conn = get_mysql_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT last_processed_end_date FROM scan_connector_last_run WHERE storeid = %s AND Connector = %s""",
            (storeid, CONNECTOR_NAME),
        )
        row = cur.fetchone()
        cur.close()
        if row and row[0]:
            return str(row[0])[:10]  # YYYY-MM-DD
        return None
    except Exception as e:
        debug_print(f"get_last_processed_end_date: {e}")
        return None
    finally:
        conn.close()


def set_last_processed_end_date(date_str):
    """Write last processed EndDate to scan_connector_last_run for Day Close."""
    if not SCAN_CONNECTOR_STATUS:
        return
    conn = get_mysql_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO scan_connector_last_run (storeid, Connector, last_processed_end_date)
               VALUES (%s, %s, %s)
               ON DUPLICATE KEY UPDATE last_processed_end_date = VALUES(last_processed_end_date)""",
            (storeid, CONNECTOR_NAME, date_str),
        )
        conn.commit()
        cur.close()
        debug_print(f"set_last_processed_end_date: wrote {date_str}")
    except Exception as e:
        debug_print(f"set_last_processed_end_date: {e}")
    finally:
        conn.close()


debug_print(f"Config: storeid={storeid}, corpid={corpid}, SCAN_CONNECTOR_LOGS={SCAN_CONNECTOR_LOGS}, SCAN_CONNECTOR_STATUS={SCAN_CONNECTOR_STATUS}, DEBUG_PRINT={DEBUG_PRINT}, LOG_SAVING={LOG_SAVING}, DAY_CLOSE_CATCHUP_DAYS={DAY_CLOSE_CATCHUP_DAYS}")
if DEBUG_PRINT:
    debug_print(f"Debug log file: {_debug_log_path}")


def parse_json(val):
    if val is None:
        return []
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return []
    return val if isinstance(val, list) else []


def log_status(status):
    debug_print(f"log_status called: status={status}")
    if not SCAN_CONNECTOR_STATUS:
        debug_print("log_status: SCAN_CONNECTOR_STATUS=no, skipping DB write")
        return
    local_time = datetime.now(local_tz)
    formatted_time = local_time.strftime("%Y-%m-%d-%H:%M:%S")
    conn = get_mysql_connection()
    if not conn:
        debug_print("log_status: MySQL connection failed")
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO scan_connector_status (storeid, Connector, corpid, status, timestamp)
               VALUES (%s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE corpid = VALUES(corpid), status = VALUES(status), timestamp = VALUES(timestamp)""",
            (storeid, CONNECTOR_NAME, corpid, status, formatted_time),
        )
        conn.commit()
        cur.close()
        debug_print(f"log_status: wrote status={status} to scan_connector_status")
    finally:
        conn.close()


def log_message(function_name, message, status="INFO"):
    msg_preview = (str(message)[:80] + "...") if isinstance(message, str) and len(message) > 80 else str(message)
    debug_print(f"log_message: fn={function_name}, status={status}, msg={msg_preview}")
    if not SCAN_CONNECTOR_LOGS:
        debug_print("log_message: SCAN_CONNECTOR_LOGS=no, skipping DB write")
        return
    log_id = f"{storeid}_{corpid}_log"
    conn = get_mysql_connection()
    if not conn:
        debug_print("log_message: MySQL connection failed")
        return
    try:
        cur = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id, storeid, corpid, function_name, message, status, now),
        )
        conn.commit()
        cur.close()
        debug_print(f"log_message: wrote to scan_connector_logs (fn={function_name})")
    finally:
        conn.close()


def _dedupe(lst):
    """Dedupe list keeping order; items compared by JSON string."""
    seen = set()
    out = []
    for x in lst:
        k = json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out


def process_one_date(conn, end_date):
    """Process a single end_date: merge CPJR and MCM for that date into scan_merge and scan_deptmerge. Uses conn."""
    function_name = "process_one_date"
    debug_print(f"{function_name}: processing EndDate={end_date}")

    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT id, storeid, corpid, StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, ReportSequenceNumbers, BeginTimes, EndTimes, SaleEvents, OtherEvents, VoidEvents, RefundEvents FROM scan_cpjr WHERE storeid = %s AND EndDate = %s",
        (storeid, end_date),
    )
    cpjr_rows = cursor.fetchall()
    cursor.close()
    debug_print(f"{function_name}: found {len(cpjr_rows)} CPJR row(s) for {end_date}")

    if cpjr_rows:
        report_seq_set = []
        begin_times_set = []
        end_times_set = []
        sale_events = []
        other_events = []
        void_events = []
        refund_events = []
        first = cpjr_rows[0]
        store_location_id = first.get("StoreLocationID")
        cid = first.get("corpid")
        sid = first.get("storeid")
        begin_date = first.get("BeginDate")
        end_date_val = first.get("EndDate")

        for row in cpjr_rows:
            report_seq_set.extend(parse_json(row.get("ReportSequenceNumbers")))
            begin_times_set.extend(parse_json(row.get("BeginTimes")))
            end_times_set.extend(parse_json(row.get("EndTimes")))
            sale_events.extend(parse_json(row.get("SaleEvents")))
            other_events.extend(parse_json(row.get("OtherEvents")))
            void_events.extend(parse_json(row.get("VoidEvents")))
            refund_events.extend(parse_json(row.get("RefundEvents")))

        report_seq_set = _dedupe(report_seq_set)
        begin_times_set = _dedupe(begin_times_set)
        end_times_set = _dedupe(end_times_set)

        cur = conn.cursor()
        cur.execute("SELECT id FROM scan_merge WHERE storeid = %s AND EndDate = %s", (storeid, end_date))
        existing = cur.fetchone()
        cur.close()

        if not existing:
            ins = conn.cursor()
            ins.execute(
                """INSERT INTO scan_merge (storeid, corpid, StoreLocationID, BeginDate, EndDate, ReportSequenceNumbers, BeginTimes, EndTimes, SaleEvents, OtherEvents, VoidEvents, RefundEvents)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    sid, cid, store_location_id, begin_date, end_date_val,
                    json.dumps(report_seq_set), json.dumps(begin_times_set), json.dumps(end_times_set),
                    json.dumps(sale_events), json.dumps(other_events), json.dumps(void_events), json.dumps(refund_events),
                ),
            )
            conn.commit()
            ins.close()
            debug_print(f"{function_name}: inserted scan_merge for {end_date}")
        else:
            debug_print(f"{function_name}: scan_merge already exists for {end_date}, skipping")

    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT storeid, corpid, StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, MCMDetail FROM scan_mcm WHERE storeid = %s AND EndDate = %s",
        (storeid, end_date),
    )
    mcm_rows = cursor.fetchall()
    cursor.close()
    debug_print(f"{function_name}: found {len(mcm_rows)} MCM row(s) for {end_date}")

    if mcm_rows:
        merged_mcm = {
            "StoreLocationID": None, "corpid": None, "storeid": None,
            "BeginDate": None, "BeginTime": None, "EndDate": end_date, "EndTime": None,
            "MCMDetail": {},
        }
        for row in mcm_rows:
            if not merged_mcm["StoreLocationID"]:
                merged_mcm["StoreLocationID"] = row.get("StoreLocationID")
                merged_mcm["corpid"] = row.get("corpid")
                merged_mcm["storeid"] = row.get("storeid")
                merged_mcm["BeginDate"] = row.get("BeginDate")
                merged_mcm["BeginTime"] = row.get("BeginTime")
                merged_mcm["EndTime"] = row.get("EndTime")
            else:
                et = row.get("EndTime")
                if et and merged_mcm["EndTime"]:
                    merged_mcm["EndTime"] = max(merged_mcm["EndTime"], et)

            mcm_detail = parse_json(row.get("MCMDetail"))
            if not isinstance(mcm_detail, list):
                mcm_detail = []
            for item in mcm_detail:
                if not isinstance(item, dict):
                    continue
                code = item.get("MerchandiseCode")
                if code not in merged_mcm["MCMDetail"]:
                    merged_mcm["MCMDetail"][code] = dict(item)
                else:
                    existing = merged_mcm["MCMDetail"][code].get("MCMSalesTotals", {})
                    if not isinstance(existing, dict):
                        existing = {}
                    new_totals = item.get("MCMSalesTotals", {})
                    if isinstance(new_totals, dict):
                        for k, v in new_totals.items():
                            try:
                                existing[k] = str(float(existing.get(k, 0)) + float(v))
                            except (TypeError, ValueError):
                                existing[k] = v
                    merged_mcm["MCMDetail"][code]["MCMSalesTotals"] = existing

        mcm_list = list(merged_mcm["MCMDetail"].values())
        mcm_detail_json = json.dumps(mcm_list)

        cur = conn.cursor()
        cur.execute("SELECT id FROM scan_deptmerge WHERE storeid = %s AND EndDate = %s", (storeid, end_date))
        existing_dm = cur.fetchone()
        cur.close()

        if not existing_dm:
            ins = conn.cursor()
            ins.execute(
                """INSERT INTO scan_deptmerge (storeid, corpid, StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, MCMDetail)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    merged_mcm["storeid"], merged_mcm["corpid"], merged_mcm["StoreLocationID"],
                    merged_mcm["BeginDate"], merged_mcm["BeginTime"], merged_mcm["EndDate"], merged_mcm["EndTime"],
                    mcm_detail_json,
                ),
            )
            conn.commit()
            ins.close()
            debug_print(f"{function_name}: inserted scan_deptmerge for {end_date}")
        else:
            debug_print(f"{function_name}: scan_deptmerge already exists for {end_date}, skipping")


def run_daily_task():
    """Process all dates from (last processed + 1) through yesterday. Update last_processed_end_date after each date."""
    function_name = "run_daily_task"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Task started.")

        yesterday = (datetime.now() - timedelta(days=1)).date()
        end_date_max = yesterday.strftime("%Y-%m-%d")

        last_processed = get_last_processed_end_date()
        if last_processed:
            try:
                start_dt = datetime.strptime(last_processed, "%Y-%m-%d").date() + timedelta(days=1)
            except ValueError:
                start_dt = yesterday - timedelta(days=DAY_CLOSE_CATCHUP_DAYS)
        else:
            start_dt = yesterday - timedelta(days=DAY_CLOSE_CATCHUP_DAYS)

        if start_dt > yesterday:
            debug_print(f"{function_name}: no dates to process (start={start_dt}, yesterday={end_date_max})")
            log_message(function_name, "No dates to process.", status="SUCCESS")
            return

        dates_to_process = []
        d = start_dt
        while d <= yesterday:
            dates_to_process.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)

        debug_print(f"{function_name}: processing {len(dates_to_process)} date(s) from {dates_to_process[0]} to {dates_to_process[-1]}")

        conn = get_mysql_connection()
        if not conn:
            debug_print(f"{function_name}: MySQL connection failed")
            log_message(function_name, "MySQL connection failed.", status="ERROR")
            return

        try:
            for end_date in dates_to_process:
                process_one_date(conn, end_date)
                set_last_processed_end_date(end_date)
            debug_print(f"{function_name}: completed successfully")
            log_message(function_name, "Task completed successfully", status="SUCCESS")
        finally:
            conn.close()
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error occurred: {str(e)}", status="ERROR")
        traceback.print_exc()


# Single run (for daily scheduling): run day-close task once, then exit.
debug_print("Main: single run (day close)...")
try:
    log_status("Active")
    run_daily_task()
    log_status("Inactive")
    log_message("main", "Single run completed.", "SUCCESS")
except Exception as e:
    debug_print(f"Main: unhandled exception: {e}")
    traceback.print_exc()
    log_status("Inactive")
    log_message("main", f"Single run failed: {str(e)}", "ERROR")
finally:
    _close_debug_log()
