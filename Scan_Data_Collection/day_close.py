"""
Day close: read scan_cpjr and scan_mcm from MySQL, aggregate in Python, write scan_merge and scan_deptmerge to MySQL.
Logs and status go to scan_connector_logs and scan_connector_status. No MongoDB.
"""
import json
import os
import atexit
import time
import traceback
from datetime import datetime, timedelta

import schedule
import pytz

from db_config import get_mysql_connection

# --- Debug / logging toggles ---
# Set via env vars:
#   DEBUG_PRINT=yes|no   - Enable debug prints to console and log file (default: no)
#   LOG_MESSAGE_STORE=yes|no - Store log_message/log_status to MySQL (default: yes)
def _is_enabled(env_key, default="no"):
    v = os.environ.get(env_key, default).lower()
    return v in ("yes", "1", "true", "on")

DEBUG_PRINT = _is_enabled("DEBUG_PRINT", "yes")
LOG_MESSAGE_STORE = _is_enabled("LOG_MESSAGE_STORE", "no")

# Log file for debug prints (when DEBUG_PRINT is on). Set DAY_CLOSE_DEBUG_LOG for custom path.
_script_dir = os.path.dirname(os.path.abspath(__file__))
_debug_log_path = os.environ.get("DAY_CLOSE_DEBUG_LOG", os.path.join(_script_dir, "day_close_debug.log"))
_debug_log_file = None


def _open_debug_log():
    """Open debug log file (append mode)."""
    global _debug_log_file
    if _debug_log_file is None and DEBUG_PRINT:
        try:
            _debug_log_file = open(_debug_log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[day_close] Warning: could not open debug log {_debug_log_path}: {e}")


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
    """Print only when DEBUG_PRINT is enabled. Also appends to log file."""
    if DEBUG_PRINT:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [day_close] " + " ".join(str(a) for a in args)
        print(line, **kwargs)
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

debug_print(f"Config: storeid={storeid}, corpid={corpid}, DEBUG_PRINT={DEBUG_PRINT}, LOG_MESSAGE_STORE={LOG_MESSAGE_STORE}")
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
    if not LOG_MESSAGE_STORE:
        debug_print("log_status: LOG_MESSAGE_STORE=no, skipping DB write")
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
    if not LOG_MESSAGE_STORE:
        debug_print("log_message: LOG_MESSAGE_STORE=no, skipping DB write")
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


def run_daily_task():
    function_name = "run_daily_task"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Task started.")
        # end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(end_date)
        # exit()
        debug_print(f"{function_name}: end_date (yesterday) = {end_date}")

        conn = get_mysql_connection()
        if not conn:
            debug_print(f"{function_name}: MySQL connection failed")
            log_message(function_name, "MySQL connection failed.", status="ERROR")
            return

        try:
            debug_print(f"{function_name}: querying scan_cpjr for storeid={storeid}, EndDate={end_date}")
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id, storeid, corpid, StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, ReportSequenceNumbers, BeginTimes, EndTimes, SaleEvents, OtherEvents, VoidEvents, RefundEvents FROM scan_cpjr WHERE storeid = %s AND EndDate = %s",
                (storeid, end_date),
            )
            cpjr_rows = cursor.fetchall()
            cursor.close()
            debug_print(f"{function_name}: found {len(cpjr_rows)} CPJR row(s)")

            if cpjr_rows:
                # Aggregate: merge all arrays (addToSet-style: collect unique)
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

                # Dedupe lists (keep order, unique)
                def dedupe(lst):
                    seen = set()
                    out = []
                    for x in lst:
                        k = json.dumps(x, sort_keys=True) if isinstance(x, (dict, list)) else x
                        if k not in seen:
                            seen.add(k)
                            out.append(x)
                    return out

                report_seq_set = dedupe(report_seq_set)
                begin_times_set = dedupe(begin_times_set)
                end_times_set = dedupe(end_times_set)
                debug_print(f"{function_name}: aggregated sale_events={len(sale_events)}, void_events={len(void_events)}, refund_events={len(refund_events)}, report_seq={len(report_seq_set)}")

                # Check if scan_merge row already exists
                cur = conn.cursor()
                cur.execute("SELECT id FROM scan_merge WHERE storeid = %s AND EndDate = %s", (storeid, end_date))
                existing = cur.fetchone()
                cur.close()
                debug_print(f"{function_name}: scan_merge check - existing={existing is not None}")

                if not existing:
                    ins = conn.cursor()
                    ins.execute(
                        """INSERT INTO scan_merge (storeid, corpid, StoreLocationID, BeginDate, EndDate, ReportSequenceNumbers, BeginTimes, EndTimes, SaleEvents, OtherEvents, VoidEvents, RefundEvents)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            sid,
                            cid,
                            store_location_id,
                            begin_date,
                            end_date_val,
                            json.dumps(report_seq_set),
                            json.dumps(begin_times_set),
                            json.dumps(end_times_set),
                            json.dumps(sale_events),
                            json.dumps(other_events),
                            json.dumps(void_events),
                            json.dumps(refund_events),
                        ),
                    )
                    conn.commit()
                    ins.close()
                    debug_print(f"{function_name}: inserted scan_merge row")
                    log_message(function_name, "CPJR documents merged and inserted successfully.")
                else:
                    debug_print(f"{function_name}: scan_merge row already exists, skipping insert")
                    log_message(function_name, "CPJR documents already exist. No insertion performed.")
            else:
                debug_print(f"{function_name}: no CPJR rows for store/date")
                log_message(function_name, "No CPJR rows found for this store/date.")

            # MCM: fetch all mcm rows for storeid and end_date, merge MCMDetail
            debug_print(f"{function_name}: querying scan_mcm for storeid={storeid}, EndDate={end_date}")
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT storeid, corpid, StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, MCMDetail FROM scan_mcm WHERE storeid = %s AND EndDate = %s",
                (storeid, end_date),
            )
            mcm_rows = cursor.fetchall()
            cursor.close()
            debug_print(f"{function_name}: found {len(mcm_rows)} MCM row(s)")

            if mcm_rows:
                merged_mcm = {
                    "StoreLocationID": None,
                    "corpid": None,
                    "storeid": None,
                    "BeginDate": None,
                    "BeginTime": None,
                    "EndDate": end_date,
                    "EndTime": None,
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
                debug_print(f"{function_name}: merged MCMDetail count={len(mcm_list)}")

                cur = conn.cursor()
                cur.execute("SELECT id FROM scan_deptmerge WHERE storeid = %s AND EndDate = %s", (storeid, end_date))
                existing_dm = cur.fetchone()
                cur.close()
                debug_print(f"{function_name}: scan_deptmerge check - existing={existing_dm is not None}")

                if not existing_dm:
                    ins = conn.cursor()
                    ins.execute(
                        """INSERT INTO scan_deptmerge (storeid, corpid, StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, MCMDetail)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            merged_mcm["storeid"],
                            merged_mcm["corpid"],
                            merged_mcm["StoreLocationID"],
                            merged_mcm["BeginDate"],
                            merged_mcm["BeginTime"],
                            merged_mcm["EndDate"],
                            merged_mcm["EndTime"],
                            mcm_detail_json,
                        ),
                    )
                    conn.commit()
                    ins.close()
                    debug_print(f"{function_name}: inserted scan_deptmerge row")
                    log_message(function_name, "MCM documents merged and inserted successfully.")
                else:
                    debug_print(f"{function_name}: scan_deptmerge row already exists, skipping insert")
                    log_message(function_name, "MCM documents already exist. No insertion performed.")
            else:
                debug_print(f"{function_name}: no MCM rows for store/date")
                log_message(function_name, "No MCM rows found for this store/date.")

            debug_print(f"{function_name}: completed successfully")
            log_message(function_name, "Task completed successfully", status="SUCCESS")
        finally:
            conn.close()
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error occurred: {str(e)}", status="ERROR")
        traceback.print_exc()


def on_exit():
    debug_print("on_exit: atexit handler called")
    log_status("Inactive")
    log_message("main", "Script terminated", status="TERMINATED BY USER")
    debug_print("on_exit: done")
    _close_debug_log()


atexit.register(on_exit)
schedule.every().day.at("00:30").do(run_daily_task)

# Run task immediately at startup (then repeat daily at 00:30)
debug_print("Main: running initial pass...")
run_daily_task()
debug_print("Main: initial pass complete. Schedule: daily at 00:30. Entering loop (check every 90s).")

log_status("Active")
loop_count = 0
try:
    while True:
        schedule.run_pending()
        time.sleep(90)
        loop_count += 1
        debug_print(f"Main: loop #{loop_count}, next scheduled run at 00:30")
        if loop_count % 20 == 0:  # ~every 30 min
            log_message("main", "Script is running", status="INFO")
except KeyboardInterrupt:
    debug_print("Main: KeyboardInterrupt received")
    log_status("Inactive")
    log_message("main", "Script terminated by user", status="TERMINATED")
except Exception as e:
    debug_print(f"Main: unhandled exception: {e}")
    log_status("Inactive")
    log_message("main", f"Script terminated due to error: {str(e)}", status="ERROR")
    traceback.print_exc()
finally:
    debug_print("Main: finally block, setting Inactive")
    log_status("Inactive")
    log_message("main", "Script has been terminated.", "INFO")
