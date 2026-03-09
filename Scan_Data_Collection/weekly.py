"""
Weekly aggregation: read scan_merge from MySQL, write scan_weekly to MySQL.
No MongoDB. Store ID can be set via env STORE_ID or change default below.
"""
import json
import os
from datetime import datetime, timedelta

from db_config import get_mysql_connection

# --- Debug / logging toggles ---
# Set via env vars:
#   DEBUG_PRINT=yes|no   - Enable debug prints to console and log file (default: no)
#   LOG_MESSAGE_STORE=yes|no - Store log_message to MySQL (default: yes)
def _is_enabled(env_key, default="no"):
    v = os.environ.get(env_key, default).lower()
    return v in ("yes", "1", "true", "on")

DEBUG_PRINT = _is_enabled("DEBUG_PRINT", "yes")
LOG_MESSAGE_STORE = _is_enabled("LOG_MESSAGE_STORE", "no")

# Log file for debug prints (when DEBUG_PRINT is on). Set WEEKLY_DEBUG_LOG for custom path.
_script_dir = os.path.dirname(os.path.abspath(__file__))
_debug_log_path = os.environ.get("WEEKLY_DEBUG_LOG", os.path.join(_script_dir, "weekly_debug.log"))
_debug_log_file = None

CONNECTOR_NAME = "Weekly Connector"
corpid = os.environ.get("CORPID", "65361d1bc436047c00231e45")


def _open_debug_log():
    global _debug_log_file
    if _debug_log_file is None and DEBUG_PRINT:
        try:
            _debug_log_file = open(_debug_log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[weekly] Warning: could not open debug log {_debug_log_path}: {e}")


def _close_debug_log():
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
        line = f"[{ts}] [weekly] " + " ".join(str(a) for a in args)
        print(line, **kwargs)
        try:
            _open_debug_log()
            if _debug_log_file is not None:
                _debug_log_file.write(line + "\n")
                _debug_log_file.flush()
        except OSError:
            pass


def log_message(function_name, message, status="INFO"):
    debug_print(f"log_message: fn={function_name}, status={status}, msg={str(message)[:80]}")
    if not LOG_MESSAGE_STORE:
        debug_print("log_message: LOG_MESSAGE_STORE=no, skipping DB write")
        return
    log_id = f"{STORE_ID}_{corpid}_log"
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
            (log_id, STORE_ID, corpid, function_name, message, status, now),
        )
        conn.commit()
        cur.close()
        debug_print(f"log_message: wrote to scan_connector_logs (fn={function_name})")
    finally:
        conn.close()


# Store ID: set env STORE_ID or change default
STORE_ID = os.environ.get("STORE_ID", "65d83ff360d8fb8e5b10b00d")

# Last complete week (Sunday to Saturday)
today = datetime.now().date()
start_date = today - timedelta(days=today.weekday() + 8)
end_date = start_date + timedelta(days=6)
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

debug_print(f"Config: STORE_ID={STORE_ID}, DEBUG_PRINT={DEBUG_PRINT}, LOG_MESSAGE_STORE={LOG_MESSAGE_STORE}")
if DEBUG_PRINT:
    debug_print(f"Debug log file: {_debug_log_path}")
debug_print(f"today={today}, start_date={start_date_str}, end_date={end_date_str}")
# exit()

conn = get_mysql_connection()
if not conn:
    msg = "Failed to connect to MySQL. Exiting."
    debug_print(msg)
    if not DEBUG_PRINT:
        print(f"[weekly] {msg}")
    exit(1)

try:
    log_message("weekly", "Weekly aggregation started.")
    debug_print(f"Querying scan_merge for storeid={STORE_ID}, EndDate between {start_date_str} and {end_date_str}")
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT id, storeid, corpid, StoreLocationID, BeginDate, EndDate, ReportSequenceNumbers, SaleEvents, VoidEvents, RefundEvents, OtherEvents, BeginTimes, EndTimes FROM scan_merge WHERE storeid = %s AND EndDate >= %s AND EndDate <= %s ORDER BY EndDate",
        (STORE_ID, start_date_str, end_date_str),
    )
    rows = cursor.fetchall()
    cursor.close()
    debug_print(f"Found {len(rows)} scan_merge row(s)")

    if not rows:
        msg = "No scan_merge rows found for this store and date range. Exiting."
        debug_print(msg)
        if not DEBUG_PRINT:
            print(f"[weekly] {msg}")
        log_message("weekly", "No scan_merge rows found for this store and date range.", status="WARNING")
        conn.close()
        _close_debug_log()
        exit(0)

    def parse_json(val):
        if val is None:
            return []
        if isinstance(val, str):
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                return []
        return val if isinstance(val, list) else []

    all_sale_events = []
    report_sequence_numbers = []
    corp_id = None
    store_id = None

    for row in rows:
        sale_events = parse_json(row.get("SaleEvents"))
        all_sale_events.extend(sale_events)
        rsn = parse_json(row.get("ReportSequenceNumbers"))
        if isinstance(rsn, list):
            report_sequence_numbers.append(rsn)
        else:
            report_sequence_numbers.append(rsn)
        if corp_id is None:
            corp_id = row.get("corpid")
        if store_id is None:
            store_id = row.get("storeid")

    debug_print(f"Aggregated: sale_events={len(all_sale_events)}, report_seq_groups={len(report_sequence_numbers)}, corp_id={corp_id}, store_id={store_id}")

    sale_event_json = json.dumps(all_sale_events)
    report_seq_json = json.dumps(report_sequence_numbers)

    ins = conn.cursor()
    ins.execute(
        """INSERT INTO scan_weekly (storeid, corpid, BeginDate, EndDate, ReportSequenceNumbers, SaleEvent)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (store_id, corp_id, start_date_str, end_date_str, report_seq_json, sale_event_json),
    )
    conn.commit()
    ins.close()
    msg = "Weekly row inserted into scan_weekly successfully."
    debug_print(msg)
    if not DEBUG_PRINT:
        print(f"[weekly] {msg}")
    log_message("weekly", "Weekly row inserted into scan_weekly successfully.", status="SUCCESS")
except Exception as e:
    debug_print(f"ERROR: {e}")
    if not DEBUG_PRINT:
        print(f"[weekly] ERROR: {e}")
    log_message("weekly", f"Error: {str(e)}", status="ERROR")
    raise
finally:
    conn.close()
    _close_debug_log()
