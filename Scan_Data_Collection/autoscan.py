"""
Autoscan (Altria): read scan_weekly, scan_data_dept, scan_brand, scan_upc, scan_stores, scan_gb_price_promotion from MySQL;
build scan data and insert into scan_data. No MongoDB.
"""
import json
import os
from datetime import datetime

import numpy as np

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

# Log file for debug prints (when DEBUG_PRINT is on). Set AUTOSCAN_DEBUG_LOG for custom path.
_script_dir = os.path.dirname(os.path.abspath(__file__))
_debug_log_path = os.environ.get("AUTOSCAN_DEBUG_LOG", os.path.join(_script_dir, "autoscan_debug.log"))
_debug_log_file = None

corpid = os.environ.get("CORPID", "65361d1bc436047c00231e45")
storeid = os.environ.get("STORE_ID", "65d83ff360d8fb8e5b10b00d")


def _open_debug_log():
    global _debug_log_file
    if _debug_log_file is None and DEBUG_PRINT:
        try:
            _debug_log_file = open(_debug_log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[autoscan] Warning: could not open debug log {_debug_log_path}: {e}")


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
        line = f"[{ts}] [autoscan] " + " ".join(str(a) for a in args)
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


debug_print(f"Config: storeid={storeid}, DEBUG_PRINT={DEBUG_PRINT}, LOG_MESSAGE_STORE={LOG_MESSAGE_STORE}")
if DEBUG_PRINT:
    debug_print(f"Debug log file: {_debug_log_path}")


def _parse_json(val):
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return None
    return val


def _json_safe_str(s):
    """Sanitize string for MySQL JSON: remove control chars that cause 'Invalid value'."""
    if s is None:
        return None
    s = str(s)
    return "".join(c for c in s if c in ("\t", "\n", "\r") or ord(c) >= 32)


conn = get_mysql_connection()
if not conn:
    msg = "Failed to connect to MySQL. Exiting."
    debug_print(msg)
    if not DEBUG_PRINT:
        print(f"[autoscan] {msg}")
    exit(1)

try:
    log_message("autoscan", "Autoscan (Altria) started.")
    debug_print(f"Querying scan_weekly for latest doc (storeid={storeid})")
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT storeid, corpid, BeginDate, EndDate, ReportSequenceNumbers, SaleEvent FROM scan_weekly WHERE storeid = %s ORDER BY id DESC LIMIT 1",
        (storeid,),
    )
    latest_doc = cur.fetchone()
    cur.close()
    if not latest_doc:
        msg = "No scan_weekly document found for storeid. Exiting."
        debug_print(msg)
        if not DEBUG_PRINT:
            print(f"[autoscan] {msg}")
        log_message("autoscan", msg, status="WARNING")
        conn.close()
        _close_debug_log()
        exit(0)

    debug_print(f"Latest scan_weekly: BeginDate={latest_doc.get('BeginDate')}, EndDate={latest_doc.get('EndDate')}")

    current_month = datetime.now().strftime("%B%Y")
    debug_print(f"Querying scan_gb_price_promotion for month={current_month}")
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT Stores FROM scan_gb_price_promotion WHERE storeid = %s AND month = %s",
        (storeid, current_month),
    )
    store_data = cur.fetchone()
    cur.close()
    if store_data and store_data.get("Stores"):
        stores_arr = _parse_json(store_data["Stores"]) if isinstance(store_data["Stores"], str) else store_data["Stores"]
        store = stores_arr[0] if stores_arr else {}
        rcn = store.get("RCN")
        address = store.get("Address")
        city = store.get("City")
        state = store.get("State")
    else:
        rcn = address = city = state = None
    debug_print(f"Store data: rcn={rcn}, address={address}, city={city}, state={state}")

    from db_normalized import get_specific_merchandise_codes_safe, get_upc_codes_safe
    merchandise_codes = list(get_specific_merchandise_codes_safe(conn, storeid))
    debug_print(f"Merchandise codes count: {len(merchandise_codes)}")

    sale_event = _parse_json(latest_doc["SaleEvent"]) if latest_doc.get("SaleEvent") else []
    if not sale_event:
        msg = "No SaleEvent in scan_weekly. Exiting."
        debug_print(msg)
        if not DEBUG_PRINT:
            print(f"[autoscan] {msg}")
        log_message("autoscan", msg, status="WARNING")
        conn.close()
        _close_debug_log()
        exit(0)
    week_ending_date = sale_event[-1].get("ReceiptDate") if isinstance(sale_event[-1], dict) else None
    if not week_ending_date:
        week_ending_date = latest_doc.get("EndDate")
    debug_print(f"SaleEvent count: {len(sale_event)}, week_ending_date={week_ending_date}")

    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, store_name, zip_code, payload FROM scan_stores WHERE id = %s", (storeid,))
    store_doc = cur.fetchone()
    cur.close()
    store_name = store_doc["store_name"] if store_doc else None
    zip_code = store_doc["zip_code"] if store_doc else None
    debug_print(f"Store: store_name={store_name}, zip_code={zip_code}")

    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT payload FROM scan_brand WHERE storeid = %s LIMIT 1", (storeid,))
    manf_row = cur.fetchone()
    cur.close()
    manf = _parse_json(manf_row["payload"]) if manf_row and manf_row.get("payload") else None
    debug_print(f"Manufacturer payload loaded: {bool(manf)}")

    data = []
    keys_to_process = ["PM USA", "JMC", "USSTC", "Helix Innovations", "NJOY"]
    debug_print(f"Processing {len(sale_event)} sale events, keys_to_process={keys_to_process}")

    for event in sale_event:
        if not isinstance(event, dict):
            continue
        transaction_id = event.get("TransactionID")
        receipt_date = event.get("ReceiptDate")
        receipt_time = event.get("ReceiptTime")
        transaction_line = event.get("TransactionLine") or event.get("TransactionLines") or []
        aggregated_items = {}
        for transaction in transaction_line:
            item = (transaction.get("ItemLine") or {}).copy()
            if not item:
                continue
            poscode = item.get("POSCode")
            if poscode is None:
                continue
            if poscode not in aggregated_items:
                aggregated_items[poscode] = {
                    "Description": item.get("Description"),
                    "MerchandiseCode": item.get("MerchandiseCode"),
                    "SalesQuantity": 0,
                    "SalesAmount": 0.0,
                    "LoyaltyID": item.get("LoyaltyID"),
                }
            aggregated_items[poscode]["SalesQuantity"] += int(float(item.get("SalesQuantity", 0)))
            aggregated_items[poscode]["SalesAmount"] += float(item.get("SalesAmount", 0.0))

        for poscode, item in aggregated_items.items():
            merchandise_code = item["MerchandiseCode"]
            if merchandise_code not in merchandise_codes:
                continue
            description = item["Description"]
            salesquantity = item["SalesQuantity"]
            salesamount = "{:.2f}".format(item["SalesAmount"])

            manufacturer_name = None
            if manf:
                for key in keys_to_process:
                    for items in manf.get(key) or []:
                        poscode_slice = poscode[2:-1] if len(str(poscode)) == 14 else poscode
                        item_to_compare = items[:-1] if len(str(items)) > 1 else items
                        if str(poscode_slice) == str(item_to_compare):
                            manufacturer_name = key
                            break

            multi_pack_indicator = "N"
            multi_pack_required_quantity = 0
            multi_pack_discount_amount = 0
            upc_codes = get_upc_codes_safe(conn, "PM USA")
            if upc_codes:
                for upc in upc_codes:
                    poscode_slice = poscode[2:-1] if len(str(poscode)) == 14 else poscode
                    upc_to_compare = upc[:-1] if len(str(upc)) > 1 else upc
                    if str(poscode_slice) == str(upc_to_compare) and salesquantity >= 2:
                        multi_pack_indicator = "Y"
                        multi_pack_required_quantity = int(float(salesquantity))
                        multi_pack_discount_amount = "{:.2f}".format(multi_pack_required_quantity * 0.10)
                        break
            if multi_pack_indicator != "Y":
                jmc_upc_codes = get_upc_codes_safe(conn, "JMC")
                if jmc_upc_codes:
                    for upc in jmc_upc_codes:
                        poscode_slice = poscode[2:] if len(str(poscode)) == 14 else poscode
                        if str(poscode_slice) == str(upc) and salesquantity >= 3 and salesquantity % 3 == 0:
                            multi_pack_indicator = "Y"
                            multi_pack_required_quantity = int(float(salesquantity))
                            multi_pack_discount_amount = "{:.2f}".format(multi_pack_required_quantity * 0.25)
                            break

            row = {
                "Retail Control Number": rcn,
                "WeekEndingDate": week_ending_date,
                "TransactionDate": _json_safe_str(str(receipt_date)) if receipt_date is not None else None,
                "TransactionTime": _json_safe_str(str(receipt_time)) if receipt_time is not None else None,
                "TransactionID": _json_safe_str(str(transaction_id)) if transaction_id is not None else None,
                "Store Number": 1,
                "Store Name": _json_safe_str(store_name),
                "Store Address": _json_safe_str(address),
                "Store City": _json_safe_str(city),
                "Store State": _json_safe_str(state),
                "Store Zip + 4 Code": _json_safe_str(zip_code),
                "Category": "CIG",
                "Manufacturer Name": _json_safe_str(manufacturer_name),
                "SKU Code": _json_safe_str(str(poscode)) if poscode is not None else None,
                "UPC Code": _json_safe_str(str(poscode)) if poscode is not None else None,
                "UPC Description": _json_safe_str(description),
                "Unit of Measure": "PACK",
                "Quantity Sold": salesquantity,
                "Consumer Units": 1,
                "Multi-Pack Indicator": multi_pack_indicator,
                "Multi-Pack Required Quantity": multi_pack_required_quantity,
                "Multi-Pack Discount Amount": multi_pack_discount_amount,
                "Retailer-Funded Discount Name": None,
                "Retailer-Funded Discount Amount": None,
                "MFG Deal Name ONE": None,
                "MFG Deal Discount Amount ONE": None,
                "MFG Deal Name TWO": None,
                "MFG Deal Discount Amount TWO": None,
                "MFG Deal Name THREE": None,
                "MFG Deal Discount Amount THREE": None,
                "Final Sales Price": salesamount,
                "R1": None, "R2": None, "R3": None, "R4": None, "R5": None,
                "R6": None, "R7": None, "R8": None, "R9": None, "R10": None,
                "R11": None, "R12": None, "R13": None, "R14": None,
            }
            data.append(row)

    debug_print(f"Built {len(data)} data rows for scan_data")

    def _json_default(obj):
        if isinstance(obj, (np.floating, np.integer)):
            if isinstance(obj, np.floating) and (np.isnan(obj) or np.isinf(obj)):
                return None
            return float(obj) if isinstance(obj, np.floating) else int(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    data_json = json.dumps(data, default=_json_default)
    ins = conn.cursor()
    ins.execute(
        """INSERT INTO scan_data (storeid, week_ending_date, brand, corpid, submitternum, submitdate, storenum, transactions, status, substatus, txtURL, data)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            storeid,
            week_ending_date,
            "Altria",
            latest_doc.get("corpid"),
            "9305",
            datetime.now().date().isoformat(),
            "1",
            str(len(data)),
            0,
            "Submitted",
            "",
            data_json,
        ),
    )
    conn.commit()
    ins.close()
    msg = "Scan data (Altria) inserted into MySQL successfully."
    debug_print(msg)
    if not DEBUG_PRINT:
        print(f"[autoscan] {msg}")
    log_message("autoscan", msg, status="SUCCESS")
except Exception as e:
    debug_print(f"ERROR: {e}")
    if not DEBUG_PRINT:
        print(f"[autoscan] ERROR: {e}")
    log_message("autoscan", f"Error: {str(e)}", status="ERROR")
    raise
finally:
    conn.close()
    _close_debug_log()
