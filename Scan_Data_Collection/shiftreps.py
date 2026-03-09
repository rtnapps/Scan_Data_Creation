"""
Shiftreps: read XML from X:\\BOOutBox; read scan_data_dept, scan_gbtax, scan_gb_department from MySQL;
write scan_msm, scan_mcm, scan_ism, scan_tlm, scan_tpm, scan_cpjr to MySQL. Logs to scan_connector_logs and scan_connector_status. No MongoDB.
"""
import os
import glob
import schedule
import time
import atexit
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

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

# Log file for debug prints (when DEBUG_PRINT is on). Set SHIFTREPS_DEBUG_LOG for custom path.
_script_dir = os.path.dirname(os.path.abspath(__file__))
_debug_log_path = os.environ.get("SHIFTREPS_DEBUG_LOG", os.path.join(_script_dir, "shiftreps_debug.log"))
_debug_log_file = None


def _open_debug_log():
    """Open debug log file (append mode)."""
    global _debug_log_file
    if _debug_log_file is None and DEBUG_PRINT:
        try:
            _debug_log_file = open(_debug_log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[shiftreps] Warning: could not open debug log {_debug_log_path}: {e}")


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
        line = f"[{ts}] [shiftreps] " + " ".join(str(a) for a in args)
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
CONNECTOR_NAME = "Shift-wise Connector"
directory = os.environ.get("SHIFTREPS_DIR", r"C:\Program Files (x86)\CStore Essentials\POS Link\Service\Archive")

# --- PLUG-IN: Process last N days (remove this block to restore original "newest only" behavior) ---
# Enable: set SHIFTREPS_PROCESS_LAST_DAYS=7 to process all XML files from the last 7 days.
# Disable: set SHIFTREPS_PROCESS_LAST_DAYS=0 or omit (default 0 = original behavior).
# TO REMOVE: delete PROCESS_LAST_N_DAYS, _get_files_to_process, and in each process_* replace
#   "files_to_process = _get_files_to_process(...); for f in files_to_process:" with
#   "if files: f = sorted(..., key=os.path.getmtime, reverse=True)[0]; ..." (newest only).
PROCESS_LAST_N_DAYS = int(os.environ.get("SHIFTREPS_PROCESS_LAST_DAYS", "0"))


def _get_files_to_process(files, sort_newest_first=False):
    """
    Return list of files to process.
    - If PROCESS_LAST_N_DAYS > 0: all files modified in last N days, sorted oldest-first (so inserts are chronological).
    - Else: only the newest file [files[0]] after sort by mtime desc.
    """
    if not files:
        return []
    files_sorted = sorted(files, key=os.path.getmtime, reverse=True)  # newest first
    if PROCESS_LAST_N_DAYS <= 0:
        return [files_sorted[0]] if files_sorted else []
    cutoff = (datetime.now() - timedelta(days=PROCESS_LAST_N_DAYS)).timestamp()
    recent = [f for f in files_sorted if os.path.getmtime(f) >= cutoff]
    if sort_newest_first:
        return recent  # newest first
    return list(reversed(recent))  # oldest first (chronological order for inserts)


debug_print(f"Config: directory={directory}, storeid={storeid}, corpid={corpid}, DEBUG_PRINT={DEBUG_PRINT}, LOG_MESSAGE_STORE={LOG_MESSAGE_STORE}, PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS}")
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


def _load_ref_data():
    """Load scan_data_dept, scan_gbtax, scan_gb_department from MySQL. Returns (specific_merchandise_codes, tax_percentage_map, department_tax_map)."""
    debug_print("_load_ref_data: loading reference data from MySQL")
    from db_normalized import get_specific_merchandise_codes_safe, get_tax_percentage_map_safe, get_department_tax_map_safe
    conn = get_mysql_connection()
    if not conn:
        debug_print("_load_ref_data: MySQL connection failed")
        raise RuntimeError("MySQL connection failed")
    try:
        specific_merchandise_codes = get_specific_merchandise_codes_safe(conn, storeid)
        tax_percentage_map = get_tax_percentage_map_safe(conn, storeid)
        department_tax_map = get_department_tax_map_safe(conn, storeid)
        debug_print(f"_load_ref_data: loaded merch_codes={len(specific_merchandise_codes)}, tax_map={len(tax_percentage_map)}, dept_map={len(department_tax_map)}")
        return specific_merchandise_codes, tax_percentage_map, department_tax_map
    finally:
        conn.close()


# Load reference data at startup (will be set by first run or on demand)
try:
    specific_merchandise_codes, tax_percentage_map, department_tax_map = _load_ref_data()
    debug_print("Startup: reference data loaded successfully")
except Exception as e:
    print(f"Warning: could not load reference data: {e}")
    debug_print(f"Startup: ref data load failed, using empty defaults: {e}")
    specific_merchandise_codes = set()
    tax_percentage_map = {}
    department_tax_map = {}


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


def on_exit():
    debug_print("on_exit: atexit handler called")
    log_status("Inactive")
    log_message("main", "Script terminated", status="TERMINATED BY USER")
    debug_print("on_exit: done")
    _close_debug_log()


atexit.register(on_exit)


# --- XML extractors (unchanged logic) ---
def extract_msm_data(root):
    data = {
        "StoreLocationID": root.find(".//TransmissionHeader/StoreLocationID").text,
        "BeginDate": root.find(".//MovementHeader/BeginDate").text,
        "BeginTime": root.find(".//MovementHeader/BeginTime").text,
        "EndDate": root.find(".//MovementHeader/EndDate").text,
        "EndTime": root.find(".//MovementHeader/EndTime").text,
        "MSMDetail": [
            {
                "MiscellaneousSummaryCodes": {
                    "MiscellaneousSummaryCode": detail.find(".//MiscellaneousSummaryCodes/MiscellaneousSummaryCode").text,
                    "MiscellaneousSummarySubCode": detail.find(".//MiscellaneousSummaryCodes/MiscellaneousSummarySubCode").text,
                },
                "MSMSalesTotals": {
                    "Tender": {
                        "TenderCode": detail.find(".//MSMSalesTotals/Tender/TenderCode").text,
                        "TenderSubCode": detail.find(".//MSMSalesTotals/Tender/TenderSubCode").text,
                    },
                    "MiscellaneousSummaryAmount": detail.find(".//MSMSalesTotals/MiscellaneousSummaryAmount").text,
                    "MiscellaneousSummaryCount": detail.find(".//MSMSalesTotals/MiscellaneousSummaryCount").text,
                },
            }
            for detail in root.findall(".//MSMDetail")
        ],
    }
    return data


def extract_mcm_data(root):
    data = {
        "StoreLocationID": root.find(".//TransmissionHeader/StoreLocationID").text,
        "storeid": storeid,
        "corpid": corpid,
        "BeginDate": root.find(".//MovementHeader/BeginDate").text,
        "BeginTime": root.find(".//MovementHeader/BeginTime").text,
        "EndDate": root.find(".//MovementHeader/EndDate").text,
        "EndTime": root.find(".//MovementHeader/EndTime").text,
        "MCMDetail": [
            {
                "MerchandiseCode": detail.find("MerchandiseCode").text,
                "MerchandiseCodeDescription": detail.find("MerchandiseCodeDescription").text,
                "MCMSalesTotals": {
                    "DiscountAmount": detail.find(".//MCMSalesTotals/DiscountAmount").text,
                    "DiscountCount": detail.find(".//MCMSalesTotals/DiscountCount").text,
                    "PromotionAmount": detail.find(".//MCMSalesTotals/PromotionAmount").text,
                    "PromotionCount": detail.find(".//MCMSalesTotals/PromotionCount").text,
                    "RefundAmount": detail.find(".//MCMSalesTotals/RefundAmount").text,
                    "RefundCount": detail.find(".//MCMSalesTotals/RefundCount").text,
                    "SalesQuantity": detail.find(".//MCMSalesTotals/SalesQuantity").text,
                    "SalesAmount": str(round(float(detail.find(".//MCMSalesTotals/SalesAmount").text), 2)),
                    "TransactionCount": detail.find(".//MCMSalesTotals/TransactionCount").text,
                    "OpenDepartmentSalesAmount": detail.find(".//MCMSalesTotals/OpenDepartmentSalesAmount").text,
                    "OpenDepartmentTransactionCount": detail.find(".//MCMSalesTotals/OpenDepartmentTransactionCount").text,
                },
            }
            for detail in root.findall(".//MCMDetail")
        ],
    }
    return data


def extract_ism_data(root):
    data = {
        "StoreLocationID": root.find(".//TransmissionHeader/StoreLocationID").text,
        "BeginDate": root.find(".//MovementHeader/BeginDate").text,
        "BeginTime": root.find(".//MovementHeader/BeginTime").text,
        "EndDate": root.find(".//MovementHeader/EndDate").text,
        "EndTime": root.find(".//MovementHeader/EndTime").text,
        "ISMDetail": [
            {
                "ItemID": detail.find("ItemID").text,
                "Description": detail.find("Description").text,
                "MerchandiseCode": detail.find("MerchandiseCode").text,
                "SellingUnits": detail.find("SellingUnits").text,
                "ISMSellPriceSummary": {
                    "ActualSalesPrice": detail.find(".//ISMSellPriceSummary/ActualSalesPrice").text,
                    "ISMSalesTotals": {
                        "SalesQuantity": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/SalesQuantity").text,
                        "SalesAmount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/SalesAmount").text,
                        "DiscountAmount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/DiscountAmount").text,
                        "DiscountCount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/DiscountCount").text,
                        "PromotionAmount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/PromotionAmount").text,
                        "PromotionCount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/PromotionCount").text,
                        "RefundAmount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/RefundAmount").text,
                        "RefundCount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/RefundCount").text,
                        "TransactionCount": detail.find(".//ISMSellPriceSummary/ISMSalesTotals/TransactionCount").text,
                    },
                },
            }
            for detail in root.findall(".//ISMDetail")
        ],
    }
    return data


def extract_tlm_data(root):
    data = {
        "StoreLocationID": root.find(".//TransmissionHeader/StoreLocationID").text,
        "TaxLevelMovement": [
            {
                "MovementHeader": {
                    "BeginDate": movement.find(".//MovementHeader/BeginDate").text,
                    "BeginTime": movement.find(".//MovementHeader/BeginTime").text,
                    "EndDate": movement.find(".//MovementHeader/EndDate").text,
                    "EndTime": movement.find(".//MovementHeader/EndTime").text,
                },
                "TLMDetail": [
                    {
                        "TaxLevelID": detail.find("TaxLevelID").text,
                        "MerchandiseCode": detail.find("MerchandiseCode").text,
                        "TaxableSalesAmount": detail.find("TaxableSalesAmount").text,
                        "TaxableSalesRefundedAmount": detail.find("TaxableSalesRefundedAmount").text,
                        "TaxCollectedAmount": detail.find("TaxCollectedAmount").text,
                        "TaxExemptSalesAmount": detail.find("TaxExemptSalesAmount").text,
                        "TaxExemptSalesRefundedAmount": detail.find("TaxExemptSalesRefundedAmount").text,
                        "TaxForgivenSalesAmount": detail.find("TaxForgivenSalesAmount").text,
                        "TaxForgivenSalesRefundedAmount": detail.find("TaxForgivenSalesRefundedAmount").text,
                        "TaxRefundedAmount": detail.find("TaxRefundedAmount").text,
                    }
                    for detail in movement.findall(".//TLMDetail")
                ],
            }
            for movement in root.findall(".//TaxLevelMovement")
        ],
    }
    return data


def extract_tpm_data(root):
    data = {
        "StoreLocationID": root.find(".//TransmissionHeader/StoreLocationID").text,
        "TankProductMovement": [
            {
                "MovementHeader": {
                    "BeginDate": movement.find(".//MovementHeader/BeginDate").text,
                    "BeginTime": movement.find(".//MovementHeader/BeginTime").text,
                    "EndDate": movement.find(".//MovementHeader/EndDate").text,
                    "EndTime": movement.find(".//MovementHeader/EndTime").text,
                },
                "TankProductMovement": {
                    "ReadingDate": movement.find(".//TankProductMovement/ReadingDate").text if movement.find(".//TankProductMovement/ReadingDate") is not None else None,
                    "ReadingTime": movement.find(".//TankProductMovement/ReadingTime").text if movement.find(".//TankProductMovement/ReadingTime") is not None else None,
                },
                "TPMDetail": [
                    {
                        "TankId": detail.find("TankId").text if detail.find("TankId") is not None else None,
                        "FuelProductId": detail.find("FuelProductId").text if detail.find("FuelProductId") is not None else None,
                        "FuelProductVolume": detail.find("FuelProductVolume").text if detail.find("FuelProductVolume") is not None else "0",
                        "FuelProductTemperature": detail.find("FuelProductTemperature").text if detail.find("FuelProductTemperature") is not None else "0",
                        "Ullage": detail.find("Ullage").text if detail.find("Ullage") is not None else "0",
                        "WaterVolume": detail.find("WaterVolume").text if detail.find("WaterVolume") is not None else "0",
                    }
                    for detail in movement.findall(".//TPMDetail")
                ]
                or [
                    {"TankId": None, "FuelProductId": None, "FuelProductVolume": "0", "FuelProductTemperature": "0", "Ullage": "0", "WaterVolume": "0"}
                ],
            }
            for movement in root.findall(".//TankProductMovement")
        ],
    }
    return data


def extract_cpjr_data(root):
    data = {
        "StoreLocationID": root.find(".//StoreLocationID").text,
        "corpid": corpid,
        "storeid": storeid,
        "ReportSequenceNumber": root.find(".//ReportSequenceNumber").text,
        "BeginDate": root.find(".//BeginDate").text,
        "BeginTime": root.find(".//BeginTime").text,
    }
    sale_events = root.findall(".//SaleEvent")
    if sale_events:
        last_sale_event = sale_events[-1]
        data["EndDate"] = last_sale_event.find(".//EventEndDate").text
        data["EndTime"] = last_sale_event.find(".//EventEndTime").text
    else:
        data["EndDate"] = data.get("BeginDate", "")
        data["EndTime"] = data.get("BeginTime", "")

    other_events = root.findall(".//OtherEvent")
    if other_events:
        data["OtherEvents"] = []
        for event in other_events:
            event_data = {
                "TransactionID": event.find(".//TransactionID").text,
                "CashierID": event.find(".//CashierID").text,
                "RegisterID": event.find(".//RegisterID").text,
                "TillID": event.find(".//TillID").text,
                "EventStartDate": event.find(".//EventStartDate").text,
                "EventStartTime": event.find(".//EventStartTime").text,
                "EventEndDate": event.find(".//EventEndDate").text,
                "EventEndTime": event.find(".//EventEndTime").text,
                "CashInDrawer": event.find(".//CashInDrawer").text if event.find(".//CashInDrawer") is not None else None,
                "FoodStampsInDrawer": event.find(".//FoodStampsInDrawer").text if event.find(".//FoodStampsInDrawer") is not None else None,
                "TenderCode": event.find(".//TenderCode").text if event.find(".//TenderCode") is not None else None,
            }
            if any(event_data.values()):
                data["OtherEvents"].append(event_data)

    sale_events = root.findall(".//SaleEvent")
    if sale_events:
        data["SaleEvents"] = []
        for event in sale_events:
            transaction_summary = event.find(".//TransactionSummary")
            transaction_summary_data = {}
            if transaction_summary is not None:
                transaction_summary_data = {
                    "TransactionTotalGrossAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalGrossAmount").text))),
                    "TransactionTotalNetAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalNetAmount").text))),
                    "TransactionTotalTaxSalesAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalTaxSalesAmount").text))),
                    "TransactionTotalTaxExemptAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalTaxExemptAmount").text))),
                    "TransactionTotalTaxNetAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalTaxNetAmount").text))),
                    "TransactionTotalGrandAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalGrandAmount").text))),
                }
            extension = event.find(".//Extension")
            loyalty_info = extension.find(".//LoyaltyInfo") if extension is not None else None
            loyalty_id = loyalty_info.find(".//LoyaltyID").text if loyalty_info is not None else None
            loyalty_entry_method = loyalty_info.find(".//LoyaltyEntryMethod").text if loyalty_info is not None else None
            transaction_detail_group = event.find(".//TransactionDetailGroup")
            transaction_lines = transaction_detail_group.findall(".//TransactionLine") if transaction_detail_group is not None else []
            transaction_line_data = []
            for line in transaction_lines:
                if line.get("status") != "cancel":
                    if line.find(".//ItemLine") is not None:
                        pos_code = line.find(".//POSCode").text
                        sales_restriction = line.find(".//SalesRestriction")
                        item_tax = line.find(".//ItemTax")
                        tax_level_id = int(item_tax.find(".//TaxLevelID").text) if item_tax is not None else None
                        sales_amount = float(line.find(".//SalesAmount").text)
                        sales_tax = sales_amount * tax_percentage_map.get(tax_level_id, 0)
                        promotion_amount = line.find(".//Promotion//PromotionAmount")
                        promotion_amount_value = abs(float(promotion_amount.text)) if promotion_amount is not None else 0
                        restriction_sales_details = line.find(".//RestrictedSalesDetail/CustomerID")
                        personal_id = restriction_sales_details.find(".//PersonalID").attrib.get("idType") if restriction_sales_details is not None else None
                        id_exp_date = restriction_sales_details.find(".//IDExpirationDate").text if restriction_sales_details is not None else None
                        birth_date = restriction_sales_details.find(".//BirthDate").text if restriction_sales_details is not None else None
                        transaction_line_data.append({
                            "ItemLine": {
                                "POSCode": pos_code,
                                "Description": line.find(".//Description").text,
                                "ActualSalesPrice": "{:.2f}".format(abs(float(line.find(".//ActualSalesPrice").text))),
                                "MerchandiseCode": line.find(".//MerchandiseCode").text,
                                "SellingUnits": line.find(".//SellingUnits").text,
                                "PromotionID": line.find(".//Promotion//PromotionID").text if line.find(".//PromotionID") is not None else None,
                                "PromotionAmount": "{:.2f}".format(promotion_amount_value),
                                "PersonalID": personal_id,
                                "IDExpirationDate": id_exp_date,
                                "BirthDate": birth_date,
                                "LoyaltyID": loyalty_id,
                                "LoyaltyEntryMethod": loyalty_entry_method,
                                "RegularSellPrice": "{:.2f}".format(abs(float(line.find(".//RegularSellPrice").text))),
                                "SalesQuantity": abs(float(line.find(".//SalesQuantity").text)),
                                "SalesAmount": "{:.2f}".format(abs(float(line.find(".//SalesAmount").text))),
                                "SalesTax": "{:.2f}".format(abs(sales_tax)),
                                "MinimumCustomerAge": sales_restriction.find(".//MinimumCustomerAge").text if sales_restriction is not None and sales_restriction.find(".//MinimumCustomerAge") is not None else None,
                                "TaxLevelID": item_tax.find(".//TaxLevelID").text if item_tax is not None else None,
                            }
                        })
                    elif line.find(".//MerchandiseCodeLine") is not None:
                        sales_restriction = line.find(".//SalesRestriction")
                        item_tax = line.find(".//ItemTax")
                        merchandise_code = int(line.find(".//MerchandiseCode").text)
                        sales_amount = float(line.find(".//SalesAmount").text)
                        taxid = department_tax_map.get(merchandise_code)
                        tax_percentage = tax_percentage_map.get(taxid, 0)
                        sales_tax = round(sales_amount * tax_percentage, 2)
                        transaction_line_data.append({
                            "MerchandiseCodeLine": {
                                "MerchandiseCode": line.find(".//MerchandiseCode").text,
                                "Description": line.find(".//Description").text,
                                "ActualSalesPrice": "{:.2f}".format(abs(float(line.find(".//ActualSalesPrice").text))),
                                "RegularSellPrice": "{:.2f}".format(abs(float(line.find(".//RegularSellPrice").text))),
                                "SalesQuantity": abs(float(line.find(".//SalesQuantity").text)),
                                "SalesAmount": "{:.2f}".format(abs(float(line.find(".//SalesAmount").text))),
                                "SalesTax": "{:.2f}".format(abs(sales_tax)),
                                "MinimumCustomerAge": sales_restriction.find(".//MinimumCustomerAge").text if sales_restriction is not None and sales_restriction.find(".//MinimumCustomerAge") is not None else None,
                                "TaxLevelID": item_tax.find(".//TaxLevelID").text if item_tax is not None else None,
                            }
                        })
                    elif line.find(".//TransactionTax") is not None:
                        transaction_line_data.append({
                            "TransactionTax": {
                                "TaxLevelID": line.find(".//TaxLevelID").text,
                                "TaxableSalesAmount": "{:.2f}".format(abs(float(line.find(".//TaxableSalesAmount").text))),
                                "TaxCollectedAmount": "{:.2f}".format(abs(float(line.find(".//TaxCollectedAmount").text))),
                                "TaxableSalesRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxableSalesRefundedAmount").text))),
                                "TaxRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxRefundedAmount").text))),
                                "TaxExemptSalesAmount": "{:.2f}".format(abs(float(line.find(".//TaxExemptSalesAmount").text))),
                                "TaxExemptSalesRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxExemptSalesRefundedAmount").text))),
                                "TaxForgivenSalesAmount": "{:.2f}".format(abs(float(line.find(".//TaxForgivenSalesAmount").text))),
                                "TaxForgivenSalesRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxForgivenSalesRefundedAmount").text))),
                                "TaxForgivenAmount": "{:.2f}".format(abs(float(line.find(".//TaxForgivenAmount").text))),
                            }
                        })
                    elif line.find(".//TenderInfo") is not None:
                        transaction_line_data.append({
                            "TenderInfo": {
                                "TenderCode": line.find(".//Tender/TenderCode").text,
                                "TenderAmount": "{:.2f}".format(abs(float(line.find(".//TenderAmount").text))),
                            }
                        })
            event_data = {
                "TransactionID": event.find(".//TransactionID").text,
                "CashierID": event.find(".//CashierID").text,
                "RegisterID": event.find(".//RegisterID").text,
                "TillID": event.find(".//TillID").text,
                "EventStartDate": event.find(".//EventStartDate").text,
                "EventStartTime": event.find(".//EventStartTime").text,
                "EventEndDate": event.find(".//EventEndDate").text,
                "EventEndTime": event.find(".//EventEndTime").text,
                "ReceiptDate": event.find(".//ReceiptDate").text,
                "ReceiptTime": event.find(".//ReceiptTime").text,
                "TransactionLine": transaction_line_data,
                "TransactionSummary": transaction_summary_data,
            }
            if any(event_data.values()):
                data["SaleEvents"].append(event_data)

    void_events = root.findall(".//VoidEvent")
    if void_events:
        data["VoidEvents"] = []
        for event in void_events:
            transaction_summary = event.find(".//TransactionSummary")
            transaction_summary_data = {}
            if transaction_summary is not None:
                transaction_summary_data = {
                    "TransactionTotalGrossAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalGrossAmount").text))),
                    "TransactionTotalNetAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalNetAmount").text))),
                    "TransactionTotalTaxSalesAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalTaxSalesAmount").text))),
                    "TransactionTotalTaxExemptAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalTaxExemptAmount").text))),
                    "TransactionTotalTaxNetAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalTaxNetAmount").text))),
                    "TransactionTotalGrandAmount": "{:.2f}".format(abs(float(transaction_summary.find(".//TransactionTotalGrandAmount").text))),
                }
            transaction_detail_group = event.find(".//TransactionDetailGroup")
            transaction_lines = transaction_detail_group.findall(".//TransactionLine") if transaction_detail_group is not None else []
            transaction_line_data = []
            for line in transaction_lines:
                if line.get("status") != "cancel":
                    if line.find(".//ItemLine") is not None:
                        pos_code = line.find(".//POSCode").text
                        sales_restriction = line.find(".//SalesRestriction")
                        item_tax = line.find(".//ItemTax")
                        tax_level_id = int(item_tax.find(".//TaxLevelID").text) if item_tax is not None else None
                        sales_amount = float(line.find(".//SalesAmount").text)
                        sales_tax = sales_amount * tax_percentage_map.get(tax_level_id, 0)
                        promotion_amount = line.find(".//Promotion//PromotionAmount")
                        promotion_amount_value = abs(float(promotion_amount.text)) if promotion_amount is not None else 0
                        restriction_sales_details = line.find(".//RestrictedSalesDetail/CustomerID")
                        personal_id = restriction_sales_details.find(".//PersonalID").attrib.get("idType") if restriction_sales_details is not None else None
                        id_exp_date = restriction_sales_details.find(".//IDExpirationDate").text if restriction_sales_details is not None else None
                        birth_date = restriction_sales_details.find(".//BirthDate").text if restriction_sales_details is not None else None
                        transaction_line_data.append({
                            "ItemLine": {
                                "POSCode": pos_code,
                                "Description": line.find(".//Description").text,
                                "ActualSalesPrice": "{:.2f}".format(abs(float(line.find(".//ActualSalesPrice").text))),
                                "MerchandiseCode": line.find(".//MerchandiseCode").text,
                                "SellingUnits": line.find(".//SellingUnits").text,
                                "PersonalID": personal_id,
                                "IDExpirationDate": id_exp_date,
                                "BirthDate": birth_date,
                                "LoyaltyID": None,
                                "LoyaltyEntryMethod": None,
                                "RegularSellPrice": "{:.2f}".format(abs(float(line.find(".//RegularSellPrice").text))),
                                "SalesQuantity": abs(float(line.find(".//SalesQuantity").text)),
                                "SalesAmount": "{:.2f}".format(abs(float(line.find(".//SalesAmount").text))),
                                "SalesTax": "{:.2f}".format(abs(sales_tax)),
                                "MinimumCustomerAge": sales_restriction.find(".//MinimumCustomerAge").text if sales_restriction is not None and sales_restriction.find(".//MinimumCustomerAge") is not None else None,
                                "TaxLevelID": item_tax.find(".//TaxLevelID").text if item_tax is not None else None,
                            }
                        })
                    elif line.find(".//MerchandiseCodeLine") is not None:
                        sales_restriction = line.find(".//SalesRestriction")
                        item_tax = line.find(".//ItemTax")
                        merchandise_code = int(line.find(".//MerchandiseCode").text)
                        sales_amount = float(line.find(".//SalesAmount").text)
                        taxid = department_tax_map.get(merchandise_code)
                        tax_percentage = tax_percentage_map.get(taxid, 0)
                        sales_tax = round(sales_amount * tax_percentage, 2)
                        transaction_line_data.append({
                            "MerchandiseCodeLine": {
                                "MerchandiseCode": line.find(".//MerchandiseCode").text,
                                "Description": line.find(".//Description").text,
                                "ActualSalesPrice": "{:.2f}".format(abs(float(line.find(".//ActualSalesPrice").text))),
                                "RegularSellPrice": "{:.2f}".format(abs(float(line.find(".//RegularSellPrice").text))),
                                "SalesQuantity": abs(float(line.find(".//SalesQuantity").text)),
                                "SalesAmount": "{:.2f}".format(abs(float(line.find(".//SalesAmount").text))),
                                "SalesTax": "{:.2f}".format(abs(sales_tax)),
                                "MinimumCustomerAge": sales_restriction.find(".//MinimumCustomerAge").text if sales_restriction is not None and sales_restriction.find(".//MinimumCustomerAge") is not None else None,
                                "TaxLevelID": item_tax.find(".//TaxLevelID").text if item_tax is not None else None,
                            }
                        })
                    elif line.find(".//TransactionTax") is not None:
                        transaction_line_data.append({
                            "TransactionTax": {
                                "TaxLevelID": line.find(".//TaxLevelID").text,
                                "TaxableSalesAmount": "{:.2f}".format(abs(float(line.find(".//TaxableSalesAmount").text))),
                                "TaxCollectedAmount": "{:.2f}".format(abs(float(line.find(".//TaxCollectedAmount").text))),
                                "TaxableSalesRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxableSalesRefundedAmount").text))),
                                "TaxRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxRefundedAmount").text))),
                                "TaxExemptSalesAmount": "{:.2f}".format(abs(float(line.find(".//TaxExemptSalesAmount").text))),
                                "TaxExemptSalesRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxExemptSalesRefundedAmount").text))),
                                "TaxForgivenSalesAmount": "{:.2f}".format(abs(float(line.find(".//TaxForgivenSalesAmount").text))),
                                "TaxForgivenSalesRefundedAmount": "{:.2f}".format(abs(float(line.find(".//TaxForgivenSalesRefundedAmount").text))),
                                "TaxForgivenAmount": "{:.2f}".format(abs(float(line.find(".//TaxForgivenAmount").text))),
                            }
                        })
                    elif line.find(".//TenderInfo") is not None:
                        transaction_line_data.append({
                            "TenderInfo": {
                                "TenderCode": line.find(".//Tender/TenderCode").text,
                                "TenderAmount": "{:.2f}".format(abs(float(line.find(".//TenderAmount").text))),
                            }
                        })
            event_data = {
                "TransactionID": event.find(".//TransactionID").text,
                "CashierID": event.find(".//CashierID").text,
                "RegisterID": event.find(".//RegisterID").text,
                "TillID": event.find(".//TillID").text,
                "EventStartDate": event.find(".//EventStartDate").text,
                "EventStartTime": event.find(".//EventStartTime").text,
                "EventEndDate": event.find(".//EventEndDate").text,
                "EventEndTime": event.find(".//EventEndTime").text,
                "ReceiptDate": event.find(".//ReceiptDate").text,
                "ReceiptTime": event.find(".//ReceiptTime").text,
                "TransactionLine": transaction_line_data,
                "TransactionSummary": transaction_summary_data,
                "VoidReason": event.find(".//VoidReason").text,
            }
            if any(event_data.values()):
                data["VoidEvents"].append(event_data)

    refund_events = root.findall(".//RefundEvent")
    if refund_events:
        data["RefundEvents"] = []
        for event in refund_events:
            transaction_summary = event.find(".//TransactionSummary")
            transaction_summary_data = {}
            if transaction_summary is not None:
                transaction_summary_data = {
                    "TransactionTotalGrossAmount": "{:.2f}".format(float(transaction_summary.find(".//TransactionTotalGrossAmount").text)),
                    "TransactionTotalNetAmount": "{:.2f}".format(float(transaction_summary.find(".//TransactionTotalNetAmount").text)),
                    "TransactionTotalTaxSalesAmount": "{:.2f}".format(float(transaction_summary.find(".//TransactionTotalTaxSalesAmount").text)),
                    "TransactionTotalTaxExemptAmount": "{:.2f}".format(float(transaction_summary.find(".//TransactionTotalTaxExemptAmount").text)),
                    "TransactionTotalTaxNetAmount": "{:.2f}".format(float(transaction_summary.find(".//TransactionTotalTaxNetAmount").text)),
                    "TransactionTotalGrandAmount": "{:.2f}".format(float(transaction_summary.find(".//TransactionTotalGrandAmount").text)),
                }
            transaction_detail_group = event.find(".//TransactionDetailGroup")
            transaction_lines = transaction_detail_group.findall(".//TransactionLine") if transaction_detail_group is not None else []
            transaction_line_data = []
            for line in transaction_lines:
                if line.get("status") != "cancel":
                    if line.find(".//ItemLine") is not None:
                        pos_code = line.find(".//POSCode").text
                        sales_restriction = line.find(".//SalesRestriction")
                        item_tax = line.find(".//ItemTax")
                        tax_level_id = int(item_tax.find(".//TaxLevelID").text) if item_tax is not None else None
                        sales_amount = float(line.find(".//SalesAmount").text)
                        sales_tax = sales_amount * tax_percentage_map.get(tax_level_id, 0)
                        transaction_line_data.append({
                            "ItemLine": {
                                "POSCode": pos_code,
                                "Description": line.find(".//Description").text,
                                "ActualSalesPrice": "{:.2f}".format(float(line.find(".//ActualSalesPrice").text)),
                                "MerchandiseCode": line.find(".//MerchandiseCode").text,
                                "SellingUnits": line.find(".//SellingUnits").text,
                                "RegularSellPrice": "{:.2f}".format(float(line.find(".//RegularSellPrice").text)),
                                "SalesQuantity": abs(float(line.find(".//SalesQuantity").text)),
                                "SalesAmount": "{:.2f}".format(float(line.find(".//SalesAmount").text)),
                                "SalesTax": "{:.2f}".format(sales_tax),
                                "MinimumCustomerAge": sales_restriction.find(".//MinimumCustomerAge").text if sales_restriction is not None and sales_restriction.find(".//MinimumCustomerAge") is not None else None,
                                "TaxLevelID": item_tax.find(".//TaxLevelID").text if item_tax is not None else None,
                            }
                        })
                    elif line.find(".//MerchandiseCodeLine") is not None:
                        sales_restriction = line.find(".//SalesRestriction")
                        item_tax = line.find(".//ItemTax")
                        merchandise_code = int(line.find(".//MerchandiseCode").text)
                        sales_amount = abs(float(line.find(".//SalesAmount").text))
                        taxid = department_tax_map.get(merchandise_code)
                        tax_percentage = tax_percentage_map.get(taxid, 0)
                        sales_tax = round(sales_amount * tax_percentage, 2)
                        transaction_line_data.append({
                            "MerchandiseCodeLine": {
                                "MerchandiseCode": line.find(".//MerchandiseCode").text,
                                "Description": line.find(".//Description").text,
                                "ActualSalesPrice": "{:.2f}".format(float(line.find(".//ActualSalesPrice").text)),
                                "RegularSellPrice": "{:.2f}".format(float(line.find(".//RegularSellPrice").text)),
                                "SalesQuantity": abs(float(line.find(".//SalesQuantity").text)),
                                "SalesAmount": "{:.2f}".format(float(line.find(".//SalesAmount").text)),
                                "SalesTax": "{:.2f}".format(sales_tax),
                                "MinimumCustomerAge": sales_restriction.find(".//MinimumCustomerAge").text if sales_restriction is not None and sales_restriction.find(".//MinimumCustomerAge") is not None else None,
                                "TaxLevelID": item_tax.find(".//TaxLevelID").text if item_tax is not None else None,
                            }
                        })
                    elif line.find(".//TransactionTax") is not None:
                        transaction_line_data.append({
                            "TransactionTax": {
                                "TaxLevelID": line.find(".//TaxLevelID").text,
                                "TaxableSalesAmount": "{:.2f}".format(float(line.find(".//TaxableSalesAmount").text)),
                                "TaxCollectedAmount": "{:.2f}".format(float(line.find(".//TaxCollectedAmount").text)),
                                "TaxableSalesRefundedAmount": "{:.2f}".format(float(line.find(".//TaxableSalesRefundedAmount").text)),
                                "TaxRefundedAmount": "{:.2f}".format(float(line.find(".//TaxRefundedAmount").text)),
                                "TaxExemptSalesAmount": "{:.2f}".format(float(line.find(".//TaxExemptSalesAmount").text)),
                                "TaxExemptSalesRefundedAmount": "{:.2f}".format(float(line.find(".//TaxExemptSalesRefundedAmount").text)),
                                "TaxForgivenSalesAmount": "{:.2f}".format(float(line.find(".//TaxForgivenSalesAmount").text)),
                                "TaxForgivenSalesRefundedAmount": "{:.2f}".format(float(line.find(".//TaxForgivenSalesRefundedAmount").text)),
                                "TaxForgivenAmount": "{:.2f}".format(float(line.find(".//TaxForgivenAmount").text)),
                            }
                        })
                    elif line.find(".//TenderInfo") is not None:
                        transaction_line_data.append({
                            "TenderInfo": {
                                "TenderCode": line.find(".//Tender/TenderCode").text,
                                "TenderAmount": "{:.2f}".format(float(line.find(".//TenderAmount").text)),
                            }
                        })
            event_data = {
                "TransactionID": event.find(".//TransactionID").text,
                "CashierID": event.find(".//CashierID").text,
                "RegisterID": event.find(".//RegisterID").text,
                "TillID": event.find(".//TillID").text,
                "EventStartDate": event.find(".//EventStartDate").text,
                "EventStartTime": event.find(".//EventStartTime").text,
                "EventEndDate": event.find(".//EventEndDate").text,
                "EventEndTime": event.find(".//EventEndTime").text,
                "ReceiptDate": event.find(".//ReceiptDate").text,
                "ReceiptTime": event.find(".//ReceiptTime").text,
                "TransactionLine": transaction_line_data,
                "TransactionSummary": transaction_summary_data,
                "RefundReason": event.find(".//RefundReason").text,
            }
            if any(event_data.values()):
                data["RefundEvents"].append(event_data)

    if "SaleEvents" not in data:
        data["SaleEvents"] = []
    if "OtherEvents" not in data:
        data["OtherEvents"] = []
    if "VoidEvents" not in data:
        data["VoidEvents"] = []
    if "RefundEvents" not in data:
        data["RefundEvents"] = []
    return data


def insert_if_not_exists_msm(conn, data):
    debug_print(f"insert_msm: checking StoreLocationID={data.get('StoreLocationID')}, BeginDate={data.get('BeginDate')}, BeginTime={data.get('BeginTime')}")
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM scan_msm WHERE StoreLocationID = %s AND BeginDate = %s AND BeginTime = %s LIMIT 1",
        (data["StoreLocationID"], data["BeginDate"], data["BeginTime"]),
    )
    if cur.fetchone():
        cur.close()
        debug_print("insert_msm: row already exists, skipping")
        return
    payload = json.dumps({"MSMDetail": data.get("MSMDetail", [])})
    cur.execute(
        "INSERT INTO scan_msm (StoreLocationID, BeginDate, BeginTime, EndDate, EndTime, payload) VALUES (%s, %s, %s, %s, %s, %s)",
        (data["StoreLocationID"], data["BeginDate"], data["BeginTime"], data.get("EndDate"), data.get("EndTime"), payload),
    )
    conn.commit()
    cur.close()
    debug_print("insert_msm: inserted new row")


def insert_if_not_exists_mcm(conn, data):
    debug_print(f"insert_mcm: checking storeid={storeid}, StoreLocationID={data.get('StoreLocationID')}, BeginDate={data.get('BeginDate')}")
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM scan_mcm WHERE storeid = %s AND StoreLocationID = %s AND BeginDate = %s AND BeginTime = %s LIMIT 1",
        (storeid, data["StoreLocationID"], data["BeginDate"], data["BeginTime"]),
    )
    if cur.fetchone():
        cur.close()
        debug_print("insert_mcm: row already exists, skipping")
        return
    mcm_detail = json.dumps(data.get("MCMDetail", []))
    cur.execute(
        """INSERT INTO scan_mcm (storeid, StoreLocationID, corpid, BeginDate, BeginTime, EndDate, EndTime, MCMDetail)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (storeid, data["StoreLocationID"], corpid, data["BeginDate"], data["BeginTime"], data["EndDate"], data["EndTime"], mcm_detail),
    )
    conn.commit()
    cur.close()
    debug_print("insert_mcm: inserted new row")


def insert_if_not_exists_ism(conn, data):
    debug_print(f"insert_ism: checking StoreLocationID={data.get('StoreLocationID')}, BeginDate={data.get('BeginDate')}")
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM scan_ism WHERE StoreLocationID = %s AND BeginDate = %s AND BeginTime = %s LIMIT 1",
        (data["StoreLocationID"], data["BeginDate"], data["BeginTime"]),
    )
    if cur.fetchone():
        cur.close()
        debug_print("insert_ism: row already exists, skipping")
        return
    payload = json.dumps({"ISMDetail": data.get("ISMDetail", [])})
    cur.execute(
        "INSERT INTO scan_ism (StoreLocationID, BeginDate, BeginTime, payload) VALUES (%s, %s, %s, %s)",
        (data["StoreLocationID"], data["BeginDate"], data["BeginTime"], payload),
    )
    conn.commit()
    cur.close()
    debug_print("insert_ism: inserted new row")


def insert_if_not_exists_tlm(conn, data):
    movements = data.get("TaxLevelMovement", [])
    debug_print(f"insert_tlm: processing {len(movements)} TaxLevelMovement(s)")
    cur = conn.cursor()
    inserted = 0
    for movement in movements:
        mh = movement.get("MovementHeader", {})
        cur.execute(
            "SELECT 1 FROM scan_tlm WHERE StoreLocationID = %s AND BeginDate = %s AND BeginTime = %s LIMIT 1",
            (data["StoreLocationID"], mh.get("BeginDate"), mh.get("BeginTime")),
        )
        if cur.fetchone():
            debug_print(f"insert_tlm: movement {mh.get('BeginDate')} {mh.get('BeginTime')} already exists")
            continue
        payload = json.dumps({"TaxLevelMovement": [movement]})
        cur.execute(
            "INSERT INTO scan_tlm (StoreLocationID, BeginDate, BeginTime, payload) VALUES (%s, %s, %s, %s)",
            (data["StoreLocationID"], mh.get("BeginDate"), mh.get("BeginTime"), payload),
        )
        inserted += 1
    conn.commit()
    cur.close()
    debug_print(f"insert_tlm: inserted {inserted} row(s)")


def insert_if_not_exists_tpm(conn, data):
    movements = data.get("TankProductMovement", [])
    debug_print(f"insert_tpm: processing {len(movements)} TankProductMovement(s)")
    cur = conn.cursor()
    inserted = 0
    for movement in movements:
        mh = movement.get("MovementHeader", {})
        cur.execute(
            "SELECT 1 FROM scan_tpm WHERE StoreLocationID = %s AND BeginDate = %s AND BeginTime = %s LIMIT 1",
            (data["StoreLocationID"], mh.get("BeginDate"), mh.get("BeginTime")),
        )
        if cur.fetchone():
            debug_print(f"insert_tpm: movement {mh.get('BeginDate')} {mh.get('BeginTime')} already exists")
            continue
        payload = json.dumps({"TankProductMovement": [movement]})
        cur.execute(
            "INSERT INTO scan_tpm (StoreLocationID, BeginDate, BeginTime, payload) VALUES (%s, %s, %s, %s)",
            (data["StoreLocationID"], mh.get("BeginDate"), mh.get("BeginTime"), payload),
        )
        inserted += 1
    conn.commit()
    cur.close()
    debug_print(f"insert_tpm: inserted {inserted} row(s)")


def insert_if_not_exists_cpjr(conn, data):
    debug_print(f"insert_cpjr: checking storeid={storeid}, StoreLocationID={data.get('StoreLocationID')}, BeginDate={data.get('BeginDate')}, BeginTime={data.get('BeginTime')}")
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM scan_cpjr WHERE storeid = %s AND StoreLocationID = %s AND BeginDate = %s AND BeginTime = %s LIMIT 1",
        (storeid, data["StoreLocationID"], data["BeginDate"], data["BeginTime"]),
    )
    if cur.fetchone():
        cur.close()
        debug_print("insert_cpjr: row already exists, skipping")
        return
    sale_count = len(data.get("SaleEvents", []))
    void_count = len(data.get("VoidEvents", []))
    refund_count = len(data.get("RefundEvents", []))
    debug_print(f"insert_cpjr: inserting SaleEvents={sale_count}, VoidEvents={void_count}, RefundEvents={refund_count}")
    cur.execute(
        """INSERT INTO scan_cpjr (storeid, StoreLocationID, corpid, ReportSequenceNumber, BeginDate, BeginTime, EndDate, EndTime, SaleEvents, OtherEvents, VoidEvents, RefundEvents)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            storeid,
            data["StoreLocationID"],
            corpid,
            data.get("ReportSequenceNumber"),
            data.get("BeginDate"),
            data.get("BeginTime"),
            data.get("EndDate"),
            data.get("EndTime"),
            json.dumps(data.get("SaleEvents", [])),
            json.dumps(data.get("OtherEvents", [])),
            json.dumps(data.get("VoidEvents", [])),
            json.dumps(data.get("RefundEvents", [])),
        ),
    )
    conn.commit()
    cur.close()
    debug_print("insert_cpjr: inserted new row")


def process_msm_files():
    function_name = "process_msm_files"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Started processing MSM files.")
        msm_files = glob.glob(os.path.join(directory, "MSM340*.xml"))
        debug_print(f"{function_name}: found {len(msm_files)} MSM340*.xml files in {directory}")
        files_to_process = _get_files_to_process(msm_files)
        debug_print(f"{function_name}: processing {len(files_to_process)} file(s) (PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS})")
        for msm_file in files_to_process:
            debug_print(f"{function_name}: processing file: {msm_file}")
            msm_tree = ET.parse(msm_file)
            msm_root = msm_tree.getroot()
            msm_data = extract_msm_data(msm_root)
            debug_print(f"{function_name}: extracted data StoreLocationID={msm_data.get('StoreLocationID')}, BeginDate={msm_data.get('BeginDate')}")
            conn = get_mysql_connection()
            if conn:
                try:
                    insert_if_not_exists_msm(conn, msm_data)
                finally:
                    conn.close()
            else:
                debug_print(f"{function_name}: MySQL connection failed")
        if not files_to_process:
            debug_print(f"{function_name}: no MSM files to process, skipping")
        log_message(function_name, "Finished processing MSM files.", "SUCCESS")
        debug_print(f"{function_name}: finished successfully")
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error: {str(e)}", "ERROR")


def process_mcm_files():
    function_name = "process_mcm_files"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Started processing MCM files.")
        mcm_files = glob.glob(os.path.join(directory, "MCM340*.xml"))
        debug_print(f"{function_name}: found {len(mcm_files)} MCM340*.xml files")
        files_to_process = _get_files_to_process(mcm_files)
        debug_print(f"{function_name}: processing {len(files_to_process)} file(s) (PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS})")
        for mcm_file in files_to_process:
            debug_print(f"{function_name}: processing file: {mcm_file}")
            mcm_tree = ET.parse(mcm_file)
            mcm_root = mcm_tree.getroot()
            mcm_data = extract_mcm_data(mcm_root)
            debug_print(f"{function_name}: extracted data, MCMDetail count={len(mcm_data.get('MCMDetail', []))}")
            conn = get_mysql_connection()
            if conn:
                try:
                    insert_if_not_exists_mcm(conn, mcm_data)
                finally:
                    conn.close()
            else:
                debug_print(f"{function_name}: MySQL connection failed")
        if not files_to_process:
            debug_print(f"{function_name}: no MCM files to process, skipping")
        log_message(function_name, "Finished processing MCM files.", "SUCCESS")
        debug_print(f"{function_name}: finished successfully")
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error: {str(e)}", "ERROR")


def process_ism_files():
    function_name = "process_ism_files"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Started processing ISM files.")
        ism_files = glob.glob(os.path.join(directory, "ISM340*.xml"))
        debug_print(f"{function_name}: found {len(ism_files)} ISM340*.xml files")
        files_to_process = _get_files_to_process(ism_files)
        debug_print(f"{function_name}: processing {len(files_to_process)} file(s) (PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS})")
        for ism_file in files_to_process:
            debug_print(f"{function_name}: processing file: {ism_file}")
            ism_tree = ET.parse(ism_file)
            ism_root = ism_tree.getroot()
            ism_data = extract_ism_data(ism_root)
            debug_print(f"{function_name}: extracted data, ISMDetail count={len(ism_data.get('ISMDetail', []))}")
            conn = get_mysql_connection()
            if conn:
                try:
                    insert_if_not_exists_ism(conn, ism_data)
                finally:
                    conn.close()
            else:
                debug_print(f"{function_name}: MySQL connection failed")
        if not files_to_process:
            debug_print(f"{function_name}: no ISM files to process, skipping")
        log_message(function_name, "Finished processing ISM files.", "SUCCESS")
        debug_print(f"{function_name}: finished successfully")
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error: {str(e)}", "ERROR")


def process_tlm_files():
    function_name = "process_tlm_files"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Started processing TLM files.")
        tlm_files = glob.glob(os.path.join(directory, "TLM340*.xml"))
        debug_print(f"{function_name}: found {len(tlm_files)} TLM340*.xml files")
        files_to_process = _get_files_to_process(tlm_files)
        debug_print(f"{function_name}: processing {len(files_to_process)} file(s) (PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS})")
        for tlm_file in files_to_process:
            debug_print(f"{function_name}: processing file: {tlm_file}")
            tlm_tree = ET.parse(tlm_file)
            tlm_root = tlm_tree.getroot()
            tlm_data = extract_tlm_data(tlm_root)
            debug_print(f"{function_name}: extracted data, TaxLevelMovement count={len(tlm_data.get('TaxLevelMovement', []))}")
            conn = get_mysql_connection()
            if conn:
                try:
                    insert_if_not_exists_tlm(conn, tlm_data)
                finally:
                    conn.close()
            else:
                debug_print(f"{function_name}: MySQL connection failed")
        if not files_to_process:
            debug_print(f"{function_name}: no TLM files to process, skipping")
        log_message(function_name, "Finished processing TLM files.", "SUCCESS")
        debug_print(f"{function_name}: finished successfully")
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error: {str(e)}", "ERROR")


def process_tpm_files():
    function_name = "process_tpm_files"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Started processing TPM files.")
        tpm_files = glob.glob(os.path.join(directory, "TPM340*.xml"))
        debug_print(f"{function_name}: found {len(tpm_files)} TPM340*.xml files")
        files_to_process = _get_files_to_process(tpm_files)
        debug_print(f"{function_name}: processing {len(files_to_process)} file(s) (PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS})")
        for tpm_file in files_to_process:
            debug_print(f"{function_name}: processing file: {tpm_file}")
            tpm_tree = ET.parse(tpm_file)
            tpm_root = tpm_tree.getroot()
            tpm_data = extract_tpm_data(tpm_root)
            debug_print(f"{function_name}: extracted data, TankProductMovement count={len(tpm_data.get('TankProductMovement', []))}")
            conn = get_mysql_connection()
            if conn:
                try:
                    insert_if_not_exists_tpm(conn, tpm_data)
                finally:
                    conn.close()
            else:
                debug_print(f"{function_name}: MySQL connection failed")
        if not files_to_process:
            debug_print(f"{function_name}: no TPM files to process, skipping")
        log_message(function_name, "Finished processing TPM files.", "SUCCESS")
        debug_print(f"{function_name}: finished successfully")
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error: {str(e)}", "ERROR")


def process_cpjr_files():
    function_name = "process_cpjr_files"
    try:
        debug_print(f"{function_name}: started")
        log_message(function_name, "Started processing CPJR files.")
        cpjr_files = glob.glob(os.path.join(directory, "CPJR*.xml"))
        debug_print(f"{function_name}: found {len(cpjr_files)} CPJR*.xml files")
        files_to_process = _get_files_to_process(cpjr_files)
        debug_print(f"{function_name}: processing {len(files_to_process)} file(s) (PROCESS_LAST_N_DAYS={PROCESS_LAST_N_DAYS})")
        for cpjr_file in files_to_process:
            debug_print(f"{function_name}: processing file: {cpjr_file}")
            cpjr_tree = ET.parse(cpjr_file)
            cpjr_root = cpjr_tree.getroot()
            cpjr_data = extract_cpjr_data(cpjr_root)
            debug_print(f"{function_name}: extracted data ReportSeq={cpjr_data.get('ReportSequenceNumber')}, SaleEvents={len(cpjr_data.get('SaleEvents', []))}")
            conn = get_mysql_connection()
            if conn:
                try:
                    insert_if_not_exists_cpjr(conn, cpjr_data)
                finally:
                    conn.close()
            else:
                debug_print(f"{function_name}: MySQL connection failed")
        if not files_to_process:
            debug_print(f"{function_name}: no CPJR files to process, skipping")
        log_message(function_name, "Finished processing CPJR files.", "SUCCESS")
        debug_print(f"{function_name}: finished successfully")
    except Exception as e:
        debug_print(f"{function_name}: ERROR {e}")
        log_message(function_name, f"Error: {str(e)}", "ERROR")


schedule.every(3600).seconds.do(process_msm_files)
schedule.every(3600).seconds.do(process_mcm_files)
schedule.every(3600).seconds.do(process_ism_files)
schedule.every(3600).seconds.do(process_tlm_files)
schedule.every(3600).seconds.do(process_tpm_files)
schedule.every(3600).seconds.do(process_cpjr_files)

# Run all jobs immediately at startup (then repeat every 3600s)
debug_print("Main: running initial pass (all jobs)...")
process_msm_files()
process_mcm_files()
process_ism_files()
process_tlm_files()
process_tpm_files()
process_cpjr_files()
debug_print("Main: initial pass complete. Schedules registered (every 3600s). Entering loop (check every 25s).")

log_status("Active")
try:
    loop_count = 0
    while True:
        schedule.run_pending()
        time.sleep(25)
        loop_count += 1
        if loop_count % 12 == 0:  # ~every 5 min
            log_message("main", "Script is running", status="INFO")
            debug_print(f"Main: loop #{loop_count}, heartbeat logged")
except KeyboardInterrupt:
    debug_print("Main: KeyboardInterrupt received")
    log_status("Inactive")
    log_message("main", "Script terminated by user (KeyboardInterrupt).", "ERROR")
except Exception as e:
    debug_print(f"Main: unhandled exception: {e}")
    log_status("Inactive")
    log_message("main", f"Script terminated due to an error: {str(e)}", "ERROR")
finally:
    debug_print("Main: finally block, setting Inactive")
    log_status("Inactive")
    log_message("main", "Script has been terminated.", "INFO")
