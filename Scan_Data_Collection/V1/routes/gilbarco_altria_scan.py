# Gilbarco Altria Scan – MySQL version
# Checks scan_connector_logs for Gilbarco_Weekly = Completed, reads scan_weekly, scan_stores,
# scan_brand, scan_upc, scan_data_dept; builds Altria scan rows; inserts into scan_data.

import json
from datetime import datetime

from flask import jsonify, Blueprint

from db_config import get_mysql_connection

gilbarco_altria_scan_bp = Blueprint("gilbarco_altria_scan_bp", __name__)

LOG_FUNCTION_CHECK = "Gilbarco_Weekly"
LOG_FUNCTION_SELF = "Gilbarco_Altria_Scan"
CORPID_DEFAULT = "65361d1bc436047c00231e45"
SUBMITTER_NUM = "9305"


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
    if s is None:
        return None
    s = str(s)
    return "".join(c for c in s if c in ("\t", "\n", "\r") or ord(c) >= 32)


def _get_merchandise_codes(conn, storeid):
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT scanDept FROM scan_data_dept WHERE storeid = %s LIMIT 1", (storeid,))
    row = cur.fetchone()
    cur.close()
    scan_dept = _parse_json(row["scanDept"]) if row and row.get("scanDept") else []
    if not isinstance(scan_dept, list):
        return set()
    return {str(d.get("sysid", "")) for d in scan_dept if isinstance(d, dict) and d.get("sysid") is not None}


def _get_upc_codes(conn, cycle_code):
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT UPCCodes FROM scan_upc WHERE cycleCode = %s LIMIT 1", (cycle_code,))
    row = cur.fetchone()
    cur.close()
    codes = _parse_json(row["UPCCodes"]) if row and row.get("UPCCodes") else []
    return [str(c) for c in (codes or []) if c is not None]


@gilbarco_altria_scan_bp.route("/gilbarco_altria_scan_data_extract_and_upload/<storeid>", methods=["GET"])
def gilbarco_altria_scan_data_extract_and_upload(storeid):
    log_id = None
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"error": "MySQL connection failed"}), 500

    try:
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """SELECT id, status FROM scan_connector_logs WHERE function_name = %s ORDER BY id DESC LIMIT 1""",
            (LOG_FUNCTION_CHECK,),
        )
        latest_check = cur.fetchone()

        if not latest_check or latest_check.get("status") != "Completed":
            log_id_val = f"{storeid}_{CORPID_DEFAULT}_log"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (log_id_val, storeid, CORPID_DEFAULT, LOG_FUNCTION_SELF, "Latest 'Gilbarco_Weekly' was not completed. Aborting Gilbarco_Altria_Scan Data generation.", "Failed", now),
            )
            conn.commit()
            conn.close()
            return jsonify({"message": "Latest 'Gilbarco_Weekly' was not completed. Aborting Gilbarco_Altria_Scan Data generation."}), 400

        log_id_val = f"{storeid}_{CORPID_DEFAULT}_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, storeid, CORPID_DEFAULT, LOG_FUNCTION_SELF, "API triggered", "Initialized", now),
        )
        conn.commit()
        log_id = cur.lastrowid

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Processing", "Fetching latest weekly data", log_id),
        )
        conn.commit()

        cur.execute(
            """SELECT id, storeid, corpid, BeginDate, EndDate, ReportSequenceNumbers, SaleEvent FROM scan_weekly
               WHERE storeid = %s ORDER BY id DESC LIMIT 1""",
            (storeid,),
        )
        latest_doc = cur.fetchone()
        if not latest_doc:
            cur.execute(
                """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
                ("Failed", "No weekly data found for this store ID", log_id),
            )
            conn.commit()
            conn.close()
            return jsonify({"status": "error", "message": "No weekly data found for this store ID"}), 404

        sale_event = _parse_json(latest_doc.get("SaleEvent"))
        if not isinstance(sale_event, list):
            sale_event = []
        week_ending_date = sale_event[-1].get("ReceiptDate") if sale_event and isinstance(sale_event[-1], dict) else latest_doc.get("EndDate")

        cur.execute("SELECT id, store_name, zip_code, retailer_acc_no AS rcn, address, city, state FROM scan_stores WHERE id = %s", (storeid,))
        store_doc = cur.fetchone()
        store_name = store_doc.get("store_name") if store_doc else None
        zip_code = store_doc.get("zip_code") if store_doc else None
        rcn = store_doc.get("rcn") if store_doc else None
        address = store_doc.get("address") if store_doc else None
        city = store_doc.get("city") if store_doc else None
        state = store_doc.get("state") if store_doc else None

        cur.execute("SELECT payload FROM scan_brand WHERE storeid = %s LIMIT 1", (storeid,))
        manf_row = cur.fetchone()
        manf = _parse_json(manf_row["payload"]) if manf_row and manf_row.get("payload") else None

        merchandise_codes = _get_merchandise_codes(conn, storeid)
        upc_pm_usa = _get_upc_codes(conn, "PM USA")
        upc_jmc = _get_upc_codes(conn, "JMC")
        keys_to_process = ["PM USA", "JMC", "USSTC", "Helix Innovations", "NJOY"]

        data = []
        for event in sale_event:
            if not isinstance(event, dict):
                continue
            transaction_id = event.get("TransactionID")
            receipt_date = event.get("ReceiptDate")
            receipt_time = event.get("TransactionTime")
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

                if upc_pm_usa:
                    for upc in upc_pm_usa:
                        poscode_slice = poscode[2:-1] if len(str(poscode)) == 14 else poscode
                        upc_to_compare = upc[:-1] if len(str(upc)) > 1 else upc
                        if str(poscode_slice) == str(upc_to_compare) and salesquantity >= 2:
                            multi_pack_indicator = "Y"
                            multi_pack_required_quantity = int(float(salesquantity))
                            multi_pack_discount_amount = "{:.2f}".format(multi_pack_required_quantity * 0.10)
                            break

                if multi_pack_indicator != "Y" and upc_jmc:
                    for upc in upc_jmc:
                        poscode_slice = poscode[2:] if len(str(poscode)) == 14 else poscode
                        if str(poscode_slice) == str(upc) and salesquantity >= 1:
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

        def _json_default(obj):
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        data_json = json.dumps(data, default=_json_default)

        cur.execute(
            """INSERT INTO scan_data (storeid, week_ending_date, brand, corpid, submitternum, submitdate, storenum, transactions, status, substatus, txtURL, data)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                storeid,
                week_ending_date,
                "Altria",
                latest_doc.get("corpid") or CORPID_DEFAULT,
                SUBMITTER_NUM,
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
        inserted_id = cur.lastrowid

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Completed", "Scan data inserted successfully", log_id),
        )
        conn.commit()

        conn.close()
        return jsonify({
            "status": "success",
            "message": "Gilbarco Altria Scan data processed and inserted successfully",
            "inserted_id": str(inserted_id),
        }), 200

    except Exception as e:
        try:
            cur = conn.cursor()
            if log_id:
                cur.execute(
                    """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
                    ("Failed", str(e), log_id),
                )
            else:
                cur.execute(
                    """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (f"{storeid}_{CORPID_DEFAULT}_log", storeid, CORPID_DEFAULT, LOG_FUNCTION_SELF, str(e), "Failed", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()
        return jsonify({"status": "error", "message": str(e)}), 500
