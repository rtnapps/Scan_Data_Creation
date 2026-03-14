# Gilbarco Circana Scan Data – MySQL version
# http://localhost:8000/gilbarco_circana_scan_data_extract_and_upload/65d83ff360d8fb8e5b10b00d

import json
from datetime import datetime

from flask import jsonify, Blueprint

from db_config import get_mysql_connection

gilbarco_circana_scan_bp = Blueprint("gilbarco_circana_scan_bp", __name__)

LOG_FUNCTION_CHECK = "Gilbarco_ITG_Weekly"
LOG_FUNCTION_SELF = "Gilbarco_Circana_Scan"
CORPID_DEFAULT = "65361d1bc436047c00231e45"
BRAND_LABEL = "Circana"
SUBMITTER_NUM = "298694"


def _parse_json(val):
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return None
    return val


def remove_duplicates_based_on_date(data):
    seen_dates = set()
    unique_data = []
    duplicates_count = 0
    removed_zero_price_count = 0
    for obj in data:
        transaction_date = obj["Transaction Date/Time"]
        price = obj["Price"]
        if price == "0" or price == "0.00":
            removed_zero_price_count += 1
            continue
        if transaction_date in seen_dates:
            duplicates_count += 1
        else:
            seen_dates.add(transaction_date)
            unique_data.append(obj)
    return unique_data, duplicates_count, removed_zero_price_count


def _get_specific_merchandise_codes_safe(conn, storeid):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT sysid FROM scan_data_dept_item WHERE storeid = %s ORDER BY sort_order",
        (storeid,),
    )
    rows = cur.fetchall()
    cur.close()
    codes = {str(r["sysid"]) for r in (rows or []) if r.get("sysid") is not None}
    if codes:
        return codes
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT scanDept FROM scan_data_dept WHERE storeid = %s", (storeid,))
    row = cur.fetchone()
    cur.close()
    sdd = _parse_json(row["scanDept"]) if row and row.get("scanDept") else []
    return {str(d.get("sysid")) for d in sdd or [] if isinstance(d, dict) and d.get("sysid") is not None}


def _get_brand_upc_codes_safe(conn, storeid, brand_type):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT poscode FROM scan_brand_upc WHERE storeid = %s AND brand_type = %s ORDER BY sort_order",
        (storeid, brand_type),
    )
    rows = cur.fetchall()
    cur.close()
    codes = [str(r["poscode"]) for r in (rows or []) if r.get("poscode") is not None]
    if codes:
        return codes
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT payload FROM scan_brand WHERE storeid = %s LIMIT 1", (storeid,))
    row = cur.fetchone()
    cur.close()
    payload = _parse_json(row["payload"]) if row and row.get("payload") else {}
    return [str(c) for c in (payload.get(brand_type) or []) if c is not None]


@gilbarco_circana_scan_bp.route("/gilbarco_circana_scan_data_extract_and_upload/<storeid>", methods=["GET"])
def gilbarco_circana_scan_data_extract_and_upload(storeid):
    log_row_id = None
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"error": "MySQL connection failed"}), 500

    try:
        cur = conn.cursor(dictionary=True)

        # Latest "Gilbarco_ITG_Weekly" log entry
        cur.execute(
            """SELECT id, function_name, status FROM scan_connector_logs
               WHERE function_name = %s ORDER BY id DESC LIMIT 1""",
            (LOG_FUNCTION_CHECK,),
        )
        latest_check = cur.fetchone()

        # If no record found OR status is not "Completed", then abort
        if not latest_check or latest_check.get("status") != "Completed":
            log_id_val = f"{storeid}_{CORPID_DEFAULT}_log"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    log_id_val,
                    storeid,
                    CORPID_DEFAULT,
                    LOG_FUNCTION_SELF,
                    "Latest 'Gilbarco_ITG_Weekly' was not completed. Aborting Gilbarco_Circana_Scan Data generation.",
                    "Failed",
                    now,
                ),
            )
            conn.commit()
            conn.close()
            return jsonify(
                {
                    "message": "Latest 'Gilbarco_ITG_Weekly' was not completed. Aborting Gilbarco_Circana_Scan Data generation."
                }
            ), 400

        # Stage 1: log initialization
        log_id_val = f"{storeid}_{CORPID_DEFAULT}_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, storeid, CORPID_DEFAULT, LOG_FUNCTION_SELF, "API triggered", "Initialized", now),
        )
        conn.commit()
        log_row_id = cur.lastrowid

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Processing", "Fetching latest weekly data", log_row_id),
        )
        conn.commit()

        # Latest ITG weekly row for this store
        cur.execute(
            """SELECT storeid, corpid, EndDate, SaleEvent FROM scan_itg_weekly
               WHERE storeid = %s ORDER BY id DESC LIMIT 1""",
            (storeid,),
        )
        latest_doc = cur.fetchone()
        cur.close()
        if not latest_doc:
            cur = conn.cursor()
            cur.execute(
                """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
                ("Failed", "No scan_itg_weekly row found for this store", log_row_id),
            )
            conn.commit()
            conn.close()
            return jsonify({"error": "No scan_itg_weekly row found for this store"}), 404

        sale_event = _parse_json(latest_doc.get("SaleEvent")) or []
        week_ending_date = latest_doc.get("EndDate")

        # Reference data from MySQL
        merchandise_codes = _get_specific_merchandise_codes_safe(conn, storeid)
        itg_upc_codes = _get_brand_upc_codes_safe(conn, storeid, "ITG")
        rjr_upc_codes = _get_brand_upc_codes_safe(conn, storeid, "RJR")

        # Store info (Circana name / site details)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, store_name, zip_code, address, city, state, payload FROM scan_stores WHERE id = %s",
            (storeid,),
        )
        store_doc = cur.fetchone()
        cur.close()
        payload = _parse_json(store_doc["payload"]) if store_doc and store_doc.get("payload") else {} if store_doc else {}
        store_name = payload.get("circana_name") or (store_doc["store_name"] if store_doc else None)
        zip_code = store_doc["zip_code"] if store_doc else None
        site_id = payload.get("site_id") or (store_doc.get("site_id") if store_doc else None)
        address = store_doc.get("address") or payload.get("address") if store_doc else None
        city = store_doc.get("city") or payload.get("city") if store_doc else None
        state = store_doc.get("state") or payload.get("state") if store_doc else None

        # RJR promo payload
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT payload FROM scan_rjr_promo WHERE storeid = %s", (storeid,))
        rjr_promo_row = cur.fetchone()
        cur.close()
        rjr_promo_doc = _parse_json(rjr_promo_row["payload"]) if rjr_promo_row and rjr_promo_row.get("payload") else {}

        data = []
        for event in sale_event or []:
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
                poscode = str(item.get("POSCode", ""))
                if poscode not in aggregated_items:
                    aggregated_items[poscode] = {
                        "Description": item.get("Description"),
                        "MerchandiseCode": item.get("MerchandiseCode"),
                        "SalesQuantity": 0,
                        "SalesAmount": 0.0,
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

                if poscode in itg_upc_codes:
                    manufacturer_buy_down_description = "ITG"
                elif poscode in rjr_upc_codes:
                    manufacturer_buy_down_description = "RJR"
                else:
                    manufacturer_buy_down_description = None

                buydown_amount = None
                manufacturer_multi_pack_discount_amount = None
                manufacturer_multi_pack_quantity = None
                promotion_flag = "N"
                manufacturer_multi_pack_flag = "N"
                manufacturer_promotion_description = None

                if manufacturer_buy_down_description == "RJR":
                    cur = conn.cursor(dictionary=True)
                    cur.execute(
                        "SELECT payload FROM scan_gbpricebook WHERE storeid = %s AND POSCode = %s LIMIT 1",
                        (storeid, poscode),
                    )
                    gbpb = cur.fetchone()
                    cur.close()
                    brand_name = None
                    if gbpb and gbpb.get("payload"):
                        pb = _parse_json(gbpb["payload"]) if isinstance(gbpb["payload"], str) else gbpb["payload"]
                        brand_name = pb.get("Brand") if pb else None
                    if not brand_name and gbpb:
                        brand_name = gbpb.get("Brand")
                    if (
                        brand_name
                        and rjr_promo_doc
                        and "Discounts" in rjr_promo_doc
                        and brand_name in rjr_promo_doc["Discounts"]
                    ):
                        single_pack_value = float(rjr_promo_doc["Discounts"][brand_name]["SinglePack"])
                        buydown_amount = "{:.2f}".format(single_pack_value)
                        if salesquantity >= 2:
                            multi_pack_value = float(rjr_promo_doc["Discounts"][brand_name]["MultiPack"])
                            manufacturer_multi_pack_discount_amount = "{:.2f}".format(
                                multi_pack_value * salesquantity
                            )
                            manufacturer_multi_pack_quantity = salesquantity
                            manufacturer_multi_pack_flag = "Y"
                            manufacturer_promotion_description = "RJR MP"
                    else:
                        buydown_amount = "0.20"
                elif manufacturer_buy_down_description == "ITG":
                    salesamount_float = float(salesamount)
                    buydown_amount = "{:.2f}".format(salesamount_float - int(salesamount_float))
                else:
                    buydown_amount = None

                manufacturer_disc_amount = None
                if buydown_amount is not None:
                    promotion_flag = "Y"
                    manufacturer_disc_amount = float(buydown_amount)
                if manufacturer_multi_pack_discount_amount is not None:
                    manufacturer_disc_amount = (manufacturer_disc_amount or 0.0) + float(
                        manufacturer_multi_pack_discount_amount
                    )
                if manufacturer_disc_amount is not None:
                    manufacturer_disc_amount = "{:.2f}".format(manufacturer_disc_amount)

                row = {
                    "Outlet Name": store_name,
                    "Outlet Number": site_id,
                    "Address 1": address,
                    "Address 2": None,
                    "City": city,
                    "State": state,
                    "Zip": zip_code,
                    "Transaction Date/Time": f"{receipt_date}-{receipt_time}",
                    "Market Basket ID": f"{site_id}{transaction_id}" if site_id else transaction_id,
                    "Scan ID": transaction_id,
                    "Register ID": 1,
                    "Quantity": salesquantity,
                    "Price": salesamount,
                    "UPC Code": poscode,
                    "UPC Description": description,
                    "Unit of Measure": "PACK",
                    "Promo Flag": promotion_flag,
                    "Outlet MultiPack Flag": "N",
                    "Outlet MultiPack Quantity": None,
                    "Outlet MultiPack Disc Amount": None,
                    "Acct Promo Name": None,
                    "Acct Disc Amount": None,
                    "Manufacturer Disc Amount": manufacturer_disc_amount,
                    "PID Coupon": None,
                    "PID Coupon Amt": None,
                    "Mfg MultiPack Flag": manufacturer_multi_pack_flag,
                    "Mfg MultiPack Quantity": manufacturer_multi_pack_quantity,
                    "Mfg MultiPack Discount Amount": manufacturer_multi_pack_discount_amount,
                    "Mfg Promo Desc": manufacturer_buy_down_description,
                    "Mfg BuyDown Desc": manufacturer_buy_down_description,
                    "Mfg BuyDown Amt": buydown_amount,
                    "Mfg MultiPack Desc": manufacturer_promotion_description,
                    "Account Loyalty ID": None,
                    "Coupon Description": None,
                }
                data.append(row)

        # Remove duplicates and zero-priced
        data_without_duplicates, duplicates_count, removed_zero_price_count = remove_duplicates_based_on_date(data)

        # Insert into scan_data
        data_json = json.dumps(data_without_duplicates)
        ins = conn.cursor()
        ins.execute(
            """INSERT INTO scan_data (storeid, week_ending_date, brand, corpid, submitternum, submitdate, storenum, transactions, status, substatus, txtURL, data)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                storeid,
                week_ending_date,
                BRAND_LABEL,
                latest_doc.get("corpid") or CORPID_DEFAULT,
                SUBMITTER_NUM,
                datetime.now().date().isoformat(),
                "1",
                str(len(data_without_duplicates)),
                0,
                "Submitted",
                "",
                data_json,
            ),
        )
        conn.commit()
        ins.close()

        # Mark log as completed
        cur = conn.cursor()
        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Completed", "Scan data inserted successfully", log_row_id),
        )
        conn.commit()

        conn.close()
        return (
            jsonify(
                {
                    "message": "Gilbarco Circana Scan data processed and inserted successfully",
                    "transactions": len(data_without_duplicates),
                    "duplicates_removed": duplicates_count,
                    "zero_price_removed": removed_zero_price_count,
                }
            ),
            200,
        )

    except Exception as e:
        try:
            cur = conn.cursor()
            if log_row_id:
                cur.execute(
                    """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
                    ("Failed", str(e), log_row_id),
                )
            else:
                cur.execute(
                    """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        f"{storeid}_{CORPID_DEFAULT}_log",
                        storeid,
                        CORPID_DEFAULT,
                        LOG_FUNCTION_SELF,
                        str(e),
                        "Failed",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()
        return jsonify({"error": str(e)}), 500