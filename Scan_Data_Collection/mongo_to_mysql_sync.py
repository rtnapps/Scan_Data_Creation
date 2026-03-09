"""
One-time (or periodic) sync: copy reference data from MongoDB to MySQL.
Run this BEFORE switching Scan_Data_Collection scripts to read only from MySQL.
Usage: python mongo_to_mysql_sync.py
"""
import json
import logging
import os
import sys
from datetime import date, datetime, time
from bson import ObjectId
from pymongo import MongoClient
from db_config import get_mysql_connection

MONGO_URI = "mongodb+srv://rtnsmart:elneebekRf3dH30z@rtn.gfl4v.mongodb.net/"
MONGO_DB = "verifone"

# Log file path (same directory as script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILENAME = os.path.join(SCRIPT_DIR, f"mongo_to_mysql_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


def setup_logging():
    """Configure logging to write to both console and log file."""
    logger = logging.getLogger("mongo_to_mysql_sync")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(LOG_FILENAME, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def log(msg, level="INFO"):
    """Write message to both console and log file."""
    if level == "DEBUG":
        LOGGER.debug(msg)
    elif level == "INFO":
        LOGGER.info(msg)
    elif level == "WARNING":
        LOGGER.warning(msg)
    elif level == "ERROR":
        LOGGER.error(msg)
    else:
        LOGGER.info(msg)


LOGGER = setup_logging()


def json_serial(obj):
    """Convert MongoDB/BSON types to JSON-serializable form."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def sync_stores(mongo_db, conn):
    log("=== sync_stores: START ===", "DEBUG")
    coll = mongo_db["stores"]
    doc_count = coll.count_documents({})
    log(f"sync_stores: MongoDB collection 'stores' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    for doc in coll.find():
        row_num += 1
        sid = str(doc["_id"])
        log(f"sync_stores: Processing row {row_num}/{doc_count} - store id={sid}", "DEBUG")
        try:
            cursor.execute(
                """
                INSERT INTO scan_stores (id, store_name, owner_gmail, retailer_acc_no, address, city, state, zip_code, drive, circana_submitterid, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    store_name = VALUES(store_name), owner_gmail = VALUES(owner_gmail), retailer_acc_no = VALUES(retailer_acc_no),
                    address = VALUES(address), city = VALUES(city), state = VALUES(state), zip_code = VALUES(zip_code),
                    drive = VALUES(drive), circana_submitterid = VALUES(circana_submitterid), payload = VALUES(payload)
                """,
                (
                    sid,
                    doc.get("store_name"),
                    doc.get("owner_gmail"),
                    doc.get("retailer_acc_no"),
                    doc.get("address"),
                    doc.get("city"),
                    doc.get("state"),
                    doc.get("zip_code"),
                    doc.get("drive"),
                    doc.get("circana_submitterid"),
                    json.dumps(doc, default=json_serial) if doc else None,
                ),
            )
            log(f"sync_stores: Row {row_num} - INSERT/UPDATE OK for storeid={sid}", "DEBUG")
        except Exception as e:
            log(f"sync_stores: ERROR on row {row_num} storeid={sid}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_stores: Committed {row_num} rows to MySQL", "DEBUG")
    cursor.close()
    log(f"  stores: synced {doc_count} rows")


def sync_scan_data_dept(mongo_db, conn):
    log("=== sync_scan_data_dept: START ===", "DEBUG")
    coll = mongo_db["ScanDataDept"]
    doc_count = coll.count_documents({})
    log(f"sync_scan_data_dept: MongoDB collection 'ScanDataDept' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    skipped = 0
    for doc in coll.find():
        storeid = doc.get("storeid")
        if not storeid:
            skipped += 1
            log(f"sync_scan_data_dept: Skipping doc (no storeid) - skipped count={skipped}", "DEBUG")
            continue
        storeid = str(storeid) if isinstance(storeid, ObjectId) else storeid
        row_num += 1
        log(f"sync_scan_data_dept: Processing row {row_num} - storeid={storeid}", "DEBUG")
        scan_dept_raw = doc.get("scanDept", [])
        scan_dept = json.dumps(scan_dept_raw, default=json_serial)
        try:
            cursor.execute(
                """
                INSERT INTO scan_data_dept (storeid, scanDept) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE scanDept = VALUES(scanDept)
                """,
                (storeid, scan_dept),
            )
            cursor.execute("DELETE FROM scan_data_dept_item WHERE storeid = %s", (storeid,))
            for i, item in enumerate(scan_dept_raw or []):
                if not isinstance(item, dict):
                    continue
                cursor.execute(
                    """INSERT INTO scan_data_dept_item (storeid, sysid, name, taxid, sort_order) VALUES (%s, %s, %s, %s, %s)""",
                    (storeid, str(item.get("sysid", "")), item.get("name"), item.get("taxid"), i),
                )
            log(f"sync_scan_data_dept: Row {row_num} - INSERT/UPDATE OK for storeid={storeid}, {len(scan_dept_raw or [])} items", "DEBUG")
        except Exception as e:
            log(f"sync_scan_data_dept: ERROR on row {row_num} storeid={storeid}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_scan_data_dept: Committed {row_num} rows, skipped {skipped}", "DEBUG")
    cursor.close()
    log(f"  scan_data_dept: synced {row_num} rows")


def sync_brand(mongo_db, conn):
    log("=== sync_brand: START ===", "DEBUG")
    coll = mongo_db["brand"]
    doc_count = coll.count_documents({})
    log(f"sync_brand: MongoDB collection 'brand' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    skipped = 0
    for doc in coll.find():
        storeid = doc.get("storeid")
        if not storeid:
            skipped += 1
            log(f"sync_brand: Skipping doc (no storeid) - skipped count={skipped}", "DEBUG")
            continue
        storeid = str(storeid) if isinstance(storeid, ObjectId) else storeid
        row_num += 1
        log(f"sync_brand: Processing row {row_num} - storeid={storeid}", "DEBUG")
        payload = json.dumps(doc, default=json_serial)
        try:
            cursor.execute(
                "INSERT INTO scan_brand (storeid, payload) VALUES (%s, %s)",
                (storeid, payload),
            )
            cursor.execute("DELETE FROM scan_brand_upc WHERE storeid = %s", (storeid,))
            for i, poscode in enumerate(doc.get("ITG") or []):
                cursor.execute(
                    "INSERT INTO scan_brand_upc (storeid, brand_type, poscode, sort_order) VALUES (%s, 'ITG', %s, %s)",
                    (storeid, str(poscode), i),
                )
            for i, poscode in enumerate(doc.get("RJR") or []):
                cursor.execute(
                    "INSERT INTO scan_brand_upc (storeid, brand_type, poscode, sort_order) VALUES (%s, 'RJR', %s, %s)",
                    (storeid, str(poscode), len(doc.get("ITG") or []) + i),
                )
            log(f"sync_brand: Row {row_num} - INSERT OK for storeid={storeid}, ITG={len(doc.get('ITG') or [])}, RJR={len(doc.get('RJR') or [])}", "DEBUG")
        except Exception as e:
            log(f"sync_brand: ERROR on row {row_num} storeid={storeid}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_brand: Committed {row_num} rows, skipped {skipped}", "DEBUG")
    cursor.close()
    log(f"  brand: synced {row_num} rows")


def sync_upc(mongo_db, conn):
    log("=== sync_upc: START ===", "DEBUG")
    coll = mongo_db["upc"]
    doc_count = coll.count_documents({})
    log(f"sync_upc: MongoDB collection 'upc' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    for doc in coll.find():
        row_num += 1
        cycle = doc.get("cycleCode")
        log(f"sync_upc: Processing row {row_num} - cycleCode={cycle}", "DEBUG")
        upc_codes_raw = doc.get("UPCCodes", [])
        upc_codes = json.dumps(upc_codes_raw, default=json_serial)
        payload = json.dumps(doc, default=json_serial)
        try:
            cursor.execute(
                "INSERT INTO scan_upc (cycleCode, UPCCodes, payload) VALUES (%s, %s, %s)",
                (cycle, upc_codes, payload),
            )
            parent_id = cursor.lastrowid
            for i, code in enumerate(upc_codes_raw or []):
                code_str = str(code) if code is not None else ""
                cursor.execute(
                    "INSERT INTO scan_upc_code (upc_parent_id, cycleCode, upc_code, sort_order) VALUES (%s, %s, %s, %s)",
                    (parent_id, cycle, code_str, i),
                )
            log(f"sync_upc: Row {row_num} - INSERT OK for cycleCode={cycle}, {len(upc_codes_raw or [])} codes", "DEBUG")
        except Exception as e:
            log(f"sync_upc: ERROR on row {row_num} cycleCode={cycle}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_upc: Committed {row_num} rows", "DEBUG")
    cursor.close()
    log(f"  upc: synced {row_num} rows")


def sync_gb_department(mongo_db, conn):
    log("=== sync_gb_department: START ===", "DEBUG")
    coll = mongo_db["gbDepartment"]
    doc_count = coll.count_documents({})
    log(f"sync_gb_department: MongoDB collection 'gbDepartment' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    skipped = 0
    for doc in coll.find():
        sid = doc.get("storeid")
        if not sid:
            skipped += 1
            log(f"sync_gb_department: Skipping doc (no storeid) - skipped count={skipped}", "DEBUG")
            continue
        sid = str(sid) if isinstance(sid, ObjectId) else sid
        row_num += 1
        log(f"sync_gb_department: Processing row {row_num} - storeid={sid}", "DEBUG")
        depts_raw = doc.get("departments", [])
        depts = json.dumps(depts_raw, default=json_serial)
        try:
            cursor.execute(
                """
                INSERT INTO scan_gb_department (storeid, departments) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE departments = VALUES(departments)
                """,
                (sid, depts),
            )
            cursor.execute("DELETE FROM scan_gb_department_item WHERE storeid = %s", (sid,))
            for i, item in enumerate(depts_raw or []):
                if not isinstance(item, dict):
                    continue
                cursor.execute(
                    """INSERT INTO scan_gb_department_item (storeid, name, sysid, taxid, negative, discountable, product_code, food_stampable, age_restriction, fractional_quantity_allowed, sort_order)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        sid,
                        item.get("name"),
                        str(item.get("sysid", "")),
                        item.get("taxid"),
                        item.get("negative"),
                        item.get("discountable"),
                        item.get("product_code"),
                        item.get("food_stampable"),
                        item.get("age_restriction"),
                        item.get("fractional_quantity_allowed"),
                        i,
                    ),
                )
            log(f"sync_gb_department: Row {row_num} - INSERT/UPDATE OK for storeid={sid}, {len(depts_raw or [])} department items", "DEBUG")
        except Exception as e:
            log(f"sync_gb_department: ERROR on row {row_num} storeid={sid}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_gb_department: Committed {row_num} rows, skipped {skipped}", "DEBUG")
    cursor.close()
    log(f"  gb_department: synced {row_num} rows")


def _parse_tax_percent(val):
    """Parse tax% from '5%' or '5' or 5 to decimal."""
    if val is None:
        return None
    s = str(val).strip().rstrip("%")
    try:
        return float(s) / 100 if s else None
    except (ValueError, TypeError):
        return None


def sync_gbtax(mongo_db, conn):
    log("=== sync_gbtax: START ===", "DEBUG")
    coll = mongo_db["gbtax"]
    doc_count = coll.count_documents({})
    log(f"sync_gbtax: MongoDB collection 'gbtax' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    skipped = 0
    for doc in coll.find():
        sid = doc.get("storeid")
        if not sid:
            skipped += 1
            log(f"sync_gbtax: Skipping doc (no storeid) - skipped count={skipped}", "DEBUG")
            continue
        sid = str(sid) if isinstance(sid, ObjectId) else sid
        row_num += 1
        log(f"sync_gbtax: Processing row {row_num} - storeid={sid}", "DEBUG")
        tax_info_raw = doc.get("taxInfo", [])
        tax_info = json.dumps(tax_info_raw, default=json_serial)
        try:
            cursor.execute(
                """
                INSERT INTO scan_gbtax (storeid, taxInfo) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE taxInfo = VALUES(taxInfo)
                """,
                (sid, tax_info),
            )
            cursor.execute("DELETE FROM scan_gbtax_item WHERE storeid = %s", (sid,))
            for i, item in enumerate(tax_info_raw or []):
                if not isinstance(item, dict):
                    continue
                taxid = str(item.get("taxid", ""))
                tax_pct = _parse_tax_percent(item.get("tax%") or item.get("tax_percent"))
                cursor.execute(
                    """INSERT INTO scan_gbtax_item (storeid, taxid, tax_percent, sort_order) VALUES (%s, %s, %s, %s)""",
                    (sid, taxid, tax_pct, i),
                )
            log(f"sync_gbtax: Row {row_num} - INSERT/UPDATE OK for storeid={sid}, {len(tax_info_raw or [])} tax items", "DEBUG")
        except Exception as e:
            log(f"sync_gbtax: ERROR on row {row_num} storeid={sid}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_gbtax: Committed {row_num} rows, skipped {skipped}", "DEBUG")
    cursor.close()
    log(f"  gbtax: synced {row_num} rows")


def sync_gb_price_promotion(mongo_db, conn):
    log("=== sync_gb_price_promotion: START ===", "DEBUG")
    coll = mongo_db["gbPricePromotion"]
    doc_count = coll.count_documents({})
    log(f"sync_gb_price_promotion: MongoDB collection 'gbPricePromotion' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    skipped = 0
    for doc in coll.find():
        storeid = doc.get("storeid")
        if not storeid:
            skipped += 1
            log(f"sync_gb_price_promotion: Skipping doc (no storeid) - skipped count={skipped}", "DEBUG")
            continue
        storeid = str(storeid) if isinstance(storeid, ObjectId) else storeid
        row_num += 1
        month = doc.get("month", "")
        log(f"sync_gb_price_promotion: Processing row {row_num} - storeid={storeid}, month={month}", "DEBUG")
        stores_raw = doc.get("Stores", [])
        stores_json = json.dumps(stores_raw, default=json_serial)
        payload = json.dumps(doc, default=json_serial)
        try:
            cursor.execute("DELETE FROM scan_gb_price_promotion_store WHERE storeid = %s AND month = %s", (storeid, month))
            cursor.execute("DELETE FROM scan_gb_price_promotion WHERE storeid = %s AND month = %s", (storeid, month))
            cursor.execute(
                "INSERT INTO scan_gb_price_promotion (storeid, month, Stores, payload) VALUES (%s, %s, %s, %s)",
                (storeid, month, stores_json, payload),
            )
            promo_id = cursor.lastrowid
            for i, store_item in enumerate(stores_raw or []):
                store_data = json.dumps(store_item, default=json_serial) if isinstance(store_item, dict) else str(store_item)
                cursor.execute(
                    """INSERT INTO scan_gb_price_promotion_store (promotion_parent_id, storeid, month, store_data, sort_order) VALUES (%s, %s, %s, %s, %s)""",
                    (promo_id, storeid, month, store_data, i),
                )
            log(f"sync_gb_price_promotion: Row {row_num} - INSERT/UPDATE OK for storeid={storeid} month={month}, {len(stores_raw or [])} stores", "DEBUG")
        except Exception as e:
            log(f"sync_gb_price_promotion: ERROR on row {row_num} storeid={storeid}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_gb_price_promotion: Committed {row_num} rows, skipped {skipped}", "DEBUG")
    cursor.close()
    log(f"  gb_price_promotion: synced {row_num} rows")


def sync_gbpricebook(mongo_db, conn):
    log("=== sync_gbpricebook: START ===", "DEBUG")
    coll = mongo_db["gbpricebook"]
    doc_count = coll.count_documents({})
    log(f"sync_gbpricebook: MongoDB collection 'gbpricebook' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    for doc in coll.find():
        row_num += 1
        storeid = doc.get("storeid")
        if storeid is not None:
            storeid = str(storeid) if isinstance(storeid, ObjectId) else storeid
        poscode = doc.get("POSCode")
        log(f"sync_gbpricebook: Processing row {row_num}/{doc_count} - storeid={storeid}, POSCode={poscode}", "DEBUG")
        try:
            cursor.execute(
                """
                INSERT INTO scan_gbpricebook (storeid, POSCode, MerchandiseCode, RegularSellPrice, Description, payload)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    storeid,
                    doc.get("POSCode"),
                    doc.get("MerchandiseCode"),
                    doc.get("RegularSellPrice"),
                    doc.get("Description"),
                    json.dumps(doc, default=json_serial),
                ),
            )
            log(f"sync_gbpricebook: Row {row_num} - INSERT OK for POSCode={poscode}", "DEBUG")
        except Exception as e:
            log(f"sync_gbpricebook: ERROR on row {row_num} POSCode={poscode}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_gbpricebook: Committed {row_num} rows", "DEBUG")
    cursor.close()
    log(f"  gbpricebook: synced {row_num} rows")


def sync_rjr_promo(mongo_db, conn):
    log("=== sync_rjr_promo: START ===", "DEBUG")
    try:
        coll = mongo_db["rjr_promo"]
        log("sync_rjr_promo: MongoDB collection 'rjr_promo' found", "DEBUG")
    except Exception as e:
        log(f"sync_rjr_promo: Collection 'rjr_promo' not found or error: {e} - SKIPPING", "WARNING")
        return

    doc_count = coll.count_documents({})
    log(f"sync_rjr_promo: MongoDB collection 'rjr_promo' has {doc_count} documents", "DEBUG")

    cursor = conn.cursor()
    row_num = 0
    skipped = 0
    for doc in coll.find():
        storeid_val = doc.get("storeid")
        if not storeid_val:
            skipped += 1
            log(f"sync_rjr_promo: Skipping doc (no storeid) - skipped count={skipped}", "DEBUG")
            continue
        row_num += 1
        log(f"sync_rjr_promo: Processing row {row_num} - storeid={storeid_val}", "DEBUG")
        payload = json.dumps(doc, default=json_serial)
        try:
            cursor.execute(
                """INSERT INTO scan_rjr_promo (storeid, payload) VALUES (%s, %s)
                   ON DUPLICATE KEY UPDATE payload = VALUES(payload)""",
                (storeid_val, payload),
            )
            log(f"sync_rjr_promo: Row {row_num} - INSERT/UPDATE OK for storeid={storeid_val}", "DEBUG")
        except Exception as e:
            log(f"sync_rjr_promo: ERROR on row {row_num} storeid={storeid_val}: {e}", "ERROR")
            raise

    conn.commit()
    log(f"sync_rjr_promo: Committed {row_num} rows, skipped {skipped}", "DEBUG")
    cursor.close()
    log(f"  rjr_promo: synced {row_num} rows")


def main():
    log("=" * 60)
    log("mongo_to_mysql_sync.py - START")
    log(f"Log file: {LOG_FILENAME}")
    log("=" * 60)

    log("Connecting to MongoDB...")
    log(f"MongoDB URI: {MONGO_URI[:50]}... (redacted)")
    log(f"MongoDB database: {MONGO_DB}")
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        log("MongoDB connection: SUCCESS")
        log(f"MongoDB server info: {mongo_client.server_info().get('version', 'unknown')}", "DEBUG")
    except Exception as e:
        log(f"MongoDB connection FAILED: {e}", "ERROR")
        raise

    log("Connecting to MySQL...")
    try:
        conn = get_mysql_connection()
        if not conn:
            log("MySQL connection FAILED: get_mysql_connection() returned None", "ERROR")
            return
        log("MySQL connection: SUCCESS")
    except Exception as e:
        log(f"MySQL connection FAILED: {e}", "ERROR")
        raise

    log("Syncing reference data MongoDB -> MySQL...")
    sync_order = [
        "sync_stores",
        "sync_scan_data_dept",
        "sync_brand",
        "sync_upc",
        "sync_gb_department",
        "sync_gbtax",
        "sync_gb_price_promotion",
        "sync_gbpricebook",
        "sync_rjr_promo",
    ]
    for i, sync_name in enumerate(sync_order, 1):
        log(f"--- Step {i}/{len(sync_order)}: {sync_name} ---", "DEBUG")
        try:
            if sync_name == "sync_stores":
                sync_stores(mongo_db, conn)
            elif sync_name == "sync_scan_data_dept":
                sync_scan_data_dept(mongo_db, conn)
            elif sync_name == "sync_brand":
                sync_brand(mongo_db, conn)
            elif sync_name == "sync_upc":
                sync_upc(mongo_db, conn)
            elif sync_name == "sync_gb_department":
                sync_gb_department(mongo_db, conn)
            elif sync_name == "sync_gbtax":
                sync_gbtax(mongo_db, conn)
            elif sync_name == "sync_gb_price_promotion":
                sync_gb_price_promotion(mongo_db, conn)
            elif sync_name == "sync_gbpricebook":
                sync_gbpricebook(mongo_db, conn)
            elif sync_name == "sync_rjr_promo":
                try:
                    sync_rjr_promo(mongo_db, conn)
                except Exception as e:
                    log(f"  rjr_promo: skip ({e})", "WARNING")
            log(f"--- Step {i} complete: {sync_name} ---", "DEBUG")
        except Exception as e:
            log(f"FATAL: {sync_name} failed: {e}", "ERROR")
            raise

    conn.close()
    log("MySQL connection closed")
    mongo_client.close()
    log("MongoDB connection closed")

    log("=" * 60)
    log("Sync complete. You can now run Scan_Data_Collection scripts with MySQL only.")
    log(f"Full log saved to: {LOG_FILENAME}")
    log("=" * 60)


if __name__ == "__main__":
    main()
