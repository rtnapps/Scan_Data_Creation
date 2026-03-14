# gilbarco_counts – MySQL version
# Reads scan_merge, scan_msm, scan_mcm, scan_ism, scan_cpjr, scan_deptmerge from MySQL.
# Logs to scan_connector_logs with function_name = 'Gilbarco_Data_Checking'.

from datetime import datetime, timedelta

from flask import jsonify, Blueprint

from db_config import get_mysql_connection

gilbarco_counts_bp = Blueprint("gilbarco_counts_bp", __name__)

# Table names: (table, store_column, order_column, fields for response)
COLLECTION_CONFIG = {
    "msm": ("scan_msm", "StoreLocationID", "EndDate", ["id", "StoreLocationID", "BeginDate", "EndDate"]),
    "mcm": ("scan_mcm", "storeid", "EndDate", ["id", "corpid", "storeid", "BeginDate", "EndDate"]),
    "ism": ("scan_ism", "StoreLocationID", "BeginDate", ["id", "StoreLocationID", "BeginDate", "BeginTime"]),
    "cpjr": ("scan_cpjr", "storeid", "EndDate", ["id", "corpid", "storeid", "BeginDate", "EndDate"]),
    "merge": ("scan_merge", "storeid", "EndDate", ["id", "corpid", "storeid", "BeginDate", "EndDate"]),
    "deptmerge": ("scan_deptmerge", "storeid", "EndDate", ["id", "corpid", "storeid", "BeginDate", "EndDate"]),
}

LOG_FUNCTION_NAME = "Gilbarco_Data_Checking"
CORPID_DEFAULT = "65361d1bc436047c00231e45"


def get_previous_sunday():
    today = datetime.now()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    return last_sunday.replace(hour=0, minute=0, second=0, microsecond=0)


def _format_row(row, keys, storeid_key):
    out = {}
    for k in keys:
        v = row.get(k)
        if k == "id":
            out["_id"] = str(v) if v is not None else None
        elif k == storeid_key and "storeid" not in out:
            out["storeid"] = v
        else:
            out[k] = v.isoformat() if isinstance(v, (datetime,)) and hasattr(v, "isoformat") else v
    if "storeid" not in out and storeid_key != "storeid":
        out["storeid"] = row.get(storeid_key)
    if "EndDate" not in out and "EndDate" in row:
        out["EndDate"] = row["EndDate"].isoformat() if hasattr(row.get("EndDate"), "isoformat") else row.get("EndDate")
    return out


@gilbarco_counts_bp.route("/dailycounts/<storeid>", methods=["GET"])
def dailycounts(storeid):
    all_data = {}
    log_id = None
    status = "Failed"
    message = "Default failure"
    end_date = None
    previous_sunday = get_previous_sunday().date()

    conn = get_mysql_connection()
    if not conn:
        return jsonify({"error": "MySQL connection failed"}), 500

    try:
        cur = conn.cursor(dictionary=True)
        log_id_val = f"{storeid}_{CORPID_DEFAULT}_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, storeid, CORPID_DEFAULT, LOG_FUNCTION_NAME, "Process started", "Initialized", now),
        )
        conn.commit()
        log_id = cur.lastrowid

        cur.execute(
            """UPDATE scan_connector_logs SET message = %s, status = %s WHERE id = %s""",
            ("Fetching data from collections", "Processing", log_id),
        )
        conn.commit()

        for coll_name, config in COLLECTION_CONFIG.items():
            table, id_col, order_col, fields = config
            where_end = "AND EndDate IS NOT NULL" if order_col == "EndDate" else ""
            cur.execute(
                f"""SELECT {", ".join(fields)} FROM {table}
                    WHERE {id_col} = %s {where_end}
                    ORDER BY {order_col} DESC LIMIT 2""",
                (storeid,),
            )
            rows = cur.fetchall()
            formatted_docs = [_format_row(r, fields, id_col) for r in rows]
            all_data[coll_name] = formatted_docs

        merge_latest = all_data.get("merge", [])
        last_doc = merge_latest[0] if merge_latest else None

        if last_doc and "EndDate" in last_doc:
            end_date_str = last_doc["EndDate"]
            try:
                if isinstance(end_date_str, str):
                    end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).date()
                else:
                    end_date = end_date_str
                if end_date == previous_sunday:
                    status = "Success"
                    message = "Last merge EndDate matches previous Sunday"
                else:
                    message = f"EndDate {end_date} does not match previous Sunday {previous_sunday}"
            except Exception as date_err:
                message = f"Invalid date format in EndDate: {end_date_str}"
        else:
            message = "No valid EndDate found in merge collection"

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s, log_entry_at = %s WHERE id = %s""",
            (
                "Completed" if status == "Success" else "Failed",
                message + (f" | actual_enddate={end_date} expected_sunday={previous_sunday}" if end_date is not None else ""),
                now,
                log_id,
            ),
        )
        conn.commit()
    except Exception as e:
        status = "Failed"
        message = str(e)
        if log_id:
            try:
                cur = conn.cursor()
                cur.execute(
                    """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
                    ("Failed", str(e), log_id),
                )
                conn.commit()
            except Exception:
                pass
        else:
            try:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        f"{storeid}_{CORPID_DEFAULT}_log",
                        storeid,
                        CORPID_DEFAULT,
                        LOG_FUNCTION_NAME,
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

    return jsonify(all_data)
