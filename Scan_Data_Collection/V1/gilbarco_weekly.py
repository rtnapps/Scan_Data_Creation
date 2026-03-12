# Gilbarco Weekly – MySQL version
# Checks scan_connector_logs for Gilbarco_Data_Checking = Completed, then reads scan_merge,
# aggregates SaleEvents, and inserts into scan_weekly.

import json
from datetime import datetime, timedelta

from flask import jsonify, Blueprint

from db_config import get_mysql_connection

gilbarco_weekly_bp = Blueprint("gilbarco_weekly_bp", __name__)

LOG_FUNCTION_CHECK = "Gilbarco_Data_Checking"
LOG_FUNCTION_SELF = "Gilbarco_Weekly"
CORPID_DEFAULT = "65361d1bc436047c00231e45"


@gilbarco_weekly_bp.route("/gilbarco_generate_weekly_sales/<store_id>", methods=["GET"])
def gilbarco_generate_weekly_sales(store_id):
    log_id = None
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"error": "MySQL connection failed"}), 500

    try:
        cur = conn.cursor(dictionary=True)

        # Latest "Gilbarco_Data_Checking" log entry
        cur.execute(
            """SELECT id, function_name, status FROM scan_connector_logs
               WHERE function_name = %s ORDER BY id DESC LIMIT 1""",
            (LOG_FUNCTION_CHECK,),
        )
        latest_check = cur.fetchone()

        if not latest_check or latest_check.get("status") != "Completed":
            log_id_val = f"{store_id}_{CORPID_DEFAULT}_log"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (log_id_val, store_id, CORPID_DEFAULT, LOG_FUNCTION_SELF, "Latest 'Gilbarco_Data_Checking' was not completed. Aborting weekly generation.", "Failed", now),
            )
            conn.commit()
            conn.close()
            return jsonify({"message": "Latest 'Gilbarco_Data_Checking' was not completed. Aborting weekly generation."}), 400

        today = datetime.now().date()
        start_date = today - timedelta(days=today.weekday() + 8)
        end_date = start_date + timedelta(days=6)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        log_id_val = f"{store_id}_{CORPID_DEFAULT}_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, store_id, CORPID_DEFAULT, LOG_FUNCTION_SELF, "API triggered", "Initialized", now),
        )
        conn.commit()
        log_id = cur.lastrowid

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Processing", "Fetching sales data from scan_merge", log_id),
        )
        conn.commit()

        cur.execute(
            """SELECT id, storeid, corpid, ReportSequenceNumbers, SaleEvents FROM scan_merge
               WHERE storeid = %s AND EndDate >= %s AND EndDate <= %s ORDER BY EndDate""",
            (store_id, start_date_str, end_date_str),
        )
        rows = cur.fetchall()

        def parse_json(val):
            if val is None:
                return [] if val is None else val
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except json.JSONDecodeError:
                    return []
            return val if isinstance(val, list) else []

        all_sale_events = []
        report_sequence_numbers = []
        corp_id = None

        for row in rows:
            sale_events = parse_json(row.get("SaleEvents"))
            all_sale_events.extend(sale_events)
            rsn = parse_json(row.get("ReportSequenceNumbers"))
            report_sequence_numbers.append(rsn)
            if corp_id is None:
                corp_id = row.get("corpid") or CORPID_DEFAULT

        if not all_sale_events:
            cur.execute(
                """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
                ("Failed", "No sales data found", log_id),
            )
            conn.commit()
            conn.close()
            return jsonify({"message": "No sales data found for the given store and week."}), 404

        sale_event_json = json.dumps(all_sale_events)
        report_seq_json = json.dumps(report_sequence_numbers)

        cur.execute(
            """INSERT INTO scan_weekly (storeid, corpid, BeginDate, EndDate, ReportSequenceNumbers, SaleEvent)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (store_id, corp_id, start_date_str, end_date_str, report_seq_json, sale_event_json),
        )
        conn.commit()

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Completed", "Weekly sales data inserted successfully", log_id),
        )
        conn.commit()

        conn.close()
        return jsonify({
            "message": "Gilbarco Weekly sales data generated and inserted successfully.",
            "storeid": store_id,
            "begin_date": start_date_str,
            "end_date": end_date_str,
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
                    (f"{store_id}_{CORPID_DEFAULT}_log", store_id, CORPID_DEFAULT, LOG_FUNCTION_SELF, str(e), "Failed", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()
        return jsonify({"error": str(e)}), 500
