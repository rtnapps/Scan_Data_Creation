# fetch_data – MySQL version
# POST /fetch_data with JSON: store_id, optional weekly_n, itg_weekly_n, scan_n.
# Returns latest weekly, itg_weekly, and scan_data rows from MySQL.

from flask import request, jsonify, Blueprint

from db_config import get_mysql_connection

fetch_data_bp = Blueprint("fetch_data_bp", __name__)


@fetch_data_bp.route("/fetch_data", methods=["POST"])
def fetch_data():
    try:
        payload = request.get_json() or {}
        store_id = payload.get("store_id")
        if not store_id:
            return jsonify({"error": "store_id is required"}), 400

        weekly_n = int(payload.get("weekly_n", 1))
        itg_weekly_n = int(payload.get("itg_weekly_n", 1))
        scan_n = int(payload.get("scan_n", 3))

        conn = get_mysql_connection()
        if not conn:
            return jsonify({"error": "MySQL connection failed"}), 500

        try:
            cur = conn.cursor(dictionary=True)

            # Weekly data (scan_weekly)
            cur.execute(
                """SELECT storeid, BeginDate, EndDate FROM scan_weekly
                   WHERE storeid = %s ORDER BY id DESC LIMIT %s""",
                (store_id, max(1, weekly_n)),
            )
            weekly_data = [
                {
                    "storeid": row.get("storeid"),
                    "BeginDate": row.get("BeginDate").isoformat() if row.get("BeginDate") else None,
                    "EndDate": row.get("EndDate").isoformat() if row.get("EndDate") else None,
                }
                for row in cur.fetchall()
            ]

            # ITG Weekly data (scan_itg_weekly)
            cur.execute(
                """SELECT storeid, BeginDate, EndDate FROM scan_itg_weekly
                   WHERE storeid = %s ORDER BY id DESC LIMIT %s""",
                (store_id, max(1, itg_weekly_n)),
            )
            itg_weekly_data = [
                {
                    "storeid": row.get("storeid"),
                    "BeginDate": row.get("BeginDate").isoformat() if row.get("BeginDate") else None,
                    "EndDate": row.get("EndDate").isoformat() if row.get("EndDate") else None,
                }
                for row in cur.fetchall()
            ]

            # Scan Data (scan_data, latest entries)
            cur.execute(
                """SELECT brand, storeid, week_ending_date, status, substatus, txtURL
                   FROM scan_data WHERE storeid = %s ORDER BY id DESC LIMIT %s""",
                (store_id, max(1, scan_n)),
            )
            scan_data_fd = [
                {
                    "brand": row.get("brand"),
                    "storeid": row.get("storeid"),
                    "week_ending_date": row.get("week_ending_date").isoformat() if hasattr(row.get("week_ending_date"), "isoformat") else row.get("week_ending_date"),
                    "status": row.get("status"),
                    "substatus": row.get("substatus"),
                    "txtURL": row.get("txtURL"),
                }
                for row in cur.fetchall()
            ]

            return jsonify({
                "weekly": weekly_data,
                "itg_weekly": itg_weekly_data,
                "scan_data": scan_data_fd,
            })
        finally:
            conn.close()

    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {e}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
