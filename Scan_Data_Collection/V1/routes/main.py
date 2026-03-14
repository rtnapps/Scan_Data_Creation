# main – MySQL only (no MongoDB)
from flask import Flask, request, Blueprint, jsonify
from werkzeug.exceptions import HTTPException
from datetime import datetime
from pydantic import BaseModel
import csv, io, json, os, boto3
from io import StringIO
import paramiko
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import requests
from azure.storage.blob import BlobServiceClient
import os
from io import StringIO, BytesIO
from dotenv import load_dotenv

load_dotenv()  # Load .env file locally

main_bp = Blueprint('main_bp', __name__)

from db_config import get_mysql_connection

class Item(BaseModel):
    storeid: str
    weekendingdate: str
    brand: str
    FTP_Server: str
    FTP_User: str
    FTP_Password: str


# Altria scan file column order per AGDC Scan Reporting Requirements (pipe-delimited)
ALTRIA_CSV_FIELDS = [
    "Retail Control Number",
    "WeekEndingDate",
    "TransactionDate",
    "TransactionTime",
    "TransactionID",
    "Store Number",
    "Store Name",
    "Store Address",
    "Store City",
    "Store State",
    "Store Zip + 4 Code",
    "Category",
    "Manufacturer Name",
    "SKU Code",
    "UPC Code",
    "UPC Description",
    "Unit of Measure",
    "Quantity Sold",
    "Consumer Units",
    "Multi-Pack Indicator",
    "Multi-Pack Required Quantity",
    "Multi-Pack Discount Amount",
    "Retailer-Funded Discount Name",
    "Retailer-Funded Discount Amount",
    "MFG Deal Name ONE",
    "MFG Deal Discount Amount ONE",
    "MFG Deal Name TWO",
    "MFG Deal Discount Amount TWO",
    "MFG Deal Name THREE",
    "MFG Deal Discount Amount THREE",
    "Final Sales Price",
    "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10", "R11", "R12", "R13", "R14",
]

# Circana scan file column order (pipe-delimited, no header) – must match expected Circana layout
CIRCANA_CSV_FIELDS = [
    "Outlet Name",
    "Outlet Number",
    "Address 1",
    "Address 2",
    "City",
    "State",
    "Zip",
    "Transaction Date/Time",
    "Market Basket ID",
    "Scan ID",
    "Register ID",
    "Quantity",
    "Price",
    "UPC Code",
    "UPC Description",
    "Unit of Measure",
    "Promo Flag",
    "Outlet MultiPack Flag",
    "Outlet MultiPack Quantity",
    "Outlet MultiPack Disc Amount",
    "Acct Promo Name",
    "Acct Disc Amount",
    "Manufacturer Disc Amount",
    "PID Coupon",
    "PID Coupon Amt",
    "Mfg MultiPack Flag",
    "Mfg MultiPack Quantity",
    "Mfg MultiPack Discount Amount",
    "Mfg Promo Desc",
    "Mfg BuyDown Desc",
    "Mfg BuyDown Amt",
    "Mfg MultiPack Desc",
    "Account Loyalty ID",
    "Coupon Description",
]

def send_email(subject, body, recipients, attachment=None, attachment_name=None):
    ses_client = boto3.client(
        'ses',
        region_name='us-east-1',
        aws_access_key_id='AKIAQ3EGQE7L2WKUPBGP',
        aws_secret_access_key='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'
    )

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = "RTN Smart <support@rtnsmart.com>"
    msg['To'] = ', '.join(recipients['ToAddresses'])
    msg['Bcc'] = ', '.join(recipients['BccAddresses'])

    msg.attach(MIMEText(body, 'plain'))

    if attachment:
        part = MIMEApplication(attachment)
        part.add_header('Content-Disposition', 'attachment', filename=attachment_name)
        msg.attach(part)

    response = ses_client.send_raw_email(
        Source=msg['From'],
        Destinations=recipients['ToAddresses'] + recipients['BccAddresses'],
        RawMessage={
            'Data': msg.as_string()
        }
    )

    return response

@main_bp.route("/home", methods=["GET"])
def root():
    return {"message": "Welcome to Scan Data Submission"}
    
@main_bp.route("/altria", methods=["POST"])
def altria():
    log_id = None
    item = request.json or {}
    storeid = item.get("storeid")
    weekendingdate = item.get("weekendingdate")

    if not get_mysql_connection:
        return jsonify({"message": "MySQL db_config not available."}), 500

    conn = get_mysql_connection()
    if not conn:
        return jsonify({"message": "MySQL connection failed."}), 500

    try:
        cur = conn.cursor(dictionary=True)

        # Require scan_data row for this store / week / Altria
        cur.execute(
            """SELECT id, data, week_ending_date FROM scan_data
               WHERE storeid = %s AND week_ending_date = %s AND brand = 'Altria' LIMIT 1""",
            (storeid, weekendingdate),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({
                "message": "Altria scan data not found for this store and week. Run Gilbarco Altria Scan first."
            }), 404

        scan_data_id = row["id"]
        data_list = row["data"]
        if isinstance(data_list, str):
            import json as _json
            data_list = _json.loads(data_list)
        if not data_list or not isinstance(data_list, list):
            conn.close()
            return jsonify({"message": "Scan data has no rows."}), 404

        # Log: Altria_Scan_File_Upload started
        log_id_val = f"{storeid}_altria_upload_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, storeid, "65361d1bc436047c00231e45", "Altria_Scan_File_Upload", "Process started", "Initialized", now),
        )
        conn.commit()
        log_id = cur.lastrowid

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Processing", "Querying and preparing file for upload", log_id),
        )
        conn.commit()

        sftp_server = item.get("FTP_Server")
        sftp_user = item.get("FTP_User")
        sftp_password = item.get("FTP_Password")

        # Store details from MySQL
        cur.execute("SELECT store_name, owner_gmail FROM scan_stores WHERE id = %s", (storeid,))
        store_details = cur.fetchone()
        if not store_details:
            conn.close()
            raise HTTPException(status_code=404, detail="Store details not found")

        store_title = (store_details.get("store_name") or "").replace(" ", "")
        week_end_date = datetime.strptime(str(weekendingdate), "%Y-%m-%d").strftime("%Y%m%d")

        recipients = {
            "ToAddresses": [store_details.get("owner_gmail") or "karkavelraja.j@ptuniv.edu.in"],
            "BccAddresses": ["ajith@realtnetworking.com", "maitry82@gmail.com"],
        }

        sum_quantity_sold = sum(int(d.get("Quantity Sold") or 0) for d in data_list if d.get("Quantity Sold") not in (None, ""))
        sum_fsp = "{:.2f}".format(sum(float(d.get("Final Sales Price") or 0) for d in data_list if d.get("Final Sales Price") not in (None, "")))
        num_transactions = len(data_list)

        def _altria_cell(key, val):
            if val is None or (isinstance(val, float) and str(val) == "nan") or str(val).strip() == "nan":
                return ""
            s = str(val).strip()
            if key == "WeekEndingDate" and s:
                return s.replace("-", "")[:8]
            if key == "TransactionDate" and s:
                return s.replace("-", "")[:8]
            return s

        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=ALTRIA_CSV_FIELDS, delimiter="|", extrasaction="ignore")
        for d in data_list:
            cleaned_item = {k: _altria_cell(k, d.get(k)) for k in ALTRIA_CSV_FIELDS}
            writer.writerow(cleaned_item)

        csv_buffer.seek(0, 0)
        csv_content = csv_buffer.getvalue()
        csv_content = f"{num_transactions}|{sum_quantity_sold}|{sum_fsp}|{store_title}\n" + csv_content

        key = f"{store_title}{week_end_date}.txt"

        # Save to local file
        with open(key, "w", encoding="utf-8") as file:
            file.write(csv_content)

        azure_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = "scandata-files"
        blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=key)
        blob_client.upload_blob(csv_content, overwrite=True)

        file_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{key}"

        download_stream = BytesIO()
        blob_data = blob_client.download_blob()
        blob_data.readinto(download_stream)
        download_stream.seek(0)

        remote_filename = f"/incoming/{store_title}{week_end_date}.txt"
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_server, username=sftp_user, password=sftp_password)
        sftp = ssh.open_sftp()
        try:
            sftp.putfo(download_stream, remote_filename)
        finally:
            sftp.close()
            ssh.close()

        cur.execute(
            """UPDATE scan_data SET txtURL = %s, status = 1 WHERE id = %s""",
            (file_url, scan_data_id),
        )
        conn.commit()

        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Completed", "File uploaded to Azure and SFTP successfully", log_id),
        )
        conn.commit()
        conn.close()

        send_email(
            subject="Scan Data File Upload Successful",
            body=f"Scan Data File of {store_title} for Altria Week Ending {weekendingdate} is Successful",
            recipients=recipients,
        )
        return {"message": f"File {key} uploaded successfully to {sftp_server} and URL updated in MySQL."}

    except HTTPException:
        if conn:
            conn.close()
        raise
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
                    (f"{storeid}_altria_upload_log", storeid, "65361d1bc436047c00231e45", "Altria_Scan_File_Upload", str(e), "Failed", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

        try:
            conn2 = get_mysql_connection()
            if conn2:
                cur2 = conn2.cursor(dictionary=True)
                cur2.execute("SELECT owner_gmail FROM scan_stores WHERE id = %s", (storeid,))
                store_details = cur2.fetchone()
                conn2.close()
                to_addr = (store_details or {}).get("owner_gmail") or "karkavelraja.j@ptuniv.edu.in"
            else:
                to_addr = "karkavelraja.j@ptuniv.edu.in"
        except Exception:
            to_addr = "karkavelraja.j@ptuniv.edu.in"

        send_email(
            subject="Scan Data File Upload Failed",
            body=f"Scan Data File for Altria Week Ending {weekendingdate} is Unsuccessful. Error: {str(e)}",
            recipients={"ToAddresses": [to_addr], "BccAddresses": ["ajith@realtnetworking.com", "maitry82@gmail.com"]},
        )
        return jsonify({"message": "Altria upload failed", "detail": str(e)}), 400
        
@main_bp.route("/itg", methods=["POST"])  # ITG/RJR – MySQL only
def itg():
    log_id = None
    item = request.json or {}
    storeid = item.get("storeid")
    weekendingdate = item.get("weekendingdate")

    if not get_mysql_connection:
        return jsonify({"message": "MySQL db_config not available."}), 500
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"message": "MySQL connection failed."}), 500

    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """SELECT id, data FROM scan_data
               WHERE storeid = %s AND week_ending_date = %s AND brand = 'ITG/RJR' LIMIT 1""",
            (storeid, weekendingdate),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"message": "ITG/RJR scan data not found for this store and week."}), 404

        scan_data_id = row["id"]
        data_list = row["data"]
        if isinstance(data_list, str):
            data_list = json.loads(data_list)
        if not data_list or not isinstance(data_list, list):
            conn.close()
            return jsonify({"message": "Scan data has no rows."}), 404

        log_id_val = f"{storeid}_itg_upload_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, storeid, "65361d1bc436047c00231e45", "ITG_Scan_File_Upload", "Process started", "Initialized", now),
        )
        conn.commit()
        log_id = cur.lastrowid
        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Processing", "Querying and preparing file for upload", log_id),
        )
        conn.commit()

        sftp_server = item.get("FTP_Server")
        sftp_user = item.get("FTP_User")
        sftp_password = item.get("FTP_Password")

        cur.execute("SELECT store_name, owner_gmail FROM scan_stores WHERE id = %s", (storeid,))
        store_details = cur.fetchone()
        if not store_details:
            conn.close()
            return jsonify({"message": "Store details not found."}), 404

        store_title = (store_details.get("store_name") or "").replace(" ", "")
        if not store_title and data_list and isinstance(data_list[0], dict):
            store_title = (data_list[0].get("Outlet Name") or "").replace(" ", "")
        week_end_date = datetime.strptime(str(weekendingdate), "%Y-%m-%d").strftime("%m%d%Y")

        recipients = {
            "ToAddresses": [store_details.get("owner_gmail") or "karkavelraja.j@ptuniv.edu.in"],
            "BccAddresses": ["ajith@realtnetworking.com", "maitry82@gmail.com"],
        }

        filename = f"{store_title}_{week_end_date}.json"
        json_data = json.dumps(data_list)
        
        # save the json data to a file
        with open(filename, "w", encoding="utf-8") as f:
            f.write(json_data)

        azure_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = "scandata-files"
        blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        blob_client.upload_blob(json_data, overwrite=True)
        file_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{filename}"

        download_stream = BytesIO()
        blob_data = blob_client.download_blob()
        blob_data.readinto(download_stream)
        download_stream.seek(0)

        remote_filename = f"/{sftp_user}_live/incoming/{filename}"
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_server, username=sftp_user, password=sftp_password)
        sftp = ssh.open_sftp()
        try:
            sftp.putfo(download_stream, remote_filename)
        finally:
            sftp.close()
            ssh.close()

        cur.execute("""UPDATE scan_data SET txtURL = %s, status = 1 WHERE id = %s""", (file_url, scan_data_id))
        conn.commit()
        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Completed", "File uploaded to Azure and SFTP successfully", log_id),
        )
        conn.commit()
        conn.close()

        send_email(
            subject="Scan Data File Upload Successful",
            body=f"Scan Data File of {store_title} for MSA Week Ending {weekendingdate} is Successful",
            recipients=recipients,
        )
        return {"message": f"File {filename} uploaded successfully to {sftp_server}."}

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
                    (f"{storeid}_itg_upload_log", storeid, "65361d1bc436047c00231e45", "ITG_Scan_File_Upload", str(e), "Failed", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

        to_addr = "karkavelraja.j@ptuniv.edu.in"
        try:
            c2 = get_mysql_connection()
            if c2:
                cur2 = c2.cursor(dictionary=True)
                cur2.execute("SELECT owner_gmail FROM scan_stores WHERE id = %s", (storeid,))
                r = cur2.fetchone()
                c2.close()
                if r and r.get("owner_gmail"):
                    to_addr = r["owner_gmail"]
        except Exception:
            pass

        send_email(
            subject="Scan Data File Upload Failed",
            body=f"Scan Data File for MSA Week Ending {weekendingdate} is Unsuccessful. Error: {str(e)}",
            recipients={"ToAddresses": [to_addr], "BccAddresses": ["ajith@realtnetworking.com", "maitry82@gmail.com"]},
        )
        return jsonify({"message": "ITG upload failed", "detail": str(e)}), 400

@main_bp.route("/circana_test", methods=["POST"])  # Circana Test – MySQL only
def circana_test():
    return _circana_upload(item=request.json, remote_subdir="Onboarding_Validation", log_program="Circana_Test_Scan_File_Upload")


@main_bp.route("/circana", methods=["POST"])  # Circana Production – MySQL only
def circana():
    return _circana_upload(item=request.json, remote_subdir="Production", log_program="Circana_Scan_File_Upload")


def _circana_upload(item, remote_subdir, log_program):
    log_id = None
    item = item or {}
    storeid = item.get("storeid")
    weekendingdate = item.get("weekendingdate")

    if not get_mysql_connection:
        return jsonify({"message": "MySQL db_config not available."}), 500
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"message": "MySQL connection failed."}), 500

    store_title = ""
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """SELECT id, data FROM scan_data
               WHERE storeid = %s AND week_ending_date = %s AND brand = 'Circana' LIMIT 1""",
            (storeid, weekendingdate),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({
                "message": "Circana scan data not found for this store and week. Run Gilbarco Circana Scan first."
            }), 404

        scan_data_id = row["id"]
        data_list = row["data"]
        if isinstance(data_list, str):
            data_list = json.loads(data_list)
        if not data_list or not isinstance(data_list, list):
            conn.close()
            return jsonify({"message": "Scan data has no rows."}), 404

        log_id_val = f"{storeid}_circana_upload_log"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """INSERT INTO scan_connector_logs (log_id, storeid, corpid, function_name, message, status, log_entry_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (log_id_val, storeid, "65361d1bc436047c00231e45", log_program, "Process started", "Initialized", now),
        )
        conn.commit()
        log_id = cur.lastrowid
        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Processing", "Querying and preparing file for upload", log_id),
        )
        conn.commit()

        sftp_server = item.get("FTP_Server")
        sftp_user = item.get("FTP_User")
        sftp_password = item.get("FTP_Password")

        cur.execute("SELECT store_name, owner_gmail, circana_submitterid FROM scan_stores WHERE id = %s", (storeid,))
        store_details = cur.fetchone()
        if not store_details:
            conn.close()
            return jsonify({"message": "Store details not found."}), 404

        store_title = (store_details.get("store_name") or "").replace(" ", "")
        if not store_title and data_list and isinstance(data_list[0], dict):
            store_title = (data_list[0].get("Outlet Name") or "").replace(" ", "")
        circana_submitterid = store_details.get("circana_submitterid") or "unknown"
        week_end_date = datetime.strptime(str(weekendingdate), "%Y-%m-%d").strftime("%Y%m%d")
        week_end_time = datetime.now().strftime("%H%M")

        recipients = {
            "ToAddresses": [store_details.get("owner_gmail") or "karkavelraja.j@ptuniv.edu.in"],
            "BccAddresses": ["ajith@realtnetworking.com", "maitry82@gmail.com"],
        }

        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=CIRCANA_CSV_FIELDS, delimiter="|", extrasaction="ignore")
        for d in data_list:
            cleaned = {}
            for k in CIRCANA_CSV_FIELDS:
                v = d.get(k)
                cleaned[k] = "" if v is None or str(v) == "nan" else v
            writer.writerow(cleaned)
        csv_buffer.seek(0, 0)
        csv_content = csv_buffer.getvalue()

        key = f"{circana_submitterid}_{week_end_date}_{week_end_time}_{store_title}.txt"
        with open(key, "w", encoding="utf-8") as f:
            f.write(csv_content)

        azure_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = "scandata-files"
        blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=key)
        blob_client.upload_blob(csv_content, overwrite=True)
        file_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{key}"

        download_stream = BytesIO()
        blob_data = blob_client.download_blob()
        blob_data.readinto(download_stream)
        download_stream.seek(0)

        remote_filename = f"/{remote_subdir}/{key}"
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_server, username=sftp_user, password=sftp_password)
        sftp = ssh.open_sftp()
        try:
            sftp.putfo(download_stream, remote_filename)
        finally:
            sftp.close()
            ssh.close()

        cur.execute("""UPDATE scan_data SET txtURL = %s, status = 1 WHERE id = %s""", (file_url, scan_data_id))
        conn.commit()
        cur.execute(
            """UPDATE scan_connector_logs SET status = %s, message = %s WHERE id = %s""",
            ("Completed", "File uploaded to Azure and SFTP successfully", log_id),
        )
        conn.commit()
        conn.close()

        send_email(
            subject="Scan Data File Upload Successful",
            body=f"Scan Data File of {store_title} for Circana Week Ending {weekendingdate} is Successful",
            recipients=recipients,
        )
        return {"message": f"File {key} uploaded successfully to {sftp_server} and URL updated in MySQL."}

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
                    (f"{storeid}_circana_upload_log", storeid, "65361d1bc436047c00231e45", log_program, str(e), "Failed", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

        to_addr = "karkavelraja.j@ptuniv.edu.in"
        try:
            c2 = get_mysql_connection()
            if c2:
                cur2 = c2.cursor(dictionary=True)
                cur2.execute("SELECT owner_gmail FROM scan_stores WHERE id = %s", (storeid,))
                r = cur2.fetchone()
                c2.close()
                if r and r.get("owner_gmail"):
                    to_addr = r["owner_gmail"]
        except Exception:
            pass

        send_email(
            subject="Scan Data File Upload Failed",
            body=f"Scan Data File for Circana Week Ending {weekendingdate} is Unsuccessful. Error: {str(e)}",
            recipients={"ToAddresses": [to_addr], "BccAddresses": ["ajith@realtnetworking.com", "maitry82@gmail.com"]},
        )
        return jsonify({"message": "Circana upload failed", "detail": str(e)}), 400