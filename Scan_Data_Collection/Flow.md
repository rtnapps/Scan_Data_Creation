PART 1
-----------------------
1. create_scan_tables_mysql.sql - run to populate SQL Tables
2. create_scan_tables_normalized.sql - run to populate normailzed SQL Tables (optional)
3. mongo_to_mysql_sync.py - migiration from Mongo DB to SQL DB

PART 2
-----------------------
0. db_config.py - this is the python dependency_file
1. shiftreps.py - updates **scan_msm, scan_mcm, scan_ism, scan_tlm, scan_tpm, scan_cpjr, scan_connector_logs, scan_connector_status**
2. day_close.py - updates **scan_merge, scan_deptmerge, scan_connector_logs, scan_connector_status**
3. weekly.py - updates **scan_weekly**
4. autoscan.py - updates **scan_data (brand Altria)**
