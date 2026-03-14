**db_config.py** - Azure MYSQL DB Credentials 

**env.example**  - Configuration Data

| Order | Python file         | Input (reads from)                                                                             | Output (writes to)                                                                                                                                                                         |
| ----- | ------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1     | **shiftreps_v1.py**    | XML (`X:\BOOutBox`), scan_data_dept, scan_gbtax, scan_gb_department                            | scan_msm, scan_mcm, scan_ism, scan_tlm, scan_tpm, scan_cpjr, scan_connector_logs, scan_connector_status                                                                                    |
| 2     | **day_close_v1.py**    | scan_cpjr, scan_mcm                                                                            | scan_merge, scan_deptmerge, scan_connector_logs, scan_connector_status                                                                                                                     |

**app.py** - to run the files inside the routes folder

**StopNSave20260307.txt** - Altria Sample File

**StopNSave_03082026.json** - ITG Sample File

**298694_20260308_0004_StopNSave.txt** - Circana Sample File

Files inside the routes folder (with order of execution)
1. **gilbarco_counts.py** - update the count to **scan_connector_logs**
2. **gilbarco_weekly.py** - update to **scan_weekly**
3. **gilbarco_itg_weekly.py** - update to **scan_itg_weekly**
4. **gilbarco_altria_scan.py** - scan data for Altria updated to **scan_data**
5.  **gilbarco_itg_scan.py** - scan data for ITG updated to **scan_data**
6.  **gilbarco_circana_scan.py** - scan data for Circana updated to **scan_data**
7.  **main.py** - for mailing and submission of scan data
