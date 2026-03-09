-- ============================================================
-- Scan_Data_Collection → MySQL (same DB: live_rtn_stopnsave)
-- Run this script in Azure MySQL after connecting to the database.
-- ============================================================

USE live_rtn_stopnsave;

-- 1. MERGE (from day_close.py)
CREATE TABLE IF NOT EXISTS scan_merge (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    StoreLocationID VARCHAR(64) DEFAULT NULL,
    BeginDate DATE DEFAULT NULL,
    EndDate DATE NOT NULL,
    ReportSequenceNumbers JSON DEFAULT NULL,
    SaleEvents JSON DEFAULT NULL,
    VoidEvents JSON DEFAULT NULL,
    RefundEvents JSON DEFAULT NULL,
    OtherEvents JSON DEFAULT NULL,
    BeginTimes JSON DEFAULT NULL,
    EndTimes JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_merge_store_end (storeid, EndDate)
);

-- 2. DEPTMERGE (from day_close.py)
CREATE TABLE IF NOT EXISTS scan_deptmerge (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    StoreLocationID VARCHAR(64) DEFAULT NULL,
    BeginDate DATE DEFAULT NULL,
    BeginTime VARCHAR(32) DEFAULT NULL,
    EndDate DATE NOT NULL,
    EndTime VARCHAR(32) DEFAULT NULL,
    MCMDetail JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_deptmerge_store_end (storeid, EndDate)
);

-- 3. WEEKLY (from weekly.py)
CREATE TABLE IF NOT EXISTS scan_weekly (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    BeginDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    ReportSequenceNumbers JSON DEFAULT NULL,
    SaleEvent JSON NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_weekly_store (storeid),
    KEY idx_weekly_dates (BeginDate, EndDate)
);

-- 4. SCAN_DATA (from autoscan.py, ITG_Scan.py, RJR_Scan.py)
CREATE TABLE IF NOT EXISTS scan_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    week_ending_date DATE NOT NULL,
    brand VARCHAR(32) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    submitternum VARCHAR(32) DEFAULT NULL,
    submitdate DATE DEFAULT NULL,
    storenum VARCHAR(16) DEFAULT NULL,
    transactions VARCHAR(32) DEFAULT NULL,
    status TINYINT DEFAULT 0,
    substatus VARCHAR(64) DEFAULT NULL,
    txtURL VARCHAR(512) DEFAULT NULL,
    data JSON NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scan_store_week_brand (storeid, week_ending_date, brand)
);

-- 5. LIVE_SUMMARY (from live_summary.py, live.py)
CREATE TABLE IF NOT EXISTS scan_live_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    EndDate DATE NOT NULL,
    StoreLocationID VARCHAR(64) DEFAULT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    salesSummary JSON DEFAULT NULL,
    salesSummaryTotals JSON DEFAULT NULL,
    fuelSummary JSON DEFAULT NULL,
    fuelSummaryTotals JSON DEFAULT NULL,
    voidSummary JSON DEFAULT NULL,
    voidSummaryTotals JSON DEFAULT NULL,
    refundSummary JSON DEFAULT NULL,
    refundSummaryTotals JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_live_summary_store_end (storeid, EndDate)
);

-- 6. GBSUMMARYALL (from allsummary.py)
CREATE TABLE IF NOT EXISTS scan_gbsummaryall (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    ReportSequenceNumber JSON DEFAULT NULL,
    BeginDate DATE DEFAULT NULL,
    BeginTime JSON DEFAULT NULL,
    EndDate DATE DEFAULT NULL,
    EndTime JSON DEFAULT NULL,
    salesSummary JSON DEFAULT NULL,
    salesSummaryTotals JSON DEFAULT NULL,
    fuelSummary JSON DEFAULT NULL,
    fuelSummaryTotals JSON DEFAULT NULL,
    voidSummary JSON DEFAULT NULL,
    voidSummaryTotals JSON DEFAULT NULL,
    refundSummary JSON DEFAULT NULL,
    refundSummaryTotals JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_gbsummary_store_end (storeid, EndDate)
);

-- 7. GBMOPSALES (from mop_totals.py)
CREATE TABLE IF NOT EXISTS scan_gbmopsales (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) DEFAULT NULL,
    TotalCash DECIMAL(14,2) DEFAULT NULL,
    TotalEBT DECIMAL(14,2) DEFAULT NULL,
    TotalCredit DECIMAL(14,2) DEFAULT NULL,
    TotalDebit DECIMAL(14,2) DEFAULT NULL,
    TotalCoupon DECIMAL(14,2) DEFAULT NULL,
    TotalMobile DECIMAL(14,2) DEFAULT NULL,
    CountCash INT DEFAULT NULL,
    CountEBT INT DEFAULT NULL,
    CountCredit INT DEFAULT NULL,
    CountDebit INT DEFAULT NULL,
    CountCoupon INT DEFAULT NULL,
    CountMobile INT DEFAULT NULL,
    TotalSalesAmount DECIMAL(14,2) DEFAULT NULL,
    TotalSalesCount INT DEFAULT NULL,
    TotalVoidAmount DECIMAL(14,2) DEFAULT NULL,
    TotalVoidCount INT DEFAULT NULL,
    TotalRefundAmount DECIMAL(14,2) DEFAULT NULL,
    TotalRefundCount INT DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 8. CONNECTOR_LOGS (from day_close.py, shiftreps.py, live.py)
CREATE TABLE IF NOT EXISTS scan_connector_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    log_id VARCHAR(128) NOT NULL,
    storeid VARCHAR(64) DEFAULT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    function_name VARCHAR(128) DEFAULT NULL,
    message TEXT,
    status VARCHAR(32) DEFAULT NULL,
    log_entry_at DATETIME DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_connector_logs_id (log_id)
);

-- 9. CONNECTOR_STATUS (from day_close.py, shiftreps.py, live.py)
CREATE TABLE IF NOT EXISTS scan_connector_status (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    Connector VARCHAR(128) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    status VARCHAR(64) DEFAULT NULL,
    timestamp VARCHAR(32) DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_connector_status (storeid, Connector)
);

-- ============================================================
-- 10+ Source/reference tables (no MongoDB reads)
-- ============================================================

-- CPJR (day_close reads; shiftreps writes from XML)
CREATE TABLE IF NOT EXISTS scan_cpjr (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    StoreLocationID VARCHAR(64) DEFAULT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    ReportSequenceNumber VARCHAR(64) DEFAULT NULL,
    BeginDate VARCHAR(32) DEFAULT NULL,
    BeginTime VARCHAR(32) DEFAULT NULL,
    EndDate VARCHAR(32) NOT NULL,
    EndTime VARCHAR(32) DEFAULT NULL,
    ReportSequenceNumbers JSON DEFAULT NULL,
    BeginTimes JSON DEFAULT NULL,
    EndTimes JSON DEFAULT NULL,
    SaleEvents JSON DEFAULT NULL,
    OtherEvents JSON DEFAULT NULL,
    VoidEvents JSON DEFAULT NULL,
    RefundEvents JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_cpjr_store_end (storeid, EndDate)
);

-- MCM (day_close reads; shiftreps writes from XML)
CREATE TABLE IF NOT EXISTS scan_mcm (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    StoreLocationID VARCHAR(64) DEFAULT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    BeginDate VARCHAR(32) DEFAULT NULL,
    BeginTime VARCHAR(32) DEFAULT NULL,
    EndDate VARCHAR(32) NOT NULL,
    EndTime VARCHAR(32) DEFAULT NULL,
    MCMDetail JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_mcm_store_end (storeid, EndDate)
);

CREATE TABLE IF NOT EXISTS scan_msm (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    StoreLocationID VARCHAR(64) NOT NULL,
    BeginDate VARCHAR(32) NOT NULL,
    BeginTime VARCHAR(32) NOT NULL,
    EndDate VARCHAR(32) DEFAULT NULL,
    EndTime VARCHAR(32) DEFAULT NULL,
    payload JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_msm (StoreLocationID, BeginDate, BeginTime)
);
CREATE TABLE IF NOT EXISTS scan_ism (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    StoreLocationID VARCHAR(64) NOT NULL,
    BeginDate VARCHAR(32) NOT NULL,
    BeginTime VARCHAR(32) NOT NULL,
    payload JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ism (StoreLocationID, BeginDate, BeginTime)
);
CREATE TABLE IF NOT EXISTS scan_tlm (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    StoreLocationID VARCHAR(64) NOT NULL,
    BeginDate VARCHAR(32) NOT NULL,
    BeginTime VARCHAR(32) NOT NULL,
    payload JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_tlm (StoreLocationID, BeginDate, BeginTime)
);
CREATE TABLE IF NOT EXISTS scan_tpm (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    StoreLocationID VARCHAR(64) NOT NULL,
    BeginDate VARCHAR(32) NOT NULL,
    BeginTime VARCHAR(32) NOT NULL,
    payload JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_tpm (StoreLocationID, BeginDate, BeginTime)
);

CREATE TABLE IF NOT EXISTS scan_stores (
    id VARCHAR(64) PRIMARY KEY,
    store_name VARCHAR(255) DEFAULT NULL,
    owner_gmail VARCHAR(255) DEFAULT NULL,
    retailer_acc_no VARCHAR(64) DEFAULT NULL,
    address TEXT,
    city VARCHAR(128) DEFAULT NULL,
    state VARCHAR(64) DEFAULT NULL,
    zip_code VARCHAR(32) DEFAULT NULL,
    drive VARCHAR(255) DEFAULT NULL,
    circana_submitterid VARCHAR(64) DEFAULT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scan_data_dept (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL UNIQUE,
    scanDept JSON NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scan_brand (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_brand_store (storeid)
);

CREATE TABLE IF NOT EXISTS scan_upc (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cycleCode VARCHAR(64) DEFAULT NULL,
    UPCCodes JSON DEFAULT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scan_gbpricebook (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) DEFAULT NULL,
    POSCode VARCHAR(64) DEFAULT NULL,
    MerchandiseCode VARCHAR(32) DEFAULT NULL,
    RegularSellPrice VARCHAR(32) DEFAULT NULL,
    Description TEXT,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_gbpb_store_pos (storeid, POSCode)
);

CREATE TABLE IF NOT EXISTS scan_gb_price_promotion (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    month VARCHAR(32) NOT NULL,
    Stores JSON DEFAULT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_gbpp_store_month (storeid, month)
);

CREATE TABLE IF NOT EXISTS scan_gb_department (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    departments JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_gbdept_store (storeid)
);

CREATE TABLE IF NOT EXISTS scan_gbtax (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    taxInfo JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_gbtax_store (storeid)
);

CREATE TABLE IF NOT EXISTS scan_livesales (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    SaleEvent JSON DEFAULT NULL,
    FuelEvent JSON DEFAULT NULL,
    VoidEvent JSON DEFAULT NULL,
    RefundEvent JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_livesales_store (storeid)
);

CREATE TABLE IF NOT EXISTS scan_gblivescan (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    SaleEvent JSON DEFAULT NULL,
    payload JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_gblivescan_store (storeid)
);

CREATE TABLE IF NOT EXISTS scan_itg_weekly (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    corpid VARCHAR(64) DEFAULT NULL,
    BeginDate DATE DEFAULT NULL,
    EndDate DATE NOT NULL,
    ReportSequenceNumbers JSON DEFAULT NULL,
    SaleEvent JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_itg_weekly_store (storeid),
    KEY idx_itg_weekly_end (EndDate)
);

-- RJR promo (ITG_Scan, RJR_Scan)
CREATE TABLE IF NOT EXISTS scan_rjr_promo (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rjr_promo_store (storeid)
);

-- coupon_sales (live.py)
CREATE TABLE IF NOT EXISTS scan_coupon_sales (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) DEFAULT NULL,
    EndDate VARCHAR(32) DEFAULT NULL,
    SaleEvents JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_coupon_sales_store_end (storeid, EndDate)
);

-- item_list (live.py)
CREATE TABLE IF NOT EXISTS scan_item_list (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_item_list_store (storeid)
);

-- mix_match (live.py)
CREATE TABLE IF NOT EXISTS scan_mix_match (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    payload JSON DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_mix_match_store (storeid)
);

-- send_item_to_pos (live.py)
CREATE TABLE IF NOT EXISTS scan_send_item_to_pos (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) DEFAULT NULL,
    Status VARCHAR(64) DEFAULT NULL,
    xml_url VARCHAR(1024) DEFAULT NULL,
    action VARCHAR(128) DEFAULT NULL,
    dataobject JSON DEFAULT NULL,
    queueID VARCHAR(128) DEFAULT NULL,
    timestamp VARCHAR(64) DEFAULT NULL,
    Response TEXT DEFAULT NULL,
    api_data JSON DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_send_item_store_status (storeid, Status)
);
