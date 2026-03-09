-- ============================================================
-- Normalized child tables: JSON arrays/objects → separate rows/columns
-- Run AFTER create_scan_tables_mysql.sql
-- ============================================================

USE live_rtn_stopnsave;

-- 1. scan_gb_department.departments → scan_gb_department_item (one row per department)
CREATE TABLE IF NOT EXISTS scan_gb_department_item (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    name VARCHAR(255) DEFAULT NULL,
    sysid VARCHAR(32) NOT NULL,
    taxid VARCHAR(32) DEFAULT NULL,
    negative VARCHAR(16) DEFAULT NULL,
    discountable VARCHAR(16) DEFAULT NULL,
    product_code VARCHAR(64) DEFAULT NULL,
    food_stampable VARCHAR(16) DEFAULT NULL,
    age_restriction VARCHAR(16) DEFAULT NULL,
    fractional_quantity_allowed VARCHAR(16) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_gbdept_item_store (storeid),
    KEY idx_gbdept_item_sysid (storeid, sysid)
);

-- 2. scan_gbtax.taxInfo → scan_gbtax_item (one row per tax)
CREATE TABLE IF NOT EXISTS scan_gbtax_item (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    taxid VARCHAR(32) NOT NULL,
    tax_percent DECIMAL(10,4) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_gbtax_item_store (storeid),
    KEY idx_gbtax_item_taxid (storeid, taxid)
);

-- 3. scan_data_dept.scanDept → scan_data_dept_item (one row per department/sysid)
CREATE TABLE IF NOT EXISTS scan_data_dept_item (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    sysid VARCHAR(32) NOT NULL,
    name VARCHAR(255) DEFAULT NULL,
    taxid VARCHAR(32) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_scan_dept_item_store (storeid),
    KEY idx_scan_dept_item_sysid (storeid, sysid)
);

-- 4. scan_upc.UPCCodes → scan_upc_code (one row per UPC code)
CREATE TABLE IF NOT EXISTS scan_upc_code (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    upc_parent_id BIGINT NOT NULL,
    cycleCode VARCHAR(64) DEFAULT NULL,
    upc_code VARCHAR(64) NOT NULL,
    sort_order INT DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_upc_code_parent (upc_parent_id),
    KEY idx_upc_code_cycle (cycleCode, upc_code)
);

-- 5. scan_brand.payload (ITG, RJR arrays) → scan_brand_upc (one row per brand+poscode)
CREATE TABLE IF NOT EXISTS scan_brand_upc (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    storeid VARCHAR(64) NOT NULL,
    brand_type VARCHAR(32) NOT NULL,
    poscode VARCHAR(64) NOT NULL,
    sort_order INT DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_brand_upc_store (storeid),
    KEY idx_brand_upc_type (storeid, brand_type),
    KEY idx_brand_upc_poscode (storeid, brand_type, poscode)
);

-- 6. scan_gb_price_promotion.Stores → scan_gb_price_promotion_store (one row per store in Stores array)
CREATE TABLE IF NOT EXISTS scan_gb_price_promotion_store (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    promotion_parent_id BIGINT NOT NULL,
    storeid VARCHAR(64) NOT NULL,
    month VARCHAR(32) NOT NULL,
    store_data VARCHAR(255) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_gbpp_store_parent (promotion_parent_id),
    KEY idx_gbpp_store_storeid (storeid, month)
);

-- 7. scan_deptmerge.MCMDetail → scan_deptmerge_mcm_item (one row per MCM detail)
CREATE TABLE IF NOT EXISTS scan_deptmerge_mcm_item (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    deptmerge_parent_id BIGINT NOT NULL,
    storeid VARCHAR(64) NOT NULL,
    EndDate DATE NOT NULL,
    MerchandiseCode VARCHAR(64) NOT NULL,
    MerchandiseCodeDescription VARCHAR(255) DEFAULT NULL,
    SalesQuantity DECIMAL(14,4) DEFAULT NULL,
    RefundCount DECIMAL(14,4) DEFAULT NULL,
    SalesAmount DECIMAL(14,2) DEFAULT NULL,
    RefundAmount DECIMAL(14,2) DEFAULT NULL,
    DiscountAmount DECIMAL(14,2) DEFAULT NULL,
    PromotionAmount DECIMAL(14,2) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_deptmerge_mcm_parent (deptmerge_parent_id),
    KEY idx_deptmerge_mcm_store_end (storeid, EndDate),
    KEY idx_deptmerge_mcm_code (storeid, EndDate, MerchandiseCode)
);

-- 8. scan_mcm.MCMDetail → scan_mcm_detail_item (one row per MCM detail from shiftreps)
CREATE TABLE IF NOT EXISTS scan_mcm_detail_item (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    mcm_parent_id BIGINT NOT NULL,
    storeid VARCHAR(64) NOT NULL,
    EndDate VARCHAR(32) NOT NULL,
    MerchandiseCode VARCHAR(64) NOT NULL,
    MerchandiseCodeDescription VARCHAR(255) DEFAULT NULL,
    SalesQuantity VARCHAR(32) DEFAULT NULL,
    SalesAmount VARCHAR(32) DEFAULT NULL,
    RefundCount VARCHAR(32) DEFAULT NULL,
    RefundAmount VARCHAR(32) DEFAULT NULL,
    DiscountAmount VARCHAR(32) DEFAULT NULL,
    PromotionAmount VARCHAR(32) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_mcm_detail_parent (mcm_parent_id),
    KEY idx_mcm_detail_store_end (storeid, EndDate)
);
