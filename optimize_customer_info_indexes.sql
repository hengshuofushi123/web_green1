-- 客户信息页面性能优化索引
-- 针对customer_info路由中的慢查询进行索引优化

-- 1. nyj_green_certificate_ledger表索引
-- 主要用于JOIN条件和WHERE过滤
CREATE INDEX idx_nyj_ledger_project_id ON nyj_green_certificate_ledger(project_id);
CREATE INDEX idx_nyj_ledger_production_ym ON nyj_green_certificate_ledger(production_year_month);
CREATE INDEX idx_nyj_ledger_project_ym ON nyj_green_certificate_ledger(project_id, production_year_month);

-- 2. gzpt_unilateral_listings表索引
-- 用于LEFT JOIN和数量过滤
CREATE INDEX idx_gzpt_ul_project_id ON gzpt_unilateral_listings(project_id);
CREATE INDEX idx_gzpt_ul_generate_ym ON gzpt_unilateral_listings(generate_ym);
CREATE INDEX idx_gzpt_ul_order_status ON gzpt_unilateral_listings(order_status);
CREATE INDEX idx_gzpt_ul_total_quantity ON gzpt_unilateral_listings(total_quantity);
CREATE INDEX idx_gzpt_ul_project_ym_status ON gzpt_unilateral_listings(project_id, generate_ym, order_status);

-- 3. gzpt_bilateral_online_trades表索引
CREATE INDEX idx_gzpt_ol_project_id ON gzpt_bilateral_online_trades(project_id);
CREATE INDEX idx_gzpt_ol_generate_ym ON gzpt_bilateral_online_trades(generate_ym);
CREATE INDEX idx_gzpt_ol_total_quantity ON gzpt_bilateral_online_trades(total_quantity);
CREATE INDEX idx_gzpt_ol_project_ym ON gzpt_bilateral_online_trades(project_id, generate_ym);

-- 4. gzpt_bilateral_offline_trades表索引
CREATE INDEX idx_gzpt_off_project_id ON gzpt_bilateral_offline_trades(project_id);
CREATE INDEX idx_gzpt_off_generate_ym ON gzpt_bilateral_offline_trades(generate_ym);
CREATE INDEX idx_gzpt_off_total_quantity ON gzpt_bilateral_offline_trades(total_quantity);
CREATE INDEX idx_gzpt_off_project_ym ON gzpt_bilateral_offline_trades(project_id, generate_ym);

-- 5. beijing_power_exchange_trades表索引
CREATE INDEX idx_bj_trades_project_id ON beijing_power_exchange_trades(project_id);
CREATE INDEX idx_bj_trades_production_ym ON beijing_power_exchange_trades(production_year_month);
CREATE INDEX idx_bj_trades_transaction_qty ON beijing_power_exchange_trades(transaction_quantity);
CREATE INDEX idx_bj_trades_project_ym ON beijing_power_exchange_trades(project_id, production_year_month);

-- 6. guangzhou_power_exchange_trades表索引
CREATE INDEX idx_gz_trades_project_id ON guangzhou_power_exchange_trades(project_id);
CREATE INDEX idx_gz_trades_product_date ON guangzhou_power_exchange_trades(product_date);
CREATE INDEX idx_gz_trades_gpc_certifi_num ON guangzhou_power_exchange_trades(gpc_certifi_num);
CREATE INDEX idx_gz_trades_project_date ON guangzhou_power_exchange_trades(project_id, product_date);

-- 7. projects表索引（如果不存在）
CREATE INDEX idx_projects_id ON projects(id);
CREATE INDEX idx_projects_secondary_unit ON projects(secondary_unit);

-- 8. customers表索引
CREATE INDEX idx_customer_name ON customers(customer_name);
CREATE INDEX idx_customer_type ON customers(customer_type);

-- 查看索引创建结果
SELECT 'Indexes created successfully for customer_info performance optimization' as result;

-- 可选：分析表统计信息（根据数据库类型调整）
-- ANALYZE TABLE nyj_green_certificate_ledger;
-- ANALYZE TABLE gzpt_unilateral_listings;
-- ANALYZE TABLE gzpt_bilateral_online_trades;
-- ANALYZE TABLE gzpt_bilateral_offline_trades;
-- ANALYZE TABLE beijing_power_exchange_trades;
-- ANALYZE TABLE guangzhou_power_exchange_trades;
-- ANALYZE TABLE projects;
-- ANALYZE TABLE customers;