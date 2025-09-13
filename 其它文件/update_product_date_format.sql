-- SQL语句：将现有数据库中的YYYY-M格式的product_date更新为YYYY-MM格式
-- 适用于guangzhou_power_exchange_trades表

-- 更新YYYY-M格式的product_date为YYYY-MM格式
UPDATE guangzhou_power_exchange_trades 
SET product_date = CASE 
    WHEN product_date LIKE '____-_' AND LENGTH(product_date) = 6 THEN 
        CONCAT(SUBSTRING(product_date, 1, 5), '0', SUBSTRING(product_date, 6, 1))
    ELSE product_date
END
WHERE product_date LIKE '____-_' AND LENGTH(product_date) = 6;

-- 查询更新后的结果，验证更新是否成功
SELECT DISTINCT product_date 
FROM guangzhou_power_exchange_trades 
WHERE product_date IS NOT NULL 
ORDER BY product_date;

-- 可选：查看更新了多少条记录
-- SELECT COUNT(*) as updated_records 
-- FROM guangzhou_power_exchange_trades 
-- WHERE product_date LIKE '____-__' AND LENGTH(product_date) = 7;