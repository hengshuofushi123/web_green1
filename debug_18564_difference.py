#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试dashboard(177,870)与query_platform_volumes_and_prices.py(159,306)之间18,564的差异
"""

import pymysql
from decimal import Decimal

def debug_18564_difference():
    """
    分析18,564差异的具体来源
    """
    
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='green',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    
    try:
        print("=== 调试18,564差异的具体来源 ===")
        print("Dashboard显示: 177,870")
        print("query_platform_volumes_and_prices.py显示: 159,306")
        print("差异: 18,564")
        print()
        
        # 1. 检查广州电力交易中心的详细数据
        print("1. 广州电力交易中心详细分析:")
        
        # 1.1 query_platform_volumes_and_prices.py的查询方式
        query_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty,
            MIN(deal_time) as min_deal_time,
            MAX(deal_time) as max_deal_time,
            MIN(product_date) as min_product_date,
            MAX(product_date) as max_product_date
        FROM guangzhou_power_exchange_trades
        WHERE deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND product_date >= '2024-01'
        AND product_date <= '2024-12'
        AND LENGTH(product_date) >= 7
        AND product_date LIKE '____-__'
        AND gpc_certifi_num IS NOT NULL;
        """
        
        cursor.execute(query_sql)
        query_result = cursor.fetchone()
        print(f"  query_platform_volumes_and_prices.py方式:")
        print(f"    记录数: {query_result[0]}")
        print(f"    总量: {float(query_result[1]):,.2f}")
        print(f"    交易时间范围: {query_result[2]} 到 {query_result[3]}")
        print(f"    生产时间范围: {query_result[4]} 到 {query_result[5]}")
        print()
        
        # 1.2 dashboard的查询方式（通过ledger表JOIN）
        dashboard_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty,
            MIN(gz.deal_time) as min_deal_time,
            MAX(gz.deal_time) as max_deal_time,
            MIN(gz.product_date) as min_product_date,
            MAX(gz.product_date) as max_product_date
        FROM nyj_green_certificate_ledger n
        LEFT JOIN guangzhou_power_exchange_trades gz ON 
            n.project_id = gz.project_id AND 
            CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month
        WHERE gz.deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12'
        AND gz.gpc_certifi_num IS NOT NULL;
        """
        
        cursor.execute(dashboard_sql)
        dashboard_result = cursor.fetchone()
        print(f"  Dashboard方式（通过ledger表JOIN）:")
        print(f"    记录数: {dashboard_result[0]}")
        print(f"    总量: {float(dashboard_result[1]):,.2f}")
        print(f"    交易时间范围: {dashboard_result[2]} 到 {dashboard_result[3]}")
        print(f"    生产时间范围: {dashboard_result[4]} 到 {dashboard_result[5]}")
        print()
        
        # 2. 检查是否有其他平台的数据
        print("2. 检查其他交易平台的数据:")
        
        # 2.1 单向挂牌
        ul_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) as total_qty
        FROM nyj_green_certificate_ledger n
        LEFT JOIN gzpt_unilateral_listings ul ON 
            n.project_id = ul.project_id AND 
            n.production_year_month = ul.generate_ym AND 
            ul.order_status = '1' AND 
            ul.total_quantity IS NOT NULL AND ul.total_quantity > 0 AND 
            ul.total_amount IS NOT NULL AND ul.total_amount > 0
        WHERE ul.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12'
        AND ul.total_quantity IS NOT NULL;
        """
        
        cursor.execute(ul_sql)
        ul_result = cursor.fetchone()
        print(f"  单向挂牌: {ul_result[0]}条记录, {float(ul_result[1]):,.2f}")
        
        # 2.2 双边线上
        ol_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) as total_qty
        FROM nyj_green_certificate_ledger n
        LEFT JOIN gzpt_bilateral_online_trades ol ON 
            n.project_id = ol.project_id AND 
            n.production_year_month = ol.generate_ym AND 
            ol.order_status = '2' AND 
            ol.total_quantity IS NOT NULL AND ol.total_quantity > 0 AND 
            ol.total_amount IS NOT NULL AND ol.total_amount > 0
        WHERE ol.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12'
        AND ol.total_quantity IS NOT NULL;
        """
        
        cursor.execute(ol_sql)
        ol_result = cursor.fetchone()
        print(f"  双边线上: {ol_result[0]}条记录, {float(ol_result[1]):,.2f}")
        
        # 2.3 双边线下
        off_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) as total_qty
        FROM nyj_green_certificate_ledger n
        LEFT JOIN gzpt_bilateral_offline_trades off ON 
            n.project_id = off.project_id AND 
            n.production_year_month = off.generate_ym AND 
            off.order_status = '3' AND 
            off.total_quantity IS NOT NULL AND off.total_quantity > 0 AND 
            off.total_amount IS NOT NULL AND off.total_amount > 0
        WHERE off.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12'
        AND off.total_quantity IS NOT NULL;
        """
        
        cursor.execute(off_sql)
        off_result = cursor.fetchone()
        print(f"  双边线下: {off_result[0]}条记录, {float(off_result[1]):,.2f}")
        
        # 2.4 北京电力交易中心
        bj_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) as total_qty
        FROM nyj_green_certificate_ledger n
        LEFT JOIN beijing_power_exchange_trades bj ON 
            n.project_id = bj.project_id AND 
            n.production_year_month = bj.production_year_month
        WHERE bj.transaction_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12'
        AND bj.transaction_quantity IS NOT NULL;
        """
        
        cursor.execute(bj_sql)
        bj_result = cursor.fetchone()
        print(f"  北京电力交易中心: {bj_result[0]}条记录, {float(bj_result[1]):,.2f}")
        
        # 计算dashboard总和
        dashboard_calculated_total = float(ul_result[1]) + float(ol_result[1]) + float(off_result[1]) + float(bj_result[1]) + float(dashboard_result[1])
        print(f"\n  Dashboard计算总和: {dashboard_calculated_total:,.2f}")
        print(f"  与177,870的差异: {abs(dashboard_calculated_total - 177870):,.2f}")
        
        # 3. 分析差异来源
        print(f"\n3. 差异分析:")
        print(f"  广州电力交易中心差异: {float(dashboard_result[1]) - float(query_result[1]):,.2f}")
        print(f"  其他平台总量: {float(ul_result[1]) + float(ol_result[1]) + float(off_result[1]) + float(bj_result[1]):,.2f}")
        
        # 4. 检查是否有时间过滤的差异
        print(f"\n4. 检查时间过滤差异:")
        
        # 检查广州交易表中是否有不符合时间条件的数据
        time_check_sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59' THEN 1 END) as in_transaction_range,
            COUNT(CASE WHEN product_date >= '2024-01' AND product_date <= '2024-12' THEN 1 END) as in_production_range,
            COUNT(CASE WHEN deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59' 
                       AND product_date >= '2024-01' AND product_date <= '2024-12' THEN 1 END) as in_both_ranges
        FROM guangzhou_power_exchange_trades
        WHERE gpc_certifi_num IS NOT NULL
        AND LENGTH(product_date) >= 7
        AND product_date LIKE '____-__';
        """
        
        cursor.execute(time_check_sql)
        time_check = cursor.fetchone()
        print(f"  广州交易表总记录数: {time_check[0]}")
        print(f"  符合交易时间范围的记录: {time_check[1]}")
        print(f"  符合生产时间范围的记录: {time_check[2]}")
        print(f"  同时符合两个时间范围的记录: {time_check[3]}")
        
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    debug_18564_difference()