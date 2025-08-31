#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试dashboard显示177,870的具体原因
分析所有交易平台的数据汇总
"""

import pymysql
from decimal import Decimal

def debug_dashboard_total():
    """
    分析dashboard显示177,870的具体构成
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
        print("=== 调试dashboard显示177,870的具体构成 ===")
        print("生产时间范围: 2024-01 至 2024-12")
        print("交易时间范围: 2025-03-01 至 2025-03-31")
        print()
        
        # 模拟dashboard的完整查询逻辑
        print("1. 模拟dashboard的完整查询（所有平台汇总）:")
        dashboard_sql = """
        SELECT 
            -- 单向挂牌
            COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) as unilateral_qty,
            -- 双边线上
            COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) as bilateral_online_qty,
            -- 双边线下
            COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) as bilateral_offline_qty,
            -- 北京电力交易中心
            COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) as beijing_qty,
            -- 广州电力交易中心
            COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as guangzhou_qty,
            -- 总计
            COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
            COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
            COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
            COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
            COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty
        FROM nyj_green_certificate_ledger n
        LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1' AND ul.total_quantity IS NOT NULL AND ul.total_quantity > 0 AND ul.total_amount IS NOT NULL AND ul.total_amount > 0 AND ul.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym AND ol.order_status = '2' AND ol.total_quantity IS NOT NULL AND ol.total_quantity > 0 AND ol.total_amount IS NOT NULL AND ol.total_amount > 0 AND ol.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym AND off.order_status = '3' AND off.total_quantity IS NOT NULL AND off.total_quantity > 0 AND off.total_amount IS NOT NULL AND off.total_amount > 0 AND off.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month AND bj.transaction_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month AND gz.deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        WHERE n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12';
        """
        
        cursor.execute(dashboard_sql)
        dashboard_result = cursor.fetchone()
        
        print(f"  单向挂牌: {float(dashboard_result[0]):,.2f}")
        print(f"  双边线上: {float(dashboard_result[1]):,.2f}")
        print(f"  双边线下: {float(dashboard_result[2]):,.2f}")
        print(f"  北京电力交易中心: {float(dashboard_result[3]):,.2f}")
        print(f"  广州电力交易中心: {float(dashboard_result[4]):,.2f}")
        print(f"  总计: {float(dashboard_result[5]):,.2f}")
        print()
        
        # 2. 分别查询各个平台的详细数据
        print("2. 分别查询各个平台的详细数据:")
        
        # 2.1 单向挂牌
        ul_sql = """
        SELECT COUNT(*), COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0)
        FROM gzpt_unilateral_listings
        WHERE order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND generate_ym >= '2024-01' AND generate_ym <= '2024-12'
        AND order_status = '1' AND total_quantity IS NOT NULL AND total_quantity > 0 
        AND total_amount IS NOT NULL AND total_amount > 0;
        """
        cursor.execute(ul_sql)
        ul_result = cursor.fetchone()
        print(f"  单向挂牌直接查询: {ul_result[0]}条记录, {float(ul_result[1]):,.2f}")
        
        # 2.2 双边线上
        ol_sql = """
        SELECT COUNT(*), COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0)
        FROM gzpt_bilateral_online_trades
        WHERE order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND generate_ym >= '2024-01' AND generate_ym <= '2024-12'
        AND order_status = '2' AND total_quantity IS NOT NULL AND total_quantity > 0 
        AND total_amount IS NOT NULL AND total_amount > 0;
        """
        cursor.execute(ol_sql)
        ol_result = cursor.fetchone()
        print(f"  双边线上直接查询: {ol_result[0]}条记录, {float(ol_result[1]):,.2f}")
        
        # 2.3 双边线下
        off_sql = """
        SELECT COUNT(*), COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0)
        FROM gzpt_bilateral_offline_trades
        WHERE order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND generate_ym >= '2024-01' AND generate_ym <= '2024-12'
        AND order_status = '3' AND total_quantity IS NOT NULL AND total_quantity > 0 
        AND total_amount IS NOT NULL AND total_amount > 0;
        """
        cursor.execute(off_sql)
        off_result = cursor.fetchone()
        print(f"  双边线下直接查询: {off_result[0]}条记录, {float(off_result[1]):,.2f}")
        
        # 2.4 北京电力交易中心
        bj_sql = """
        SELECT COUNT(*), COALESCE(SUM(CAST(transaction_quantity AS DECIMAL(15,2))), 0)
        FROM beijing_power_exchange_trades
        WHERE transaction_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND production_year_month >= '2024-01' AND production_year_month <= '2024-12';
        """
        cursor.execute(bj_sql)
        bj_result = cursor.fetchone()
        print(f"  北京电力交易中心直接查询: {bj_result[0]}条记录, {float(bj_result[1]):,.2f}")
        
        # 2.5 广州电力交易中心
        gz_sql = """
        SELECT COUNT(*), COALESCE(SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))), 0)
        FROM guangzhou_power_exchange_trades
        WHERE deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND product_date >= '2024-01' AND product_date <= '2024-12'
        AND LENGTH(product_date) >= 7 AND product_date LIKE '____-__'
        AND gpc_certifi_num IS NOT NULL;
        """
        cursor.execute(gz_sql)
        gz_result = cursor.fetchone()
        print(f"  广州电力交易中心直接查询: {gz_result[0]}条记录, {float(gz_result[1]):,.2f}")
        
        # 计算直接查询的总和
        direct_total = float(ul_result[1]) + float(ol_result[1]) + float(off_result[1]) + float(bj_result[1]) + float(gz_result[1])
        print(f"\n  直接查询总和: {direct_total:,.2f}")
        print(f"  dashboard查询总和: {float(dashboard_result[5]):,.2f}")
        print(f"  差异: {abs(direct_total - float(dashboard_result[5])):,.2f}")
        
        # 3. 检查是否有重复计算
        print("\n3. 检查可能的重复计算:")
        
        # 检查是否有项目在多个平台都有交易
        overlap_sql = """
        SELECT 
            'ul_ol' as platforms,
            COUNT(DISTINCT ul.project_id) as ul_projects,
            COUNT(DISTINCT ol.project_id) as ol_projects,
            COUNT(DISTINCT CASE WHEN ul.project_id IS NOT NULL AND ol.project_id IS NOT NULL THEN ul.project_id END) as overlap_projects
        FROM gzpt_unilateral_listings ul
        FULL OUTER JOIN gzpt_bilateral_online_trades ol ON ul.project_id = ol.project_id AND ul.generate_ym = ol.generate_ym
        WHERE (ul.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59' OR ol.order_time_str BETWEEN '2025-03-01' AND '2025-03-31 23:59:59')
        AND (ul.generate_ym >= '2024-01' AND ul.generate_ym <= '2024-12' OR ol.generate_ym >= '2024-01' AND ol.generate_ym <= '2024-12');
        """
        
        try:
            cursor.execute(overlap_sql)
            overlap_result = cursor.fetchone()
            if overlap_result:
                print(f"  单向挂牌与双边线上重叠项目: {overlap_result[3] if overlap_result[3] else 0}")
        except Exception as e:
            print(f"  重叠检查失败: {e}")
        
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    debug_dashboard_total()