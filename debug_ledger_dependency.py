#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试dashboard与query_platform_volumes_and_prices.py数据差异的原因
重点分析nyj_green_certificate_ledger表的影响
"""

import pymysql
from decimal import Decimal

def debug_ledger_dependency():
    """
    分析dashboard依赖nyj_green_certificate_ledger表导致的数据差异
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
        print("=== 调试dashboard与query_platform_volumes_and_prices.py的数据差异 ===")
        print("生产时间范围: 2024-01 至 2024-12")
        print("交易时间范围: 2025-03-01 至 2025-03-31")
        print()
        
        # 1. 直接查询广州电力交易中心（不依赖ledger表）
        print("1. 直接查询广州电力交易中心（query_platform_volumes_and_prices.py方式）:")
        direct_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty
        FROM guangzhou_power_exchange_trades
        WHERE deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND product_date >= '2024-01'
        AND product_date <= '2024-12'
        AND LENGTH(product_date) >= 7
        AND product_date LIKE '____-__'
        AND gpc_certifi_num IS NOT NULL;
        """
        
        cursor.execute(direct_sql)
        direct_result = cursor.fetchone()
        print(f"  记录数: {direct_result[0]}")
        print(f"  总量: {float(direct_result[1]):,.2f}")
        print()
        
        # 2. 通过ledger表JOIN查询（dashboard方式）
        print("2. 通过ledger表JOIN查询（dashboard方式）:")
        ledger_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty
        FROM nyj_green_certificate_ledger n
        LEFT JOIN guangzhou_power_exchange_trades gz ON 
            n.project_id = gz.project_id AND 
            CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month
        WHERE gz.deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND n.production_year_month >= '2024-01'
        AND n.production_year_month <= '2024-12'
        AND gz.gpc_certifi_num IS NOT NULL;
        """
        
        cursor.execute(ledger_sql)
        ledger_result = cursor.fetchone()
        print(f"  记录数: {ledger_result[0]}")
        print(f"  总量: {float(ledger_result[1]):,.2f}")
        print()
        
        # 3. 检查ledger表中的项目ID覆盖情况
        print("3. 检查ledger表中的项目ID覆盖情况:")
        ledger_projects_sql = """
        SELECT COUNT(DISTINCT project_id) as ledger_project_count
        FROM nyj_green_certificate_ledger
        WHERE production_year_month >= '2024-01'
        AND production_year_month <= '2024-12';
        """
        
        cursor.execute(ledger_projects_sql)
        ledger_projects = cursor.fetchone()[0]
        print(f"  ledger表中的项目数: {ledger_projects}")
        
        # 4. 检查广州交易表中的项目ID覆盖情况
        gz_projects_sql = """
        SELECT COUNT(DISTINCT project_id) as gz_project_count
        FROM guangzhou_power_exchange_trades
        WHERE deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND product_date >= '2024-01'
        AND product_date <= '2024-12'
        AND LENGTH(product_date) >= 7
        AND product_date LIKE '____-__'
        AND gpc_certifi_num IS NOT NULL;
        """
        
        cursor.execute(gz_projects_sql)
        gz_projects = cursor.fetchone()[0]
        print(f"  广州交易表中的项目数: {gz_projects}")
        print()
        
        # 5. 找出在广州交易表中但不在ledger表中的项目
        print("5. 在广州交易表中但不在ledger表中的项目:")
        missing_projects_sql = """
        SELECT DISTINCT gz.project_id
        FROM guangzhou_power_exchange_trades gz
        WHERE gz.deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
        AND gz.product_date >= '2024-01'
        AND gz.product_date <= '2024-12'
        AND LENGTH(gz.product_date) >= 7
        AND gz.product_date LIKE '____-__'
        AND gz.gpc_certifi_num IS NOT NULL
        AND gz.project_id NOT IN (
            SELECT DISTINCT n.project_id
            FROM nyj_green_certificate_ledger n
            WHERE n.production_year_month >= '2024-01'
            AND n.production_year_month <= '2024-12'
        );
        """
        
        cursor.execute(missing_projects_sql)
        missing_projects = cursor.fetchall()
        print(f"  缺失的项目数: {len(missing_projects)}")
        if missing_projects:
            print(f"  缺失的项目ID: {[p[0] for p in missing_projects[:10]]}{'...' if len(missing_projects) > 10 else ''}")
        print()
        
        # 6. 计算缺失项目的交易量
        if missing_projects:
            print("6. 缺失项目的交易量:")
            missing_project_ids = [str(p[0]) for p in missing_projects]
            missing_volume_sql = f"""
            SELECT 
                COUNT(*) as record_count,
                COALESCE(SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty
            FROM guangzhou_power_exchange_trades
            WHERE deal_time BETWEEN '2025-03-01' AND '2025-03-31 23:59:59'
            AND product_date >= '2024-01'
            AND product_date <= '2024-12'
            AND LENGTH(product_date) >= 7
            AND product_date LIKE '____-__'
            AND gpc_certifi_num IS NOT NULL
            AND project_id IN ({','.join(missing_project_ids)});
            """
            
            cursor.execute(missing_volume_sql)
            missing_volume = cursor.fetchone()
            print(f"  缺失项目记录数: {missing_volume[0]}")
            print(f"  缺失项目总量: {float(missing_volume[1]):,.2f}")
            print()
            
            # 验证数据差异
            expected_dashboard_volume = float(direct_result[1]) - float(missing_volume[1])
            print(f"7. 数据差异验证:")
            print(f"  直接查询总量: {float(direct_result[1]):,.2f}")
            print(f"  缺失项目总量: {float(missing_volume[1]):,.2f}")
            print(f"  预期dashboard总量: {expected_dashboard_volume:,.2f}")
            print(f"  实际dashboard总量: {float(ledger_result[1]):,.2f}")
            print(f"  差异: {abs(expected_dashboard_volume - float(ledger_result[1])):,.2f}")
        
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    debug_ledger_dependency()