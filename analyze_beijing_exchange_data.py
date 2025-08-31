#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析北京电力交易中心的数据，找出价格计算差异的原因
"""

import pymysql
from decimal import Decimal
from datetime import datetime

def analyze_beijing_exchange_data():
    """
    分析北京电力交易中心的数据
    """
    
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='green',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    
    # 查询条件
    production_start = "2024-01"
    production_end = "2024-12"
    transaction_start = "2025-01-01"
    transaction_end = "2025-01-31"
    
    print("=== 北京电力交易中心数据分析 ===")
    print(f"查询条件:")
    print(f"  生产时间: {production_start} 至 {production_end}")
    print(f"  交易时间: {transaction_start} 至 {transaction_end}")
    print()
    
    try:
        # 我的查询脚本方式
        print("【我的查询脚本方式】")
        my_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(transaction_quantity AS DECIMAL(15,2))), 0) as total_qty,
            COALESCE(AVG(CAST(transaction_price AS DECIMAL(15,4))), 0) as avg_price,
            COALESCE(SUM(CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,4))), 0) as calculated_total_amount
        FROM beijing_power_exchange_trades
        WHERE transaction_time BETWEEN %s AND %s
        AND production_year_month >= %s
        AND production_year_month <= %s
        AND LENGTH(production_year_month) >= 7
        AND production_year_month LIKE '____-__'
        AND transaction_quantity IS NOT NULL
        AND transaction_price IS NOT NULL;
        """
        
        cursor.execute(my_sql, (transaction_start, transaction_end, production_start, production_end))
        my_result = cursor.fetchone()
        
        print(f"  记录数量: {my_result[0]}")
        print(f"  总成交量: {my_result[1]:.2f}")
        print(f"  平均价格: {my_result[2]:.4f} 元")
        print(f"  计算总金额: {my_result[3]:.2f} 元")
        print()
        
        # Dashboard API方式 - 模拟其逻辑
        print("【Dashboard API方式】")
        # 根据dashboard_routes_final.py的逻辑，它使用不同的查询方式
        api_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CASE WHEN transaction_quantity IS NOT NULL AND transaction_quantity != 0 THEN CAST(transaction_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as total_qty,
            COALESCE(SUM(CASE WHEN transaction_quantity != 0 AND transaction_price != 0 THEN CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,4)) ELSE 0 END), 0) as total_amt
        FROM beijing_power_exchange_trades
        WHERE transaction_time BETWEEN %s AND %s
        AND YEAR(STR_TO_DATE(CONCAT(production_year_month, '-01'), '%%Y-%%m-%%d')) IN (2023, 2024, 2025)
        AND CONCAT(YEAR(transaction_time), '-', LPAD(MONTH(transaction_time), 2, '0')) = '2025-01'
        """
        
        cursor.execute(api_sql, (transaction_start, transaction_end))
        api_result = cursor.fetchone()
        
        total_qty_api = float(api_result[1] or 0)
        total_amt_api = float(api_result[2] or 0)
        avg_price_api = round(total_amt_api / total_qty_api, 2) if total_qty_api > 0 else 0
        
        print(f"  记录数量: {api_result[0]}")
        print(f"  总成交量: {total_qty_api:.2f}")
        print(f"  总金额: {total_amt_api:.2f} 元")
        print(f"  平均价格: {avg_price_api:.4f} 元")
        print()
        
        # 详细数据对比
        print("【详细数据对比】")
        detail_sql = """
        SELECT 
            id, project_id, transaction_quantity, transaction_price,
            transaction_time, production_year_month,
            CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,4)) as calculated_amount
        FROM beijing_power_exchange_trades
        WHERE transaction_time BETWEEN %s AND %s
        AND production_year_month >= %s
        AND production_year_month <= %s
        AND LENGTH(production_year_month) >= 7
        AND production_year_month LIKE '____-__'
        AND transaction_quantity IS NOT NULL
        AND transaction_price IS NOT NULL
        ORDER BY transaction_time;
        """
        
        cursor.execute(detail_sql, (transaction_start, transaction_end, production_start, production_end))
        details = cursor.fetchall()
        
        print(f"详细记录 ({len(details)} 条):")
        print("ID\t项目ID\t成交量\t单价\t计算金额\t交易时间\t\t生产年月")
        print("-" * 100)
        
        total_calculated_amount = 0
        total_qty_sum = 0
        
        for detail in details:
            id_val, project_id, volume, price, transaction_time, production_ym, calculated_amount = detail
            total_calculated_amount += float(calculated_amount or 0)
            total_qty_sum += float(volume or 0)
            
            print(f"{id_val}\t{project_id}\t{volume}\t{price}\t{calculated_amount:.2f}\t{transaction_time}\t{production_ym}")
        
        print("-" * 100)
        print(f"汇总:")
        print(f"  总成交量: {total_qty_sum:.2f}")
        print(f"  计算总金额: {total_calculated_amount:.2f}")
        print(f"  我的方法平均价格: {my_result[2]:.4f}")
        print(f"  API方法平均价格: {avg_price_api:.4f}")
        print(f"  实际计算平均价格: {total_calculated_amount/total_qty_sum:.4f}")
        print()
        
        # 差异分析
        print("【差异分析】")
        price_diff = abs(float(my_result[2]) - avg_price_api)
        
        print(f"  平均价格差异: {price_diff:.4f} 元")
        
        if price_diff > 0.01:  # 如果价格差异超过1分钱
            print(f"  ⚠️  两种计算方法存在显著价格差异！")
            print(f"  可能的原因:")
            print(f"    1. 查询条件不同")
            print(f"    2. 数据过滤逻辑不同")
            print(f"    3. 计算方法不同")
        
        # 检查是否有其他符合API条件但不符合我的条件的数据
        print("\n【检查API可能包含的其他数据】")
        api_extended_sql = """
        SELECT 
            id, project_id, transaction_quantity, transaction_price,
            transaction_time, production_year_month,
            CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,4)) as calculated_amount
        FROM beijing_power_exchange_trades
        WHERE YEAR(STR_TO_DATE(CONCAT(production_year_month, '-01'), '%%Y-%%m-%%d')) IN (2023, 2024, 2025)
        AND CONCAT(YEAR(transaction_time), '-', LPAD(MONTH(transaction_time), 2, '0')) = '2025-01'
        AND transaction_quantity IS NOT NULL
        AND transaction_price IS NOT NULL
        ORDER BY transaction_time;
        """
        
        cursor.execute(api_extended_sql)
        api_details = cursor.fetchall()
        
        print(f"API查询到的所有记录 ({len(api_details)} 条):")
        if len(api_details) != len(details):
            print("⚠️  API查询到的记录数与我的查询不同！")
            print("API额外包含的记录:")
            print("ID\t项目ID\t成交量\t单价\t计算金额\t交易时间\t\t生产年月")
            print("-" * 100)
            
            my_ids = {detail[0] for detail in details}
            for detail in api_details:
                if detail[0] not in my_ids:
                    id_val, project_id, volume, price, transaction_time, production_ym, calculated_amount = detail
                    print(f"{id_val}\t{project_id}\t{volume}\t{price}\t{calculated_amount:.2f}\t{transaction_time}\t{production_ym}")
        else:
            print("✓ API查询到的记录数与我的查询相同")
        
    except Exception as e:
        print(f"查询过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    analyze_beijing_exchange_data()