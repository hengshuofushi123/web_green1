#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询指定生产时间和交易时间范围内，各交易平台的总成交量和均价
包括：北京电力交易中心、广州电力交易中心、绿证交易平台
"""

import pymysql
from decimal import Decimal
from datetime import datetime

def query_platform_volumes_and_prices(production_start, production_end, transaction_start, transaction_end):
    """
    查询各平台在指定时间范围内的成交量和均价
    
    Args:
        production_start (str): 生产时间起始，格式：YYYY-MM
        production_end (str): 生产时间结束，格式：YYYY-MM
        transaction_start (str): 交易时间起始，格式：YYYY-MM-DD
        transaction_end (str): 交易时间结束，格式：YYYY-MM-DD
    
    Returns:
        dict: 包含各平台数据的字典
    """
    
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='green',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    
    results = {}
    
    try:
        # 1. 查询广州电力交易中心
        print("\n=== 查询广州电力交易中心 ===")
        guangzhou_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty,
            COALESCE(SUM(CASE WHEN gpc_certifi_num != 0 AND unit_price != 0 THEN CAST(gpc_certifi_num AS DECIMAL(15,2)) * CAST(unit_price AS DECIMAL(15,4)) ELSE 0 END), 0) as total_amount
        FROM guangzhou_power_exchange_trades
        WHERE deal_time BETWEEN %s AND %s
        AND product_date >= %s
        AND product_date <= %s
        AND LENGTH(product_date) >= 7
        AND product_date LIKE '____-__'
        AND gpc_certifi_num IS NOT NULL
        AND unit_price IS NOT NULL;
        """
        
        cursor.execute(guangzhou_sql, (transaction_start, transaction_end, production_start, production_end))
        gz_result = cursor.fetchone()
        gz_volume = float(gz_result[1])
        gz_amount = float(gz_result[2])
        gz_avg_price = round(gz_amount / gz_volume, 4) if gz_volume > 0 else 0
        
        results['guangzhou'] = {
            'platform_name': '广州电力交易中心',
            'record_count': gz_result[0],
            'total_quantity': gz_volume,
            'average_price': gz_avg_price
        }
        
        # 2. 查询北京电力交易中心
        print("\n=== 查询北京电力交易中心 ===")
        beijing_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(transaction_quantity AS DECIMAL(15,2))), 0) as total_qty,
            COALESCE(SUM(CASE WHEN transaction_quantity != 0 AND transaction_price != 0 THEN CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,4)) ELSE 0 END), 0) as total_amount
        FROM beijing_power_exchange_trades
        WHERE transaction_time BETWEEN %s AND %s
        AND production_year_month >= %s
        AND production_year_month <= %s
        AND LENGTH(production_year_month) >= 7
        AND production_year_month LIKE '____-__'
        AND transaction_quantity IS NOT NULL
        AND transaction_price IS NOT NULL;
        """
        
        cursor.execute(beijing_sql, (transaction_start, transaction_end, production_start, production_end))
        bj_result = cursor.fetchone()
        bj_volume = float(bj_result[1])
        bj_amount = float(bj_result[2])
        bj_avg_price = round(bj_amount / bj_volume, 4) if bj_volume > 0 else 0
        
        results['beijing'] = {
            'platform_name': '北京电力交易中心',
            'record_count': bj_result[0],
            'total_quantity': bj_volume,
            'average_price': bj_avg_price
        }
        
        # 3. 查询绿证交易平台（包括三个子表）
        print("\n=== 查询绿证交易平台 ===")
        
        # 3.1 单向挂牌
        unilateral_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0) as total_qty,
            COALESCE(SUM(CASE WHEN total_quantity != 0 AND total_amount != 0 THEN CAST(total_amount AS DECIMAL(15,4)) ELSE 0 END), 0) as total_amount
        FROM gzpt_unilateral_listings
        WHERE order_time BETWEEN %s AND %s
        AND generate_ym >= %s
        AND generate_ym <= %s
        AND LENGTH(generate_ym) >= 7
        AND generate_ym LIKE '____-__'
        AND total_quantity IS NOT NULL
        AND total_amount IS NOT NULL
        AND order_status = '1';
        """
        
        cursor.execute(unilateral_sql, (transaction_start, transaction_end, production_start, production_end))
        ul_result = cursor.fetchone()
        
        # 3.2 双边线上
        bilateral_online_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0) as total_qty,
            COALESCE(SUM(CAST(total_amount AS DECIMAL(15,4))), 0) as total_amount
        FROM gzpt_bilateral_online_trades
        WHERE order_time BETWEEN %s AND %s
        AND generate_ym >= %s
        AND generate_ym <= %s
        AND LENGTH(generate_ym) >= 7
        AND generate_ym LIKE '____-__'
        AND total_quantity IS NOT NULL
        AND total_amount IS NOT NULL
        AND order_status = '2';
        """
        
        cursor.execute(bilateral_online_sql, (transaction_start, transaction_end, production_start, production_end))
        bo_result = cursor.fetchone()
        
        # 3.3 双边线下
        bilateral_offline_sql = """
        SELECT 
            COUNT(*) as record_count,
            COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0) as total_qty,
            COALESCE(SUM(CAST(total_amount AS DECIMAL(15,4))), 0) as total_amount
        FROM gzpt_bilateral_offline_trades
        WHERE order_time BETWEEN %s AND %s
        AND generate_ym >= %s
        AND generate_ym <= %s
        AND LENGTH(generate_ym) >= 7
        AND generate_ym LIKE '____-__'
        AND total_quantity IS NOT NULL
        AND total_amount IS NOT NULL
        AND order_status = '3';
        """
        
        cursor.execute(bilateral_offline_sql, (transaction_start, transaction_end, production_start, production_end))
        bf_result = cursor.fetchone()
        
        # 合并绿证交易平台数据
        gzpt_total_records = ul_result[0] + bo_result[0] + bf_result[0]
        gzpt_total_quantity = float(ul_result[1]) + float(bo_result[1]) + float(bf_result[1])
        gzpt_total_amount = float(ul_result[2]) + float(bo_result[2]) + float(bf_result[2])
        
        # 计算加权平均价格
        if gzpt_total_quantity > 0:
            gzpt_weighted_avg_price = round(gzpt_total_amount / gzpt_total_quantity, 4)
        else:
            gzpt_weighted_avg_price = 0.0
        
        results['gzpt'] = {
            'platform_name': '绿证交易平台',
            'record_count': gzpt_total_records,
            'total_quantity': gzpt_total_quantity,
            'average_price': gzpt_weighted_avg_price,
            'sub_platforms': {
                'unilateral': {'records': ul_result[0], 'quantity': float(ul_result[1]), 'avg_price': round(float(ul_result[2]) / float(ul_result[1]), 4) if float(ul_result[1]) > 0 else 0},
                'bilateral_online': {'records': bo_result[0], 'quantity': float(bo_result[1]), 'avg_price': round(float(bo_result[2]) / float(bo_result[1]), 4) if float(bo_result[1]) > 0 else 0},
                'bilateral_offline': {'records': bf_result[0], 'quantity': float(bf_result[1]), 'avg_price': round(float(bf_result[2]) / float(bf_result[1]), 4) if float(bf_result[1]) > 0 else 0}
            }
        }
        

        
        # 4. 计算总计
        total_records = results['guangzhou']['record_count'] + results['beijing']['record_count'] + results['gzpt']['record_count']
        total_quantity = results['guangzhou']['total_quantity'] + results['beijing']['total_quantity'] + results['gzpt']['total_quantity']
        
        # 计算总体加权平均价格
        total_amount = gz_amount + bj_amount + gzpt_total_amount
        if total_quantity > 0:
            total_weighted_avg_price = round(total_amount / total_quantity, 4)
        else:
            total_weighted_avg_price = 0.0
        
        results['total'] = {
            'platform_name': '三个平台总计',
            'record_count': total_records,
            'total_quantity': total_quantity,
            'average_price': total_weighted_avg_price
        }
        
    except Exception as e:
        print(f"查询过程中发生错误: {e}")
        raise e
    finally:
        conn.close()
    
    return results

def print_results(results, production_start, production_end, transaction_start, transaction_end):
    """
    格式化打印查询结果
    """
    print(f"\n{'='*80}")
    print(f"查询条件:")
    print(f"  生产时间范围: {production_start} 至 {production_end}")
    print(f"  交易时间范围: {transaction_start} 至 {transaction_end}")
    print(f"{'='*80}")
    
    for platform_key, data in results.items():
        if platform_key == 'total':
            print(f"\n{'='*50}")
        
        print(f"\n【{data['platform_name']}】")
        print(f"  记录数量: {data['record_count']:,}")
        print(f"  总成交量: {data['total_quantity']:,.2f}")
        print(f"  平均价格: {data['average_price']:.4f} 元")
        
        # 如果是绿证交易平台，显示子平台详情
        if platform_key == 'gzpt' and 'sub_platforms' in data:
            print(f"  子平台详情:")
            for sub_key, sub_data in data['sub_platforms'].items():
                sub_name = {'unilateral': '单向挂牌', 'bilateral_online': '双边线上', 'bilateral_offline': '双边线下'}[sub_key]
                print(f"    {sub_name}: {sub_data['records']} 条记录, {sub_data['quantity']:,.2f} 成交量, {sub_data['avg_price']:.4f} 元均价")

if __name__ == "__main__":
    # 示例查询参数
    production_start = "2024-01"  # 生产时间起始
    production_end = "2024-12"    # 生产时间结束
    transaction_start = "2025-03-01"  # 交易时间起始
    transaction_end = "2025-03-31"    # 交易时间结束
    
    print("开始查询各平台成交量和均价...")
    
    try:
        results = query_platform_volumes_and_prices(
            production_start, production_end, 
            transaction_start, transaction_end
        )
        
        print_results(results, production_start, production_end, transaction_start, transaction_end)
        
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()