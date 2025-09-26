#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
客户信息页面性能优化脚本
分析查询性能瓶颈并创建必要的数据库索引
"""

import time
import sys
import os
from sqlalchemy import create_engine, text
from config import SQLALCHEMY_DATABASE_URI, SQLALCHEMY_ENGINE_OPTIONS

def get_db_engine():
    """获取数据库连接"""
    try:
        engine = create_engine(SQLALCHEMY_DATABASE_URI, **SQLALCHEMY_ENGINE_OPTIONS)
        return engine
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def execute_sql_file(engine, sql_file_path):
    """执行SQL文件"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        # 分割SQL语句（以分号分隔）
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        with engine.connect() as connection:
            for sql in sql_statements:
                if sql and not sql.startswith('--'):
                    try:
                        result = connection.execute(text(sql))
                        print(f"✓ 执行成功: {sql[:50]}...")
                    except Exception as e:
                        print(f"✗ 执行失败: {sql[:50]}... 错误: {e}")
            connection.commit()
        
        print("\n索引创建完成！")
        return True
    except Exception as e:
        print(f"执行SQL文件失败: {e}")
        return False

def test_query_performance(engine, project_ids_list):
    """测试查询性能"""
    print("\n=== 查询性能测试 ===")
    
    # 测试客户名称查询
    customer_sql = """
        SELECT DISTINCT
            CASE 
                WHEN bj.buyer_entity_name IS NOT NULL THEN bj.buyer_entity_name
                WHEN gz.buyer_entity_name IS NOT NULL THEN gz.buyer_entity_name
                WHEN ul.member_name IS NOT NULL THEN ul.member_name
                WHEN ol.member_name IS NOT NULL THEN ol.member_name
                WHEN off.member_name IS NOT NULL THEN off.member_name
                ELSE '未知客户'
            END as customer_name
        FROM projects p 
        JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
        LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
        LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym
        LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym
        LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month
        LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND n.production_year_month = gz.product_date
        WHERE p.id IN :project_ids
        AND (
            ul.total_quantity > 0 OR ol.total_quantity > 0 OR off.total_quantity > 0 OR
            bj.transaction_quantity > 0 OR gz.gpc_certifi_num > 0
        )
        ORDER BY customer_name
    """
    
    try:
        with engine.connect() as connection:
            start_time = time.time()
            result = connection.execute(text(customer_sql), {"project_ids": project_ids_list})
            customer_names = [row.customer_name for row in result]
            end_time = time.time()
            
            print(f"客户名称查询耗时: {end_time - start_time:.2f}秒")
            print(f"查询到客户数量: {len(customer_names)}")
            
    except Exception as e:
        print(f"查询测试失败: {e}")

def get_table_info(engine):
    """获取表信息和索引状态"""
    print("\n=== 数据库表信息 ===")
    
    tables = [
        'projects',
        'nyj_green_certificate_ledger',
        'gzpt_unilateral_listings',
        'gzpt_bilateral_online_trades',
        'gzpt_bilateral_offline_trades',
        'beijing_power_exchange_trades',
        'guangzhou_power_exchange_trades',
        'customers'
    ]
    
    try:
        with engine.connect() as connection:
            for table in tables:
                try:
                    # 获取表行数
                    count_result = connection.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    row_count = count_result.fetchone().count
                    print(f"{table}: {row_count:,} 行")
                except Exception as e:
                    print(f"{table}: 无法获取行数 ({e})")
                    
    except Exception as e:
        print(f"获取表信息失败: {e}")

def main():
    print("客户信息页面性能优化工具")
    print("=" * 50)
    
    # 获取数据库连接
    engine = get_db_engine()
    if not engine:
        print("无法连接数据库，退出程序")
        sys.exit(1)
    
    # 获取表信息
    get_table_info(engine)
    
    # 询问是否执行索引优化
    response = input("\n是否执行索引优化？(y/n): ").lower().strip()
    if response == 'y' or response == 'yes':
        sql_file_path = os.path.join(os.path.dirname(__file__), 'optimize_customer_info_indexes.sql')
        if os.path.exists(sql_file_path):
            print(f"\n执行索引优化脚本: {sql_file_path}")
            execute_sql_file(engine, sql_file_path)
        else:
            print(f"索引优化脚本不存在: {sql_file_path}")
    
    # 性能测试
    response = input("\n是否进行性能测试？(y/n): ").lower().strip()
    if response == 'y' or response == 'yes':
        # 获取一些项目ID进行测试
        try:
            with engine.connect() as connection:
                result = connection.execute(text("SELECT id FROM projects LIMIT 10"))
                project_ids = [row.id for row in result]
                if project_ids:
                    test_query_performance(engine, project_ids)
                else:
                    print("没有找到项目数据进行测试")
        except Exception as e:
            print(f"性能测试失败: {e}")
    
    print("\n优化完成！")
    print("\n建议:")
    print("1. 重启应用服务器以确保连接池刷新")
    print("2. 监控客户信息页面的加载时间")
    print("3. 如果仍然较慢，考虑添加查询缓存")
    print("4. 定期更新表统计信息以保持查询计划最优")

if __name__ == '__main__':
    main()