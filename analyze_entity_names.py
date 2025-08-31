#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析buyer_entity_name和member_name字段的数据重叠情况
查询beijing_power_exchange_trades表的buyer_entity_name字段和
gzpt_bilateral_offline_trades表的member_name字段中出现过的所有不重复的数据，
统计有多少个在guangzhou_power_exchange_trades表的buyer_entity_name字段中出现过，
有多少个没出现过。
"""

from flask import Flask
from models import db
from sqlalchemy import text
import sys

def query_beijing_buyer_names(connection):
    """查询beijing_power_exchange_trades表的buyer_entity_name字段"""
    query = text("""
        SELECT DISTINCT buyer_entity_name 
        FROM beijing_power_exchange_trades 
        WHERE buyer_entity_name IS NOT NULL 
        AND buyer_entity_name != '' 
        AND buyer_entity_name != 'NULL'
    """)
    result = connection.execute(query)
    return [row[0] for row in result.fetchall()]

def query_gzpt_member_names(connection):
    """查询gzpt_bilateral_offline_trades表的member_name字段"""
    query = text("""
        SELECT DISTINCT member_name 
        FROM gzpt_bilateral_offline_trades 
        WHERE member_name IS NOT NULL 
        AND member_name != '' 
        AND member_name != 'NULL'
    """)
    result = connection.execute(query)
    return [row[0] for row in result.fetchall()]

def query_guangzhou_buyer_names(connection):
    """查询guangzhou_power_exchange_trades表的buyer_entity_name字段"""
    query = text("""
        SELECT DISTINCT buyer_entity_name 
        FROM guangzhou_power_exchange_trades 
        WHERE buyer_entity_name IS NOT NULL 
        AND buyer_entity_name != '' 
        AND buyer_entity_name != 'NULL'
    """)
    result = connection.execute(query)
    return [row[0] for row in result.fetchall()]

def analyze_entity_names():
    """主分析函数"""
    print("开始分析buyer_entity_name和member_name字段的数据重叠情况...")
    print("=" * 60)
    
    # 创建Flask应用上下文
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/green'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_recycle': 280,
        'pool_pre_ping': True
    }
    
    with app.app_context():
        db.init_app(app)
        
        with db.engine.connect() as connection:
            try:
                # 1. 查询beijing_power_exchange_trades表的buyer_entity_name
                print("1. 查询beijing_power_exchange_trades表的buyer_entity_name字段...")
                beijing_buyers = query_beijing_buyer_names(connection)
                print(f"   找到 {len(beijing_buyers)} 个不重复的buyer_entity_name")
                
                # 2. 查询gzpt_bilateral_offline_trades表的member_name
                print("2. 查询gzpt_bilateral_offline_trades表的member_name字段...")
                gzpt_members = query_gzpt_member_names(connection)
                print(f"   找到 {len(gzpt_members)} 个不重复的member_name")
                
                # 3. 合并两个列表并去重
                print("3. 合并两个数据源并去重...")
                all_entities = list(set(beijing_buyers + gzpt_members))
                print(f"   合并后共有 {len(all_entities)} 个不重复的实体名称")
                
                # 4. 查询guangzhou_power_exchange_trades表的buyer_entity_name
                print("4. 查询guangzhou_power_exchange_trades表的buyer_entity_name字段...")
                guangzhou_buyers = query_guangzhou_buyer_names(connection)
                guangzhou_buyers_set = set(guangzhou_buyers)
                print(f"   找到 {len(guangzhou_buyers)} 个不重复的buyer_entity_name")
                
                # 5. 分析重叠情况
                print("5. 分析数据重叠情况...")
                print("=" * 60)
                
                # 统计在guangzhou表中出现的实体
                found_in_guangzhou = []
                not_found_in_guangzhou = []
                
                for entity in all_entities:
                    if entity in guangzhou_buyers_set:
                        found_in_guangzhou.append(entity)
                    else:
                        not_found_in_guangzhou.append(entity)
                
                # 输出统计结果
                print(f"总计分析实体数量: {len(all_entities)}")
                print(f"在guangzhou_power_exchange_trades表中出现的实体数量: {len(found_in_guangzhou)}")
                print(f"在guangzhou_power_exchange_trades表中未出现的实体数量: {len(not_found_in_guangzhou)}")
                print(f"重叠率: {len(found_in_guangzhou)/len(all_entities)*100:.2f}%")
                
                print("\n" + "=" * 60)
                print("详细分析结果:")
                print("=" * 60)
                
                # 显示在guangzhou表中出现的实体
                if found_in_guangzhou:
                    print(f"\n在guangzhou_power_exchange_trades表中出现的实体 ({len(found_in_guangzhou)}个):")
                    print("-" * 40)
                    for i, entity in enumerate(sorted(found_in_guangzhou), 1):
                        print(f"{i:3d}. {entity}")
                
                # 显示在guangzhou表中未出现的实体
                if not_found_in_guangzhou:
                    print(f"\n在guangzhou_power_exchange_trades表中未出现的实体 ({len(not_found_in_guangzhou)}个):")
                    print("-" * 40)
                    for i, entity in enumerate(sorted(not_found_in_guangzhou), 1):
                        print(f"{i:3d}. {entity}")
                
                # 数据来源分析
                print("\n" + "=" * 60)
                print("数据来源分析:")
                print("=" * 60)
                
                beijing_only = set(beijing_buyers) - set(gzpt_members)
                gzpt_only = set(gzpt_members) - set(beijing_buyers)
                both_sources = set(beijing_buyers) & set(gzpt_members)
                
                print(f"仅在beijing_power_exchange_trades表中出现: {len(beijing_only)}个")
                print(f"仅在gzpt_bilateral_offline_trades表中出现: {len(gzpt_only)}个")
                print(f"在两个表中都出现: {len(both_sources)}个")
                
                if both_sources:
                    print(f"\n在两个源表中都出现的实体:")
                    for i, entity in enumerate(sorted(both_sources), 1):
                        print(f"{i:3d}. {entity}")
        
            except Exception as e:
                print(f"分析过程中发生错误: {e}")
                sys.exit(1)
    
    print("\n分析完成!")

if __name__ == "__main__":
    analyze_entity_names()