# -*- coding: utf-8 -*-
"""
Dashboard数据缓存系统
用于缓存dashboard页面的统计数据，避免每次访问时重复计算
"""

import json
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from .models import db, Project, Customer
from decimal import Decimal

# 全局缓存存储
dashboard_cache = {
    'data': None,
    'timestamp': None,
    'cache_duration': 10 * 60,  # 10分钟，单位：秒
    'is_calculating': False  # 防止重复计算的标志
}

def is_cache_valid():
    """检查缓存是否有效"""
    if dashboard_cache['data'] is None or dashboard_cache['timestamp'] is None:
        return False
    
    current_time = time.time()
    cache_time = dashboard_cache['timestamp']
    
    return (current_time - cache_time) < dashboard_cache['cache_duration']

def get_cached_data():
    """获取缓存的数据"""
    if is_cache_valid():
        return dashboard_cache['data']
    return None

def calculate_dashboard_data():
    """计算dashboard页面所需的所有数据"""
    print(f"[{datetime.now()}] 开始计算dashboard数据...")
    
    # 防止重复计算
    if dashboard_cache['is_calculating']:
        print("数据正在计算中，跳过本次计算")
        return dashboard_cache['data']
    
    dashboard_cache['is_calculating'] = True
    
    try:
        # 确保在Flask应用上下文中执行
        from flask import current_app
        if not current_app:
            from app import app
            with app.app_context():
                return _do_calculate_dashboard_data()
        else:
            return _do_calculate_dashboard_data()
    except Exception as e:
        print(f"[{datetime.now()}] Dashboard数据计算出错: {str(e)}")
        return None
    finally:
        dashboard_cache['is_calculating'] = False

def _do_calculate_dashboard_data():
    """实际执行dashboard数据计算的内部函数"""
    try:
        # 1. 项目总数统计
        total_projects = Project.query.count()
        
        # 2. 从各个数据表中获取核发和销售统计
        with db.engine.connect() as connection:
            # 获取核发总量（从绿证台账）
            issued_result = connection.execute(text("""
                SELECT COALESCE(SUM(CAST(ordinary_quantity AS DECIMAL(15,2))), 0) as total_issued
                FROM nyj_green_certificate_ledger 
                WHERE ordinary_quantity IS NOT NULL AND ordinary_quantity != ''
            """))
            total_issued = float(issued_result.scalar() or 0) / 10000  # 转换为万张
            
            # 获取销售总量（从绿证台账）
            sold_result = connection.execute(text("""
                SELECT COALESCE(SUM(CAST(sold_quantity AS DECIMAL(15,2))), 0) as total_sold
                FROM nyj_green_certificate_ledger 
                WHERE sold_quantity IS NOT NULL AND sold_quantity != ''
            """))
            total_sold = float(sold_result.scalar() or 0) / 10000  # 转换为万张
            
            # 获取平均成交价（加权平均）
            avg_price_result = connection.execute(text("""
                SELECT
                    -- 计算加权平均价：总金额 / 总数量
                    CASE
                        WHEN SUM(total_quantity) > 0 THEN SUM(total_amount) / SUM(total_quantity)
                        ELSE 0
                    END as avg_price
                FROM (
                    -- 广州电力交易中心
                    SELECT
                        CAST(gpc_certifi_num AS DECIMAL(15,2)) as total_quantity,
                        CAST(total_cost AS DECIMAL(15,2)) as total_amount
                    FROM guangzhou_power_exchange_trades
                    WHERE gpc_certifi_num IS NOT NULL AND gpc_certifi_num > 0 AND total_cost IS NOT NULL

                    UNION ALL

                    -- 绿证交易平台 - 单向挂牌
                    SELECT
                        CAST(total_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(total_amount AS DECIMAL(15,2)) as total_amount
                    FROM gzpt_unilateral_listings
                    WHERE total_quantity IS NOT NULL AND total_quantity > 0 AND total_amount IS NOT NULL AND order_status = '1'

                    UNION ALL

                    -- 绿证交易平台 - 双边线下
                    SELECT
                        CAST(total_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(total_amount AS DECIMAL(15,2)) as total_amount
                    FROM gzpt_bilateral_offline_trades
                    WHERE total_quantity IS NOT NULL AND total_quantity > 0 AND total_amount IS NOT NULL AND order_status = '3'
                    
                ) as all_trades;
            """))
            avg_price = float(avg_price_result.scalar() or 0)
            
            # 获取按省份的销售TOP10（卖方省份）
            province_sales = connection.execute(text("""
                SELECT province, COALESCE(SUM(CAST(sold_quantity AS DECIMAL(15,2))), 0) as sales
                FROM nyj_green_certificate_ledger 
                WHERE sold_quantity IS NOT NULL AND sold_quantity != '' AND province IS NOT NULL
                GROUP BY province 
                ORDER BY sales DESC 
                LIMIT 10
            """)).fetchall()
            
            # 获取按买方省份的成交TOP10
            projects = Project.query.all()
            project_ids_list = [p.id for p in projects]
            
            if not project_ids_list:
                buyer_province_sales = []
            else:
                # 获取交易数据
                transaction_sql = """
                    SELECT 
                        CASE 
                            WHEN bj.buyer_entity_name IS NOT NULL THEN bj.buyer_entity_name
                            WHEN gz.buyer_entity_name IS NOT NULL THEN gz.buyer_entity_name
                            WHEN ul.member_name IS NOT NULL THEN ul.member_name
                            WHEN ol.member_name IS NOT NULL THEN ol.member_name
                            WHEN off.member_name IS NOT NULL THEN off.member_name
                            ELSE '未知客户'
                        END as customer_name,
                        
                        -- 总成交量
                        COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                        COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                        COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                        COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                        COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_quantity
                    FROM projects p 
                    JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
                    LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
                    LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym
                    LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym
                    LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month
                    LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND n.production_year_month = gz.product_date
                    WHERE p.id IN :project_ids
                    AND n.production_year_month >= '0000-01'
                    AND n.production_year_month <= '9999-12'
                    GROUP BY customer_name
                    HAVING total_quantity > 0
                """
                
                transaction_result = connection.execute(
                    text(transaction_sql),
                    {"project_ids": project_ids_list}
                )
                
                # 按客户名称聚合交易量
                customer_volumes = {}
                for row in transaction_result:
                    if row.customer_name and row.customer_name != '未知客户':
                        customer_volumes[row.customer_name] = float(row.total_quantity or 0)
            
                # 获取所有有省份信息的客户
                all_customers = Customer.query.filter(
                    Customer.province.isnot(None),
                    Customer.province != '未设置'
                ).all()
                
                # 按省份聚合成交量
                province_volumes = {}
                for customer in all_customers:
                    if customer.customer_name in customer_volumes:
                        province = customer.province
                        volume = customer_volumes[customer.customer_name]
                        if province not in province_volumes:
                            province_volumes[province] = 0
                        province_volumes[province] += volume
                
                # 省份名称映射
                province_name_mapping = {
                    '北京市': '北京', '天津市': '天津', '河北省': '河北', '山西省': '山西',
                    '内蒙古自治区': '内蒙古', '辽宁省': '辽宁', '吉林省': '吉林', '黑龙江省': '黑龙江',
                    '上海市': '上海', '江苏省': '江苏', '浙江省': '浙江', '安徽省': '安徽',
                    '福建省': '福建', '江西省': '江西', '山东省': '山东', '河南省': '河南',
                    '湖北省': '湖北', '湖南省': '湖南', '广东省': '广东', '广西壮族自治区': '广西',
                    '海南省': '海南', '重庆市': '重庆', '四川省': '四川', '贵州省': '贵州',
                    '云南省': '云南', '西藏自治区': '西藏', '陕西省': '陕西', '甘肃省': '甘肃',
                    '青海省': '青海', '宁夏回族自治区': '宁夏', '新疆维吾尔自治区': '新疆',
                    '台湾省': '台湾', '香港特别行政区': '香港', '澳门特别行政区': '澳门'
                }
                
                # 转换为前端需要的格式
                province_data_list = []
                for province, volume in province_volumes.items():
                    mapped_name = province_name_mapping.get(province, province)
                    province_data_list.append({
                        'name': mapped_name,
                        'value': round(volume / 10000, 2)
                    })
                
                # 按成交量降序排序并取前10
                province_data_list.sort(key=lambda x: x['value'], reverse=True)
                buyer_province_sales = [(item['name'], item['value'] * 10000) for item in province_data_list[:10]]
            
            # 获取按二级单位的销售TOP10
            unit_sales = connection.execute(text("""
                SELECT p.secondary_unit, COALESCE(SUM(CAST(n.sold_quantity AS DECIMAL(15,2))), 0) as sales
                FROM projects p 
                LEFT JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
                WHERE n.sold_quantity IS NOT NULL AND n.sold_quantity != '' 
                AND p.secondary_unit IS NOT NULL
                GROUP BY p.secondary_unit 
                ORDER BY sales DESC 
                LIMIT 10
            """)).fetchall()
            
            # 获取成交量TOP10买方
            volume_top10 = connection.execute(text("""
                SELECT
                    customer_name,
                    SUM(total_quantity) as total_quantity,
                    CASE
                        WHEN SUM(total_quantity) > 0 THEN SUM(total_amount) / SUM(total_quantity)
                        ELSE 0
                    END as avg_price
                FROM (
                    -- 绿证交易平台 - 单向挂牌
                    SELECT
                        ul.member_name as customer_name,
                        CAST(ul.total_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(ul.total_amount AS DECIMAL(15,2)) as total_amount
                    FROM gzpt_unilateral_listings ul
                    WHERE ul.total_quantity IS NOT NULL AND ul.total_quantity > 0 AND ul.order_status = '1'

                    UNION ALL

                    -- 绿证交易平台 - 双边线下
                    SELECT
                        off.member_name as customer_name,
                        CAST(off.total_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(off.total_amount AS DECIMAL(15,2)) as total_amount
                    FROM gzpt_bilateral_offline_trades off
                    WHERE off.total_quantity IS NOT NULL AND off.total_quantity > 0 AND off.order_status = '3'

                    UNION ALL
                    
                    -- 绿证交易平台 - 双边线上 (如有)
                    SELECT
                        ol.member_name as customer_name,
                        CAST(ol.total_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(ol.total_amount AS DECIMAL(15,2)) as total_amount
                    FROM gzpt_bilateral_online_trades ol
                    WHERE ol.total_quantity IS NOT NULL AND ol.total_quantity > 0

                    UNION ALL

                    -- 北京电力交易中心
                    SELECT
                        bj.buyer_entity_name as customer_name,
                        CAST(bj.transaction_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(bj.transaction_quantity AS DECIMAL(15,2)) * CAST(bj.transaction_price AS DECIMAL(15,2)) as total_amount
                    FROM beijing_power_exchange_trades bj
                    WHERE bj.transaction_quantity IS NOT NULL AND bj.transaction_quantity > 0

                    UNION ALL

                    -- 广州电力交易中心
                    SELECT
                        gz.buyer_entity_name as customer_name,
                        CAST(gz.gpc_certifi_num AS DECIMAL(15,2)) as total_quantity,
                        CAST(gz.total_cost AS DECIMAL(15,2)) as total_amount
                    FROM guangzhou_power_exchange_trades gz
                    WHERE gz.gpc_certifi_num IS NOT NULL AND gz.gpc_certifi_num > 0
                ) as all_trades
                WHERE customer_name IS NOT NULL AND customer_name != '未知客户'
                GROUP BY customer_name
                HAVING total_quantity > 0
                ORDER BY total_quantity DESC
                LIMIT 10
            """)).fetchall()
            
            # 获取成交价TOP10
            price_top10 = connection.execute(text("""
                SELECT 
                    buyer_name,
                    seller_name,
                    total_quantity,
                    unit_price,
                    platform
                FROM (
                    -- 广州电力交易中心
                    SELECT 
                        buyer_entity_name as buyer_name,
                        COALESCE(p.secondary_unit, '未知单位') as seller_name,
                        CAST(gpc_certifi_num AS DECIMAL(15,2)) as total_quantity,
                        CAST(unit_price AS DECIMAL(10,2)) as unit_price,
                        '广交平台' as platform
                    FROM guangzhou_power_exchange_trades gz
                    LEFT JOIN projects p ON gz.project_id = p.id
                    WHERE gpc_certifi_num IS NOT NULL AND gpc_certifi_num >= 100 
                    AND unit_price IS NOT NULL AND unit_price > 0
                    
                    UNION ALL
                    
                    -- 绿证交易平台 - 双边线下
                    SELECT 
                        member_name as buyer_name,
                        COALESCE(p.secondary_unit, '双边交易') as seller_name,
                        CAST(total_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(total_amount / total_quantity AS DECIMAL(10,2)) as unit_price,
                        '绿证平台-双边' as platform
                    FROM gzpt_bilateral_offline_trades off
                    LEFT JOIN projects p ON off.project_id = p.id
                    WHERE total_quantity IS NOT NULL AND total_quantity >= 100 
                    AND total_amount IS NOT NULL AND total_amount > 0
                    AND order_status = '3'
                    
                    UNION ALL
                    
                    -- 北京电力交易中心
                    SELECT 
                        buyer_entity_name as buyer_name,
                        COALESCE(p.secondary_unit, '北交平台') as seller_name,
                        CAST(transaction_quantity AS DECIMAL(15,2)) as total_quantity,
                        CAST(transaction_price AS DECIMAL(10,2)) as unit_price,
                        '北交平台' as platform
                    FROM beijing_power_exchange_trades bj
                    LEFT JOIN projects p ON bj.project_id = p.id
                    WHERE transaction_quantity IS NOT NULL AND transaction_quantity >= 100 
                    AND transaction_price IS NOT NULL AND transaction_price > 0
                ) as all_trades
                ORDER BY unit_price DESC
                LIMIT 10
            """)).fetchall()
            
            # 获取近6个月的趋势数据
            trend_data = connection.execute(text("""
                SELECT 
                    DATE_FORMAT(STR_TO_DATE(production_year_month, '%Y%m'), '%m月') as month_name,
                    COALESCE(SUM(CAST(shelf_load AS DECIMAL(15,2))), 0) / 10000 as issued,
                    COALESCE(SUM(CAST(sold_quantity AS DECIMAL(15,2))), 0) / 10000 as sold
                FROM nyj_green_certificate_ledger 
                WHERE production_year_month IS NOT NULL 
                AND production_year_month REGEXP '^[0-9]{6}$'
                AND production_year_month >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 6 MONTH), '%Y%m')
                GROUP BY production_year_month 
                ORDER BY production_year_month 
                LIMIT 6
            """)).fetchall()
            
            # 获取主要项目信息表数据
            main_projects = connection.execute(text("""
                SELECT 
                    p.project_name,
                    p.province,
                    p.secondary_unit,
                    p.power_type,
                    COALESCE(SUM(CAST(n.shelf_load AS DECIMAL(15,2))), 0) as issued,
                    COALESCE(SUM(CAST(n.sold_quantity AS DECIMAL(15,2))), 0) as sold,
                    COALESCE(AVG(CASE 
                        WHEN n.sold_quantity > 0 AND CAST(n.sold_quantity AS DECIMAL) > 0 
                        THEN CAST(n.tra_quantity AS DECIMAL) / CAST(n.sold_quantity AS DECIMAL) 
                        ELSE NULL 
                    END), 0) as avg_price
                FROM projects p 
                LEFT JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
                WHERE p.project_name IS NOT NULL
                GROUP BY p.id, p.project_name, p.province, p.secondary_unit, p.power_type
                ORDER BY issued DESC 
                LIMIT 10
            """)).fetchall()
        
        # 处理和格式化数据
        stats = {
            'total_projects': total_projects,
            'total_issued': round(total_issued, 1),
            'total_sold': round(total_sold, 1),
            'avg_price': round(avg_price, 1)
        }
        
        # 处理省份销售数据
        top_provinces = [
            {'name': row[0], 'value': round(float(row[1])/10000, 1)} 
            for row in province_sales
        ] if province_sales else []
        
        # 处理买方省份数据
        top_buyer_provinces = [
            {'name': row[0], 'value': round(float(row[1])/10000, 1)} 
            for row in buyer_province_sales
        ] if buyer_province_sales else []
        
        # 处理二级单位数据
        top_secondary_units = [
            {'name': row[0], 'value': round(float(row[1])/10000, 1)} 
            for row in unit_sales
        ] if unit_sales else []
        
        # 处理成交量TOP10数据
        top_volume_trades = [
            {
                'buyer': row[0] or '未知',
                'quantity': round(float(row[1]), 0) if row[1] else 0,
                'price': round(float(row[2]), 1) if row[2] else 0
            }
            for row in volume_top10
        ] if volume_top10 else []
        
        # 处理成交价TOP10数据
        top_price_trades = [
            {
                'buyer': row[0] or '未知',
                'seller': row[1] or '未知',
                'quantity': round(float(row[2]), 0) if row[2] else 0,
                'price': round(float(row[3]), 1) if row[3] else 0,
                'platform': row[4] or '未知'
            }
            for row in price_top10
        ] if price_top10 else []
        
        # 处理趋势数据
        trend_labels = [row[0] for row in trend_data] if trend_data else ['01月','02月','03月','04月','05月','06月']
        trend_issued = [round(float(row[1]), 1) for row in trend_data] if trend_data else [0, 0, 0, 0, 0, 0]
        trend_sold = [round(float(row[2]), 1) for row in trend_data] if trend_data else [0, 0, 0, 0, 0, 0]
        
        trend = {
            'labels': trend_labels,
            'issued': trend_issued,
            'sold': trend_sold
        }
        
        # 处理主要项目数据
        main_projects_list = []
        for row in main_projects:
            main_projects_list.append({
                'project_name': row[0],
                'province': row[1],
                'secondary_unit': row[2],
                'power_type': row[3],
                'total_issued': round(float(row[4])/10000, 1),
                'total_sold': round(float(row[5])/10000, 1),
            })
        
        # 组装最终数据
        cached_data = {
            'stats': stats,
            'top_provinces': top_provinces,
            'top_buyer_provinces': top_buyer_provinces,
            'top_secondary_units': top_secondary_units,
            'top_volume_trades': top_volume_trades,
            'top_price_trades': top_price_trades,
            'trend': trend,
            'main_projects': main_projects_list,
            'calculated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 更新缓存
        dashboard_cache['data'] = cached_data
        dashboard_cache['timestamp'] = time.time()
        
        print(f"[{datetime.now()}] Dashboard数据计算完成")
        return cached_data
        
    except Exception as e:
        print(f"[{datetime.now()}] Dashboard数据计算出错: {str(e)}")
        return dashboard_cache['data']  # 返回旧数据

def force_refresh_cache():
    """强制刷新缓存"""
    dashboard_cache['timestamp'] = None
    return calculate_dashboard_data()

def get_cache_info():
    """获取缓存信息"""
    return {
        'is_valid': is_cache_valid(),
        'last_updated': datetime.fromtimestamp(dashboard_cache['timestamp']).strftime('%Y-%m-%d %H:%M:%S') if dashboard_cache['timestamp'] else None,
        'cache_duration_minutes': dashboard_cache['cache_duration'] // 60,
        'is_calculating': dashboard_cache['is_calculating']
    }