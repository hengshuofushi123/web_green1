from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, jsonify
from flask_login import login_required, current_user
from models import db, Project, User, FAQ
from sqlalchemy import text, func, desc, or_, and_
from decimal import Decimal
import calendar
import io
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from utils import generate_random_password, update_pwd_excel, project_to_dict, populate_project_from_form
from config import TABLE_HEADER_ORDERS

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')


@dashboard_bp.route('/')
@login_required
def overview():
    """渲染数据概览仪表盘主页，提供真实的统计数据"""
    
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
        
        # 【修正】获取平均成交价（加权平均）
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
                
                -- 如果未来要加入北京电力交易中心，可以在这里添加 UNION ALL
                -- UNION ALL
                -- SELECT
                --    CAST(transaction_quantity AS DECIMAL(15,2)) as total_quantity,
                --    CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,2)) as total_amount
                -- FROM beijing_power_exchange_trades
                -- WHERE transaction_quantity IS NOT NULL AND transaction_quantity > 0 AND transaction_price IS NOT NULL
                
            ) as all_trades;
        """))
        avg_price = float(avg_price_result.scalar() or 0)  # 默认值如果没有数据
        
        # 获取按省份的销售TOP5
        province_sales = connection.execute(text("""
            SELECT province, COALESCE(SUM(CAST(sold_quantity AS DECIMAL(15,2))), 0) as sales
            FROM nyj_green_certificate_ledger 
            WHERE sold_quantity IS NOT NULL AND sold_quantity != '' AND province IS NOT NULL
            GROUP BY province 
            ORDER BY sales DESC 
            LIMIT 5
        """)).fetchall()
        
        # 获取按二级单位的销售TOP5（从项目表关联）
        unit_sales = connection.execute(text("""
            SELECT p.secondary_unit, COALESCE(SUM(CAST(n.sold_quantity AS DECIMAL(15,2))), 0) as sales
            FROM projects p 
            LEFT JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
            WHERE n.sold_quantity IS NOT NULL AND n.sold_quantity != '' 
            AND p.secondary_unit IS NOT NULL
            GROUP BY p.secondary_unit 
            ORDER BY sales DESC 
            LIMIT 5
        """)).fetchall()
        
        # 获取成交量TOP5买方（按买方汇总成交量）
        volume_top10 = connection.execute(text("""
            SELECT 
                buyer_name,
                total_quantity,
                avg_price
            FROM (
                SELECT 
                    CASE 
                        WHEN bj.buyer_entity_name IS NOT NULL THEN bj.buyer_entity_name
                        WHEN gz.buyer_entity_name IS NOT NULL THEN gz.buyer_entity_name
                        WHEN ul.member_name IS NOT NULL THEN ul.member_name
                        WHEN ol.member_name IS NOT NULL THEN ol.member_name
                        WHEN off.member_name IS NOT NULL THEN off.member_name
                        ELSE '未知客户'
                    END as buyer_name,
                    -- 总成交量
                    COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_quantity,
                    -- 成交均价
                    CASE 
                        WHEN (COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0)) > 0
                        THEN (COALESCE(SUM(CAST(ul.total_amount AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(ol.total_amount AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(off.total_amount AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2)) * CAST(bj.transaction_price AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(gz.total_cost AS DECIMAL(15,2))), 0)) / 
                             (COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                              COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0))
                        ELSE 0 
                    END as avg_price
                FROM projects p 
                JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
                LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
                LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym
                LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym
                LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month
                LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND n.production_year_month = gz.product_date
                GROUP BY buyer_name
                HAVING total_quantity > 0
            ) as buyer_summary
            ORDER BY total_quantity DESC
            LIMIT 5
        """)).fetchall()
        
        # 获取成交价TOP5（显示买方/卖方/量/价格）
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
                
                -- 绿证交易平台 - 单向挂牌
                SELECT 
                    member_name as buyer_name,
                    COALESCE(p.secondary_unit, '平台挂牌') as seller_name,
                    CAST(total_quantity AS DECIMAL(15,2)) as total_quantity,
                    CAST(total_amount / total_quantity AS DECIMAL(10,2)) as unit_price,
                    '绿证平台-单向挂牌' as platform
                FROM gzpt_unilateral_listings ul
                LEFT JOIN projects p ON ul.project_id = p.id
                WHERE total_quantity IS NOT NULL AND total_quantity >= 100 
                AND total_amount IS NOT NULL AND total_amount > 0
                AND order_status = '1'
                
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
            LIMIT 5
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
    
    # 准备数据传递给模板
    stats = {
        'total_projects': total_projects,
        'total_issued': round(total_issued, 1),
        'total_sold': round(total_sold, 1),
        'avg_price': round(avg_price, 1)
    }
    
    # 处理省份销售数据
    province_data = {
        'labels': [row[0] for row in province_sales] if province_sales else ['暂无数据'],
        'data': [round(float(row[1])/10000, 1) for row in province_sales] if province_sales else [0]
    }
    
    # 处理二级单位销售数据
    unit_data = {
        'labels': [row[0] for row in unit_sales] if unit_sales else ['暂无数据'],
        'data': [round(float(row[1])/10000, 1) for row in unit_sales] if unit_sales else [0]
    }
    
    # 处理趋势数据
    trend_labels = [row[0] for row in trend_data] if trend_data else ['01月','02月','03月','04月','05月','06月']
    trend_issued = [round(float(row[1]), 1) for row in trend_data] if trend_data else [0, 0, 0, 0, 0, 0]
    trend_sold = [round(float(row[2]), 1) for row in trend_data] if trend_data else [0, 0, 0, 0, 0, 0]
    
    # 准备Top10列表数据
    top_provinces = [
        {'name': row[0], 'value': round(float(row[1])/10000, 1)} 
        for row in province_sales
    ] if province_sales else []
    
    top_secondary_units = [
        {'name': row[0], 'value': round(float(row[1])/10000, 1)} 
        for row in unit_sales
    ] if unit_sales else []
    
    # 准备成交量Top5数据（按买方汇总）
    top_volume_trades = [
        {
            'buyer': row[0] or '未知',
            'quantity': round(float(row[1]), 0) if row[1] else 0,
            'price': round(float(row[2]), 1) if row[2] else 0
        }
        for row in volume_top10
    ] if volume_top10 else []
    
    # 准备成交价Top5数据
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
    
    # 准备趋势数据
    trend = {
        'labels': trend_labels,
        'issued': trend_issued,
        'sold': trend_sold
    }
    
    # 将main_projects转换为字典列表并添加总数计算
    main_projects_list = []
    for row in main_projects:
        main_projects_list.append({
            'project_name': row[0],
            'province': row[1],
            'secondary_unit': row[2],
            'power_type': row[3],
            'total_issued': round(float(row[4])/10000, 1),  # 转万张
            'total_sold': round(float(row[5])/10000, 1),    # 转万张
        })
    
    return render_template('dashboard.html',
                         total_projects=total_projects,
                         total_issued=round(total_issued, 1),
                         total_sold=round(total_sold, 1),
                         avg_price=round(avg_price, 1),
                         top_provinces=top_provinces,
                         top_secondary_units=top_secondary_units,
                         top_volume_trades=top_volume_trades,
                         top_price_trades=top_price_trades,
                         trend=trend,
                         main_projects=main_projects_list)


@dashboard_bp.route('/projects')
@login_required
def projects():
    """
    渲染项目管理页面，嵌入原版首页功能，包含搜索、高级筛选、增删改查等功能
    """
    is_admin = current_user.is_admin

    # 为筛选下拉菜单准备数据 - 修改二级单位为按拼音首字母排序
    secondary_units_query = db.session.query(
        Project.secondary_unit
    ).group_by(Project.secondary_unit).all()
    
    # 使用自定义排序键函数，按照汉字拼音首字母排序
    def get_pinyin_sort_key(s):
        # 简单的拼音首字母排序规则
        # 对于常见的二级单位名称进行硬编码排序
        pinyin_map = {
            '上海电力': 'shanghai',
            '上海能科': 'shanghai',
            '东北公司': 'dongbei',
            '中国电力': 'zhongguo',
            '云南国际': 'yunnan',
            '五凌电力': 'wuling',
            '内蒙古公司': 'neimenggu',
            '北京公司': 'beijing',
            '吉电股份': 'jidian',
            '四川公司': 'sichuan',
            '国能生物': 'guoneng',
            '安徽公司': 'anhui',
            '山东能源': 'shandong',
            '山西公司': 'shanxi',
            '工程公司': 'gongcheng',
            '广东公司': 'guangdong',
            '广西公司': 'guangxi',
            '新疆能源化工': 'xinjiang',
            '江苏公司': 'jiangsu',
            '江西公司': 'jiangxi',
            '河南公司': 'henan',
            '浙江公司': 'zhejiang',
            '海南公司': 'hainan',
            '湖北公司': 'hubei',
            '湖南公司': 'hunan',
            '甘肃公司': 'gansu',
            '福建公司': 'fujian',
            '贵州公司': 'guizhou',
            '重庆公司': 'chongqing',
            '陕西公司': 'shaanxi',
            '黄河公司': 'huanghe',
            '黑龙江公司': 'heilongjiang'
        }
        # 如果在映射表中，直接返回对应的拼音
        if s in pinyin_map:
            return pinyin_map[s]
        # 否则返回原字符串（英文字符会自然排序）
        return s
        
    secondary_units = sorted([r[0] for r in secondary_units_query if r[0]], key=get_pinyin_sort_key)
    provinces = db.session.query(Project.province).distinct().order_by(Project.province).all()
    regions = db.session.query(Project.region).distinct().order_by(Project.region).all()
    investment_scopes = db.session.query(Project.investment_scope).distinct().order_by(Project.investment_scope).all()
    project_natures = db.session.query(Project.project_nature).distinct().order_by(Project.project_nature).all()
    power_types = db.session.query(Project.power_type).distinct().order_by(Project.power_type).all()

    filter_options = {
        'secondary_units': secondary_units,
        'provinces': [r[0] for r in provinces if r[0]],
        'regions': [r[0] for r in regions if r[0]],
        'investment_scopes': [r[0] for r in investment_scopes if r[0]],
        'project_natures': [r[0] for r in project_natures if r[0]],
        'power_types': [r[0] for r in power_types if r[0]],
    }

    # 获取当前激活的筛选条件
    active_filters = {
        'query': request.args.get('query', ''),
        'secondary_unit': request.args.get('secondary_unit', ''),
        'province': request.args.get('province', ''),
        'region': request.args.get('region', ''),
        'investment_scope': request.args.get('investment_scope', ''),
        'project_nature': request.args.get('project_nature', ''),
        'power_type': request.args.get('power_type', ''),
        'is_uhv_support': request.args.get('is_uhv_support', ''),
        'has_subsidy': request.args.get('has_subsidy', ''),
    }



    query_builder = Project.query

    if not is_admin:
        query_builder = query_builder.filter(Project.secondary_unit == current_user.username)

    # 应用筛选条件
    if active_filters['query']:
        query_builder = query_builder.filter(Project.project_name.like(f"%{active_filters['query']}%"))
    if active_filters['secondary_unit'] and is_admin:
        query_builder = query_builder.filter_by(secondary_unit=active_filters['secondary_unit'])
    if active_filters['province']:
        query_builder = query_builder.filter_by(province=active_filters['province'])
    if active_filters['region']:
        query_builder = query_builder.filter_by(region=active_filters['region'])
    if active_filters['investment_scope']:
        query_builder = query_builder.filter_by(investment_scope=active_filters['investment_scope'])
    if active_filters['project_nature']:
        query_builder = query_builder.filter_by(project_nature=active_filters['project_nature'])
    if active_filters['power_type']:
        query_builder = query_builder.filter_by(power_type=active_filters['power_type'])
    if active_filters['is_uhv_support'] in ['1', '0']:
        query_builder = query_builder.filter_by(is_uhv_support=(active_filters['is_uhv_support'] == '1'))
    if active_filters['has_subsidy'] in ['1', '0']:
        query_builder = query_builder.filter_by(has_subsidy=(active_filters['has_subsidy'] == '1'))

    projects = query_builder.order_by(Project.id).all()

    return render_template('projects.html',
                           projects=projects,
                           filter_options=filter_options,
                           active_filters=active_filters,
                           is_admin=is_admin)


# 项目管理路由
@dashboard_bp.route('/projects/add', methods=['GET', 'POST'])
@login_required
def add_project():
    """新增项目"""
    is_admin = current_user.is_admin
    if request.method == 'POST':
        form_data = request.form.to_dict()
        if not form_data.get('project_name'):
            flash('项目名称是必填项！')
            return redirect(url_for('dashboard.projects'))

        secondary_unit = form_data.get('secondary_unit') if is_admin else current_user.username
        if not secondary_unit:
            flash('二级单位是必填项！')
            return redirect(url_for('dashboard.projects'))

        existing_project = Project.query.filter_by(project_name=form_data['project_name']).first()
        if existing_project:
            flash('项目名称已存在！')
            return redirect(url_for('dashboard.projects'))

        new_project = Project()
        form_data['secondary_unit'] = secondary_unit
        new_project = populate_project_from_form(new_project, form_data)

        db.session.add(new_project)
        db.session.commit()

        if User.query.filter_by(username=secondary_unit).first() is None:
            new_user = User(username=secondary_unit)
            pw = generate_random_password()
            new_user.set_password(pw)
            db.session.add(new_user)
            db.session.commit()
            update_pwd_excel(secondary_unit, pw)

        flash('项目已成功新增！')
        return redirect(url_for('dashboard.projects'))
    else:
        return render_template('project_form.html', project={}, action='Add', is_admin=is_admin)


@dashboard_bp.route('/projects/edit/<int:project_id>', methods=['GET', 'POST'])
@login_required
def edit_project(project_id):
    """编辑项目"""
    is_admin = current_user.is_admin
    project = Project.query.get_or_404(project_id)
    if not is_admin and project.secondary_unit != current_user.username:
        flash('您没有权限编辑此项目！')
        return redirect(url_for('dashboard.projects'))

    if request.method == 'POST':
        form_data = request.form.to_dict()
        if not form_data.get('project_name') or not form_data.get('secondary_unit'):
            flash('项目名称和二级单位是必填项！')
            return redirect(url_for('dashboard.projects'))

        old_secondary_unit = project.secondary_unit
        secondary_unit = form_data.get('secondary_unit') if is_admin else current_user.username
        form_data['secondary_unit'] = secondary_unit
        project = populate_project_from_form(project, form_data)
        db.session.commit()

        if secondary_unit != old_secondary_unit:
            if User.query.filter_by(username=secondary_unit).first() is None:
                new_user = User(username=secondary_unit)
                pw = generate_random_password()
                new_user.set_password(pw)
                db.session.add(new_user)
                db.session.commit()
                update_pwd_excel(secondary_unit, pw)

        flash('项目已成功更新！')
        return redirect(url_for('dashboard.projects'))

    project_dict = project_to_dict(project)
    return render_template('project_form.html', project=project_dict, action='Edit', is_admin=is_admin)


@dashboard_bp.route('/projects/get/<int:project_id>', methods=['GET'])
@login_required
def get_project(project_id):
    project = Project.query.get_or_404(project_id)
    if not current_user.is_admin and project.secondary_unit != current_user.username:
        return jsonify({'error': 'No permission'}), 403
    return jsonify(project_to_dict(project))

@dashboard_bp.route('/projects/delete/<int:project_id>', methods=['POST'])
@login_required
def delete_project(project_id):
    """删除项目"""
    project = Project.query.get_or_404(project_id)
    if not current_user.is_admin and project.secondary_unit != current_user.username:
        flash('您没有权限删除此项目！')
        return redirect(url_for('dashboard.projects'))
    db.session.delete(project)
    db.session.commit()
    flash('项目已成功删除！')
    return redirect(url_for('dashboard.projects'))


@dashboard_bp.route('/projects/export')
@login_required
def export_projects():
    """导出项目Excel"""
    is_admin = current_user.is_admin
    query_builder = Project.query
    if not is_admin:
        query_builder = query_builder.filter(Project.secondary_unit == current_user.username)
    
    projects = query_builder.all()
    data = []
    
    for p in projects:
        data.append({
            '项目ID': p.id, '项目名称': p.project_name, '二级单位': p.secondary_unit,
            '二级单位联系人': p.secondary_unit_contact, '项目所在省份': p.province,
            '项目所在区域': p.region, '公司名称': p.company_name, '项目投资口径': p.investment_scope,
            '项目性质': p.project_nature, '电源品种': p.power_type,
            '是否特高压配套电源': '是' if p.is_uhv_support else '否',
            '是否含补贴': '是' if p.has_subsidy else '否',
            '装机容量(万千瓦)': p.capacity_mw, '投产年份': p.production_year,
            '投产月份': p.production_month, '是否建档立卡': '是' if p.is_filed else '否',
            '是否完成北交注册': '是' if p.is_beijing_registered else '否',
            '是否完成广交注册': '是' if p.is_guangzhou_registered else '否',
            '核对及更新日期': p.last_updated_date.strftime('%Y-%m-%d') if p.last_updated_date else ''
        })

    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Projects')
    output.seek(0)

    return send_file(output, as_attachment=True, download_name='projects.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@dashboard_bp.route('/projects/import', methods=['POST'])
@login_required
def import_excel():
    """导入项目Excel"""
    if not current_user.is_admin:
        flash('只有管理员可以导入Excel文件！')
        return redirect(url_for('dashboard.projects'))

    file = request.files.get('file')
    if not file or not file.filename.endswith(('.xlsx', '.xls')):
        flash('请上传有效的Excel文件 (.xlsx, .xls)')
        return redirect(url_for('dashboard.projects'))

    try:
        df = pd.read_excel(file, sheet_name='Sheet1', dtype=str)
        df.columns = df.columns.str.strip()

        column_map = {
            '核对及更新日期': 'last_updated_date', '二级单位': 'secondary_unit',
            '二级单位联系人': 'secondary_unit_contact', '项目所在省份': 'province',
            '项目所在区域': 'region', '项目名称': 'project_name',
            '公司名称': 'company_name', '项目投资口径': 'investment_scope',
            '项目性质': 'project_nature', '电源品种': 'power_type',
            '是否特高压配套电源': 'is_uhv_support', '是否含补贴': 'has_subsidy',
            '装机容量\n（万千瓦）': 'capacity_mw', '装机容量（万千瓦）': 'capacity_mw',
            '（预计）投产年份': 'production_year', '（预计）投产月份': 'production_month',
            '是否建档立卡': 'is_filed', '是否完成北交注册': 'is_beijing_registered',
            '是否完成广交注册': 'is_guangzhou_registered'
        }

        # 处理布尔值字段
        bool_columns = ['is_uhv_support', 'has_subsidy', 'is_filed', 'is_beijing_registered', 'is_guangzhou_registered']

        # 更新或创建项目
        updated_count = 0
        created_count = 0

        for _, row in df.iterrows():
            project_name = row.get('项目名称')
            if not project_name:
                continue

            # 查找现有项目或创建新项目
            project = Project.query.filter_by(project_name=project_name).first()
            is_new = False
            if not project:
                project = Project(project_name=project_name)
                is_new = True

            # 更新项目字段
            for excel_col, db_col in column_map.items():
                if excel_col in row and pd.notna(row[excel_col]):
                    value = row[excel_col]
                    if db_col in bool_columns:
                        value = value.strip() in ['是', '1', 'True', 'true', 'Yes', 'yes']
                    elif db_col == 'capacity_mw' and value:
                        try:
                            value = float(value.replace(',', ''))
                        except (ValueError, TypeError):
                            value = None
                    elif db_col == 'last_updated_date' and value:
                        try:
                            if isinstance(value, str):
                                value = datetime.strptime(value, '%Y-%m-%d')
                        except ValueError:
                            value = datetime.now()
                    setattr(project, db_col, value)

            # 保存项目
            if is_new:
                db.session.add(project)
                created_count += 1
            else:
                updated_count += 1

        db.session.commit()
        flash(f'Excel导入成功！更新了{updated_count}个项目，新增了{created_count}个项目。')
    except Exception as e:
        db.session.rollback()
        flash(f'Excel导入失败：{str(e)}')

    return redirect(url_for('dashboard.projects'))


@dashboard_bp.route('/data_tables')
@login_required
def data_tables():
    is_admin = current_user.is_admin
    selected_unit = request.args.get('secondary_unit', '')
    selected_project_id = request.args.get('project_id', '')
    selected_table = request.args.get('table', 'nyj_ledger')
    page = request.args.get('page', 1, type=int)
    per_page = 100
    show_all = request.args.get('show_all', 'false').lower() in ('true', '1', 'on', 'yes')
    
    # 获取所有二级单位
    if is_admin:
        secondary_units_query = db.session.query(
            Project.secondary_unit
        ).group_by(Project.secondary_unit).all()
        # 按照拼音首字母排序二级单位
        # 使用自定义排序键函数，按照汉字拼音首字母排序
        def get_pinyin_sort_key(s):
            # 简单的拼音首字母排序规则
            # 对于常见的二级单位名称进行硬编码排序
            pinyin_map = {
                '上海电力': 'shanghai',
                '上海能科': 'shanghai',
                '东北公司': 'dongbei',
                '中国电力': 'zhongguo',
                '云南国际': 'yunnan',
                '五凌电力': 'wuling',
                '内蒙古公司': 'neimenggu',
                '北京公司': 'beijing',
                '吉电股份': 'jidian',
                '四川公司': 'sichuan',
                '国能生物': 'guoneng',
                '安徽公司': 'anhui',
                '山东能源': 'shandong',
                '山西公司': 'shanxi',
                '工程公司': 'gongcheng',
                '广东公司': 'guangdong',
                '广西公司': 'guangxi',
                '新疆能源化工': 'xinjiang',
                '江苏公司': 'jiangsu',
                '江西公司': 'jiangxi',
                '河南公司': 'henan',
                '浙江公司': 'zhejiang',
                '海南公司': 'hainan',
                '湖北公司': 'hubei',
                '湖南公司': 'hunan',
                '甘肃公司': 'gansu',
                '福建公司': 'fujian',
                '贵州公司': 'guizhou',
                '重庆公司': 'chongqing',
                '陕西公司': 'shaanxi',
                '黄河公司': 'huanghe',
                '黑龙江公司': 'heilongjiang'
            }
            # 如果在映射表中，直接返回对应的拼音
            if s in pinyin_map:
                return pinyin_map[s]
            # 否则返回原字符串（英文字符会自然排序）
            return s
            
        secondary_units = sorted([r[0] for r in secondary_units_query if r[0]], key=get_pinyin_sort_key)
    else:
        secondary_units = [current_user.username]
    
    # 如果没有选择单位，默认选择第一个
    if not selected_unit and secondary_units:
        selected_unit = secondary_units[0]
    
    # 获取选定单位的可用项目
    if is_admin:
        projects_query = Project.query.filter_by(secondary_unit=selected_unit).order_by(Project.project_name)
    else:
        projects_query = Project.query.filter_by(secondary_unit=current_user.username).order_by(Project.project_name)
    projects = projects_query.all()
    
    # 如果没有选择项目，默认选择第一个
    if not selected_project_id and projects:
        selected_project_id = str(projects[0].id)
    
    # 如果选择了项目，获取数据
    data = []
    pagination = None
    comments = {}
    if selected_project_id:
        project_id = int(selected_project_id)
        table_map = {
            'nyj_ledger': 'nyj_green_certificate_ledger',
            'nyj_transactions': 'nyj_transaction_records',
            'gzpt_unilateral': 'gzpt_unilateral_listings',
            'gzpt_online': 'gzpt_bilateral_online_trades',
            'gzpt_offline': 'gzpt_bilateral_offline_trades',
            'beijing_trades': 'beijing_power_exchange_trades',
            'guangzhou_trades': 'guangzhou_power_exchange_trades'
        }
        table_name = table_map.get(selected_table, 'nyj_green_certificate_ledger')
        
        # 获取表注释
        comments_result = db.session.execute(text(f"SHOW FULL COLUMNS FROM {table_name}"))
        comments = {row[0]: row[8] for row in comments_result if row[8]}
        
        # 获取数据并分页
        total = db.session.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE project_id = {project_id}")).scalar()
        pagination = {
            'page': page,
            'pages': (total + per_page - 1) // per_page,
            'has_prev': page > 1,
            'has_next': page < (total + per_page - 1) // per_page,
            'prev_num': page - 1,
            'next_num': page + 1
        }
        
        offset = (page - 1) * per_page
        where_clause = f"WHERE project_id = {project_id}"
        if table_name == 'gzpt_unilateral_listings':
            where_clause += " AND order_status = '1'"
        elif table_name == 'gzpt_bilateral_offline_trades':
            where_clause += " AND order_status = '3'"
        total = db.session.execute(text(f"SELECT COUNT(*) FROM {table_name} {where_clause}")).scalar()
        pagination = {
            'page': page,
            'pages': (total + per_page - 1) // per_page,
            'has_prev': page > 1,
            'has_next': page < (total + per_page - 1) // per_page,
            'prev_num': page - 1,
            'next_num': page + 1
        }
        data_result = db.session.execute(text(f"SELECT * FROM {table_name} {where_clause} LIMIT {per_page} OFFSET {offset}"))
        data = [dict(row) for row in data_result.mappings()]
        header_order = TABLE_HEADER_ORDERS.get(table_name, [])

        if data:
            if not show_all:
                # show_all为false时：只显示有注释的列
                commented_columns = set(comments.keys())
                for row in data:
                    keys_to_remove = [k for k in row.keys() if k not in commented_columns]
                    for k in keys_to_remove:
                        del row[k]
                    for k in list(row.keys()):
                        if k in comments:
                            row[comments[k]] = row.pop(k)
            else:
                # show_all为true时：显示所有列
                for row in data:
                    for k in list(row.keys()):
                        if k in comments:
                            row[comments[k]] = row.pop(k)

        # 计算单价和格式化金额 - 在字段重命名之后执行
        if selected_table in ['gzpt_unilateral', 'gzpt_online', 'gzpt_offline']:
            for row in data:
                if selected_table in ['gzpt_unilateral', 'gzpt_offline', 'gzpt_online']:
                    quantity_key = '交易量（张）'  # 使用重命名后的中文字段名
                    amount_key = '金额'           # 所有表的金额字段注释都是'金额'
                
                quantity = row.get(quantity_key)
                amount = row.get(amount_key)
                if quantity is not None and amount is not None and quantity != 0:
                    try:
                        row['单价'] = round(float(amount) / float(quantity), 2)
                    except (ValueError, TypeError):
                        row['单价'] = ''
                else:
                    row['单价'] = ''
                
                # 格式化金额字段
                for key in list(row.keys()):
                    if 'amount' in key.lower() or '金额' in key:
                        try:
                            row[key] = f"{float(row[key]):.2f}" if row[key] else ''
                        except (ValueError, TypeError):
                            pass

        # 应用列排序逻辑 - 对所有表都应用TABLE_HEADER_ORDERS配置
        if data and header_order:
            ordered_data = []
            for row in data:
                ordered_row = {}
                # 首先按header_order顺序添加列
                for col in header_order:
                    if col in row:
                        ordered_row[col] = row[col]
                # 对于绿证交易平台表，确保单价列始终显示（即使不在header_order中）
                if selected_table in ['gzpt_unilateral', 'gzpt_online', 'gzpt_offline']:
                    if '单价' in row and '单价' not in ordered_row:
                        ordered_row['单价'] = row['单价']
                # 如果show_all为true，添加所有剩余的列
                if show_all:
                    for col in row:
                        if col not in ordered_row:
                            ordered_row[col] = row[col]
                ordered_data.append(ordered_row)
            data = ordered_data

        return render_template('data_tables.html',
                             secondary_units=secondary_units,
                             selected_unit=selected_unit,
                             projects=projects,
                             selected_project_id=selected_project_id,
                             selected_table=selected_table,
                             data=data,
                             pagination=pagination,
                             comments=comments,
                             show_all=show_all)


@dashboard_bp.route('/download_table_data')
@login_required
def download_table_data():
    selected_unit = request.args.get('secondary_unit', '')
    selected_project_id = request.args.get('project_id', '')
    selected_table = request.args.get('table', 'nyj_ledger')
    show_all = True  # 始终使用show_all=true逻辑
    
    if not selected_project_id:
        flash('请选择一个项目', 'warning')
        return redirect(url_for('dashboard.data_tables'))
    
    project_id = int(selected_project_id)
    table_map = {
        'nyj_ledger': 'nyj_green_certificate_ledger',
        'nyj_transactions': 'nyj_transaction_records',
        'gzpt_unilateral': 'gzpt_unilateral_listings',
        'gzpt_online': 'gzpt_bilateral_online_trades',
        'gzpt_offline': 'gzpt_bilateral_offline_trades',
        'beijing_trades': 'beijing_power_exchange_trades',
        'guangzhou_trades': 'guangzhou_power_exchange_trades'
    }
    table_name = table_map.get(selected_table, 'nyj_green_certificate_ledger')
    
    # 获取表注释
    comments_result = db.session.execute(text(f"SHOW FULL COLUMNS FROM {table_name}"))
    comments = {row[0]: row[8] for row in comments_result if row[8]}
    
    # 获取所有数据（无分页）
    where_clause = f"WHERE project_id = {project_id}"
    if table_name == 'gzpt_unilateral_listings':
        where_clause += " AND order_status = '1'"
    elif table_name == 'gzpt_bilateral_offline_trades':
        where_clause += " AND order_status = '3'"
    data_result = db.session.execute(text(f"SELECT * FROM {table_name} {where_clause}"))
    data = [dict(row) for row in data_result.mappings()]
    header_order = TABLE_HEADER_ORDERS.get(table_name, [])
    
    if data:
        # 应用show_all=true逻辑：显示所有列，并重命名有注释的列
        for row in data:
            for k in list(row.keys()):
                if k in comments:
                    row[comments[k]] = row.pop(k)
    
        # 计算单价和格式化金额
        if selected_table in ['gzpt_unilateral', 'gzpt_online', 'gzpt_offline']:
            for row in data:
                quantity_key = '交易量（张）'
                amount_key = '金额'
                quantity = row.get(quantity_key)
                amount = row.get(amount_key)
                if quantity is not None and amount is not None and quantity != 0:
                    try:
                        row['单价'] = round(float(amount) / float(quantity), 2)
                    except (ValueError, TypeError):
                        row['单价'] = ''
                else:
                    row['单价'] = ''
                
                for key in list(row.keys()):
                    if 'amount' in key.lower() or '金额' in key:
                        try:
                            row[key] = f"{float(row[key]):.2f}" if row[key] else ''
                        except (ValueError, TypeError):
                            pass
    
        # 应用列排序逻辑
        if header_order:
            ordered_data = []
            for row in data:
                ordered_row = {}
                for col in header_order:
                    if col in row:
                        ordered_row[col] = row[col]
                if selected_table in ['gzpt_unilateral', 'gzpt_online', 'gzpt_offline']:
                    if '单价' in row and '单价' not in ordered_row:
                        ordered_row['单价'] = row['单价']
                for col in row:
                    if col not in ordered_row:
                        ordered_row[col] = row[col]
                ordered_data.append(ordered_row)
            data = ordered_data
    
    # 创建DataFrame
    if data:
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame()
    
    # 生成Excel文件
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    
    # 发送文件
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{selected_table}_data.xlsx'
    )


@dashboard_bp.route('/user-management')
@login_required
def user_management():
    """嵌入用户管理页面内容"""
    if not current_user.is_admin:
        flash('只有管理员可以管理用户！')
        return redirect(url_for('dashboard.overview'))
    users = User.query.filter(User.username != 'admin').order_by(User.username).all()
    return render_template('manage_users.html', users=users)


@dashboard_bp.route('/user-management/reset-password/<username>', methods=['POST'])
@login_required
def reset_password(username):
    """重置用户密码"""
    if not current_user.is_admin:
        flash('只有管理员可以重置密码！')
        return redirect(url_for('dashboard.user_management'))
    
    user = User.query.filter_by(username=username).first()
    if user:
        new_password = generate_random_password()
        user.set_password(new_password)
        db.session.commit()
        update_pwd_excel(username, new_password)
        flash(f'用户 {username} 的密码已重置！')
    else:
        flash('用户不存在！')
    
    return redirect(url_for('dashboard.user_management'))


@dashboard_bp.route('/user-management/download-passwords')
@login_required
def download_passwords():
    """下载密码Excel文件"""
    if not current_user.is_admin:
        flash('只有管理员可以下载密码文件！')
        return redirect(url_for('dashboard.user_management'))
    
    return send_file('pwd.xlsx', as_attachment=True)


@dashboard_bp.route('/user-management/get-password/<username>')
@login_required
def get_password(username):
    """获取用户密码"""
    if not current_user.is_admin:
        return jsonify({'error': '只有管理员可以查看密码！'}), 403
    
    try:
        # 从Excel文件中读取密码
        file = 'pwd.xlsx'
        if os.path.exists(file):
            df = pd.read_excel(file)
            user_row = df[df['username'] == username]
            if not user_row.empty:
                password = user_row['password'].values[0]
                return jsonify({'password': password})
        
        return jsonify({'error': '未找到用户密码！'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@dashboard_bp.route('/statistics')
@login_required
def statistics():
    """渲染统计分析页面，提供真实的汇总计算，默认时间范围为当年1月到当前月"""
    
    # 获取当前日期，设置默认时间范围
    current_date = datetime.now()
    default_start = f"{current_date.year}-01"
    default_end = f"{current_date.year}-{str(current_date.month).zfill(2)}"
    
    # 获取请求参数
    dimension = request.args.get('dimension', '省份')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    
    # 如果时间参数为空，表示不限制时间；否则使用默认值
    if not start_date and not end_date:
        # 不限制时间，保持为空
        pass
    elif not start_date or not end_date:
        # 如果只有一个为空，使用默认值
        start_date = start_date or default_start
        end_date = end_date or default_end
    
    # 获取交易日期筛选参数
    current_date_str = current_date.strftime('%Y-%m-%d')
    transaction_start_date = request.args.get('transaction_start_date', '')
    transaction_end_date = request.args.get('transaction_end_date', '')
    
    # 如果交易时间参数为空，表示不限制交易时间
    if not transaction_start_date and not transaction_end_date:
        # 不限制交易时间，保持为空
        pass
    elif not transaction_start_date or not transaction_end_date:
        # 如果只有一个为空，使用默认值
        transaction_start_date = transaction_start_date or ''
        transaction_end_date = transaction_end_date or current_date_str
    
    summary_data = []
    chart_labels = []
    chart_issued = []
    chart_issued_platform_sold = []
    chart_trading_platform_sold = []
    chart_issued_ratio = []
    chart_avg_price = []
    
    # 添加项目权限控制
    query = Project.query
    # 非管理员只能查看自己的数据
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return render_template('statistics.html',
                             dimension=dimension,
                             start_date=start_date,
                             end_date=end_date,
                             transaction_start_date=transaction_start_date,
                             transaction_end_date=transaction_end_date,
                             summary_data=summary_data,
                             chart_labels=chart_labels,
                             chart_issued=chart_issued,
                             chart_issued_platform_sold=chart_issued_platform_sold,
                             chart_trading_platform_sold=chart_trading_platform_sold,
                             chart_issued_ratio=chart_issued_ratio,
                             chart_avg_price=chart_avg_price)
    
    with db.engine.connect() as connection:
        # 生产期筛选策略：
        # - 如果时间参数为空，表示不限制时间
        # - 如果时间参数有值，按用户选择范围
        # - 初次进入时使用默认值
        if not start_date and not end_date:
            # 不限制生产期，使用极值范围
            start_month = '0000-01'
            end_month = '9999-12'
        elif start_date and end_date:
            start_month = start_date[:7]
            end_month = end_date[:7]
        else:
            # 使用默认值
            start_month = default_start
            end_month = default_end
        
        # 交易期筛选SQL片段
        transaction_filter_clauses = {
            'tr': '', 'ul': '', 'ol': '', 'off': '', 'bj': '', 'gz': ''
        }
        # 只有当交易时间参数都不为空时才添加过滤条件
        if transaction_start_date and transaction_end_date:
            # 假设 transaction_start_date 和 transaction_end_date 是 'YYYY-MM-DD' 格式
            # 为确保覆盖全天，将 end_date 调整为 'YYYY-MM-DD 23:59:59'
            end_date_for_query = f"{transaction_end_date} 23:59:59"
            
            # 【修正】移除了所有字段名前的表别名（如 tr., ul., bj. 等）
            transaction_filter_clauses['tr'] = f"AND transaction_time BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"
            transaction_filter_clauses['ul'] = f"AND order_time_str BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"
            transaction_filter_clauses['ol'] = f"AND order_time_str BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"
            transaction_filter_clauses['off'] = f"AND order_time_str BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"
            transaction_filter_clauses['bj'] = f"AND transaction_time BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"
            transaction_filter_clauses['gz'] = f"AND deal_time BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"


        # 根据聚合维度构建不同的SQL查询
        dimension_column_map = {
            '省份': 'p.province',
            '二级单位': 'p.secondary_unit',
            '电源品种': 'p.power_type',
            '项目性质': 'p.project_nature',
            '项目投资口径': 'p.investment_scope'
        }
        
        dimension_column = dimension_column_map.get(dimension, 'p.region')
        group_by_clause = dimension_column

        # 特殊维度处理
        if dimension == '装机容量(万千瓦)':
            group_by_clause = """
                CASE
                    WHEN p.capacity_mw IS NULL OR p.capacity_mw = 0 THEN '未知'
                    WHEN p.capacity_mw < 5 THEN '<5万千瓦'
                    WHEN p.capacity_mw < 10 THEN '5-10万千瓦'
                    WHEN p.capacity_mw < 20 THEN '10-20万千瓦'
                    WHEN p.capacity_mw < 50 THEN '20-50万千瓦'
                    ELSE '≥50万千瓦'
                END
            """
            dimension_column = group_by_clause
        elif dimension == '是否特高压配套电源':
            group_by_clause = "p.is_uhv_support"
            dimension_column = "CASE WHEN p.is_uhv_support = 1 THEN '是' ELSE '否' END"
        elif dimension == '是否含补贴':
            group_by_clause = "p.has_subsidy"
            dimension_column = "CASE WHEN p.has_subsidy = 1 THEN '是' ELSE '否' END"
        elif dimension == '是否建档立卡':
            group_by_clause = "p.is_filed"
            dimension_column = "CASE WHEN p.is_filed = 1 THEN '是' ELSE '否' END"
        elif dimension == '是否完成北交注册':
            group_by_clause = "p.is_beijing_registered"
            dimension_column = "CASE WHEN p.is_beijing_registered = 1 THEN '是' ELSE '否' END"
        elif dimension == '是否完成广交注册':
            group_by_clause = "p.is_guangzhou_registered"
            dimension_column = "CASE WHEN p.is_guangzhou_registered = 1 THEN '是' ELSE '否' END"

        # 构建新的、正确的SQL查询
        sql = text(f"""
            WITH
            -- CTE for Trading Platform Summaries (unchanged logic)
            ul_summary AS (
                SELECT
                    project_id, generate_ym as production_ym,
                    COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0) as total_qty,
                    COALESCE(SUM(CAST(total_amount AS DECIMAL(15,2))), 0) as total_amt
                FROM gzpt_unilateral_listings
                WHERE project_id IN :project_ids AND order_status = '1' {transaction_filter_clauses['ul']}
                GROUP BY project_id, production_ym
            ),
            off_summary AS (
                SELECT
                    project_id, generate_ym as production_ym,
                    COALESCE(SUM(CAST(total_quantity AS DECIMAL(15,2))), 0) as total_qty,
                    COALESCE(SUM(CAST(total_amount AS DECIMAL(15,2))), 0) as total_amt
                FROM gzpt_bilateral_offline_trades
                WHERE project_id IN :project_ids AND order_status = '3' {transaction_filter_clauses['off']}
                GROUP BY project_id, production_ym
            ),
            bj_summary AS (
                SELECT
                    project_id, production_year_month as production_ym,
                    COALESCE(SUM(CAST(transaction_quantity AS DECIMAL(15,2))), 0) as total_qty,
                    COALESCE(SUM(CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,2))), 0) as total_amt
                FROM beijing_power_exchange_trades
                WHERE project_id IN :project_ids {transaction_filter_clauses['bj']}
                GROUP BY project_id, production_ym
            ),
            gz_summary AS (
                SELECT
                    project_id, CONCAT(SUBSTRING(product_date, 1, 4), '-', LPAD(SUBSTRING(product_date, 6), 2, '0')) as production_ym,
                    COALESCE(SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))), 0) as total_qty,
                    COALESCE(SUM(CAST(total_cost AS DECIMAL(15,2))), 0) as total_amt
                FROM guangzhou_power_exchange_trades
                WHERE project_id IN :project_ids {transaction_filter_clauses['gz']}
                GROUP BY project_id, production_ym
            ),

            -- CTE 1: Ledger-based metrics (ordinary_total, trading_platform_sold, avg_price)
            LedgerMetrics AS (
                SELECT
                    {group_by_clause} as dimension_value,
                    COALESCE(SUM(CASE WHEN n.ordinary_quantity IS NOT NULL AND n.ordinary_quantity != '' THEN CAST(n.ordinary_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as ordinary_total,
                    (COALESCE(SUM(ul.total_qty), 0) + COALESCE(SUM(off.total_qty), 0) + COALESCE(SUM(bj.total_qty), 0) + COALESCE(SUM(gz.total_qty), 0)) as trading_platform_sold,
                    CASE
                        WHEN (COALESCE(SUM(ul.total_qty), 0) + COALESCE(SUM(off.total_qty), 0) + COALESCE(SUM(bj.total_qty), 0) + COALESCE(SUM(gz.total_qty), 0)) > 0
                        THEN (COALESCE(SUM(ul.total_amt), 0) + COALESCE(SUM(off.total_amt), 0) + COALESCE(SUM(bj.total_amt), 0) + COALESCE(SUM(gz.total_amt), 0)) /
                             (COALESCE(SUM(ul.total_qty), 0) + COALESCE(SUM(off.total_qty), 0) + COALESCE(SUM(bj.total_qty), 0) + COALESCE(SUM(gz.total_qty), 0))
                        ELSE 0
                    END as avg_price
                FROM projects p
                JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
                LEFT JOIN ul_summary ul ON p.id = ul.project_id AND n.production_year_month = ul.production_ym
                LEFT JOIN off_summary off ON p.id = off.project_id AND n.production_year_month = off.production_ym
                LEFT JOIN bj_summary bj ON p.id = bj.project_id AND n.production_year_month = bj.production_ym
                LEFT JOIN gz_summary gz ON p.id = gz.project_id AND n.production_year_month = gz.production_ym
                WHERE p.id IN :project_ids
                  AND n.production_year_month >= :start_month
                  AND n.production_year_month <= :end_month
                  AND {group_by_clause} IS NOT NULL
                GROUP BY {group_by_clause}
            ),

            -- CTE 2: Direct query for Issued Platform metrics
            IssuedMetrics AS (
                SELECT
                    {group_by_clause} as dimension_value,
                    COALESCE(SUM(CAST(tr.transaction_num AS DECIMAL(15,2))), 0) as issued_platform_sold
                FROM projects p
                JOIN nyj_transaction_records tr ON p.id = tr.project_id
                WHERE p.id IN :project_ids
                  AND CONCAT(tr.production_year, '-', LPAD(tr.production_month, 2, '0')) >= :start_month
                  AND CONCAT(tr.production_year, '-', LPAD(tr.production_month, 2, '0')) <= :end_month
                  {transaction_filter_clauses['tr']} -- This is the transaction time filter string
                  AND {group_by_clause} IS NOT NULL
                GROUP BY {group_by_clause}
            ),

            -- Master list of all possible dimension values that appear in either calculation
            AllDimensions AS (
                SELECT dimension_value FROM LedgerMetrics
                UNION
                SELECT dimension_value FROM IssuedMetrics
            )

            -- Final SELECT to combine the two metrics
            SELECT
                d.dimension_value,
                COALESCE(lm.ordinary_total, 0) as ordinary_total,
                COALESCE(im.issued_platform_sold, 0) as issued_platform_sold,
                COALESCE(lm.trading_platform_sold, 0) as trading_platform_sold,
                COALESCE(lm.avg_price, 0) as avg_price
            FROM AllDimensions d
            LEFT JOIN LedgerMetrics lm ON d.dimension_value = lm.dimension_value
            LEFT JOIN IssuedMetrics im ON d.dimension_value = im.dimension_value
            ORDER BY ordinary_total DESC
        """)
        
        # 构建SQL查询参数（始终绑定）
        sql_params = {
            'project_ids': tuple(project_ids_list),
            'start_month': start_month,
            'end_month': end_month
        }
        
        
        
        result = connection.execute(sql, sql_params).fetchall()
        
        # 处理查询结果
        for row in result:
            ordinary_total = int(row[1] or 0)
            issued_platform_sold = int(row[2] or 0)
            trading_platform_sold = int(row[3] or 0)
            avg_price = round(float(row[4] or 0), 1)
            
            # 计算售出比例
            issued_ratio = round((issued_platform_sold / ordinary_total * 100), 1) if ordinary_total > 0 else 0
            trading_ratio = round((trading_platform_sold / ordinary_total * 100), 1) if ordinary_total > 0 else 0
            
            summary_data.append({
                'dimension_value': row[0] or '未知',
                'ordinary_total': ordinary_total,
                'issued_platform_sold': issued_platform_sold,
                'trading_platform_sold': trading_platform_sold,
                'issued_ratio': issued_ratio,
                'trading_ratio': trading_ratio,
                'avg_price': avg_price
            })
            chart_labels.append(row[0] or '未知')
            chart_issued.append(round(float(ordinary_total) / 10000, 1))  # 转万张
            chart_issued_platform_sold.append(round(float(issued_platform_sold) / 10000, 1))  # 转万张
            chart_trading_platform_sold.append(round(float(trading_platform_sold) / 10000, 1))  # 转万张
            chart_issued_ratio.append(round(float(issued_ratio), 1))  # 售出比例
            chart_avg_price.append(round(float(avg_price), 1))  # 平均成交价
    
    # 计算汇总数据并插入到第一行
    if summary_data:
        total_ordinary = sum(row['ordinary_total'] for row in summary_data)
        total_issued_platform_sold = sum(row['issued_platform_sold'] for row in summary_data)
        total_trading_platform_sold = sum(row['trading_platform_sold'] for row in summary_data)
        
        # 计算汇总的售出比例
        total_issued_ratio = round((total_issued_platform_sold / total_ordinary * 100), 1) if total_ordinary > 0 else 0
        total_trading_ratio = round((total_trading_platform_sold / total_ordinary * 100), 1) if total_ordinary > 0 else 0
        
        # 计算汇总的平均成交价（加权平均）
        total_trading_amount = 0
        for row in summary_data:
            total_trading_amount += row['trading_platform_sold'] * row['avg_price']
        total_avg_price = round(total_trading_amount / total_trading_platform_sold, 1) if total_trading_platform_sold > 0 else 0
        
        # 创建汇总行
        summary_row = {
            'dimension_value': '汇总',
            'ordinary_total': total_ordinary,
            'issued_platform_sold': total_issued_platform_sold,
            'trading_platform_sold': total_trading_platform_sold,
            'issued_ratio': total_issued_ratio,
            'trading_ratio': total_trading_ratio,
            'avg_price': total_avg_price
        }
        
        # 将汇总行插入到第一位
        summary_data.insert(0, summary_row)
    
    return render_template('statistics.html',
                         dimension=dimension,
                         start_date=start_date,
                         end_date=end_date,
                         transaction_start_date=transaction_start_date,
                         transaction_end_date=transaction_end_date,
                         summary_data=summary_data,
                         chart_labels=chart_labels,
                         chart_issued=chart_issued,
                         chart_issued_platform_sold=chart_issued_platform_sold,
                         chart_trading_platform_sold=chart_trading_platform_sold,
                         chart_issued_ratio=chart_issued_ratio,
                         chart_avg_price=chart_avg_price)


@dashboard_bp.route('/get_filtered_projects', methods=['POST'])
@login_required
def get_filtered_projects():
    filters = request.json
    print(f'Received filters: {filters}')
    query = Project.query
    for key, value in filters.items():
        if value and value != '--所有--':
            if key in ['is_uhv_support', 'has_subsidy']:
                query = query.filter(getattr(Project, key) == (value == '1'))
            else:
                query = query.filter(getattr(Project, key) == value)
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    projects = query.order_by(Project.project_name).all()
    print(f'Found {len(projects)} projects')
    project_list = [{'id': p.id, 'project_name': p.project_name} for p in projects]
    return jsonify({'projects': project_list})


@dashboard_bp.route('/get_analysis_data', methods=['POST'])
@login_required
def get_analysis_data():
    """根据筛选条件获取交易分析数据，按电量生产年月聚合"""
    filters = request.json
    query = Project.query
    
    # 获取时间筛选参数
    production_start_month = filters.get('production_start_month')
    production_end_month = filters.get('production_end_month')
    transaction_start_date = filters.get('transaction_start_date')
    transaction_end_date = filters.get('transaction_end_date')
    
    # 应用筛选条件
    for key, value in filters.items():
        if key not in ['projects', 'production_start_month', 'production_end_month', 'transaction_start_date', 'transaction_end_date'] and value and value != '--所有--':
            if key in ['is_uhv_support', 'has_subsidy']:
                query = query.filter(getattr(Project, key) == (value == '1'))
            else:
                query = query.filter(getattr(Project, key) == value)
    
    # 处理项目筛选
    project_ids = filters.get('projects', [])
    if project_ids and '--所有--' not in project_ids:
        query = query.filter(Project.id.in_(project_ids))
    
    # 非管理员只能查看自己的数据
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return jsonify([])
    
    # 执行聚合统计
    with db.engine.connect() as connection:
        # 主查询：获取所有电量生产年月并按时间倒序排列
        main_sql_conditions = ["project_id IN :project_ids", "production_year_month IS NOT NULL", "production_year_month != ''"]
        main_sql_params = {'project_ids': tuple(project_ids_list)}
        
        # 添加生产时间筛选条件（只有当参数不为空时才添加）
        if production_start_month and production_start_month != '':
            main_sql_conditions.append("production_year_month >= :production_start_month")
            main_sql_params['production_start_month'] = production_start_month
        if production_end_month and production_end_month != '':
            main_sql_conditions.append("production_year_month <= :production_end_month")
            main_sql_params['production_end_month'] = production_end_month
            
        main_sql = text(f"""
            SELECT DISTINCT production_year_month
            FROM nyj_green_certificate_ledger 
            WHERE {' AND '.join(main_sql_conditions)}
            ORDER BY production_year_month DESC
        """)
        
        months_result = connection.execute(main_sql, main_sql_params).fetchall()
        
        data = []
        for month_row in months_result:
            month = month_row[0]
            
            # 1. 普通绿证和绿电绿证 - 从nyj_green_certificate_ledger表
            ledger_sql = text("""
                SELECT 
                    COALESCE(SUM(CASE 
                        WHEN ordinary_quantity IS NOT NULL AND ordinary_quantity != '' 
                        THEN CAST(ordinary_quantity AS DECIMAL(15,2)) 
                        ELSE 0 
                    END), 0) as ordinary_total,
                    COALESCE(SUM(CASE 
                        WHEN green_quantity IS NOT NULL AND green_quantity != '' 
                        THEN CAST(green_quantity AS DECIMAL(15,2)) 
                        ELSE 0 
                    END), 0) as green_total
                FROM nyj_green_certificate_ledger 
                WHERE project_id IN :project_ids 
                AND production_year_month = :month
            """)
            ledger_result = connection.execute(ledger_sql, {
                'project_ids': tuple(project_ids_list), 
                'month': month
            }).fetchone()
            
            # 2. 核发平台售出量 - 从nyj_transaction_records表
            transaction_sql_conditions = [
                "project_id IN :project_ids",
                "CONCAT(production_year, '-', LPAD(production_month, 2, '0')) = :month",
                "transaction_num IS NOT NULL AND transaction_num != ''"
            ]
            transaction_sql_params = {'project_ids': tuple(project_ids_list), 'month': month}
            
            # 添加交易时间筛选条件（只有当参数不为空时才添加）
            if transaction_start_date and transaction_start_date != '':
                transaction_sql_conditions.append("transaction_time >= :transaction_start_date")
                transaction_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date and transaction_end_date != '':
                transaction_sql_conditions.append("transaction_time <= :transaction_end_date")
                transaction_sql_params['transaction_end_date'] = transaction_end_date
                
            transaction_sql = text(f"""
                SELECT COALESCE(SUM(CAST(transaction_num AS DECIMAL(15,2))), 0) as transaction_total
                FROM nyj_transaction_records 
                WHERE {' AND '.join(transaction_sql_conditions)}
            """)
            transaction_result = connection.execute(transaction_sql, transaction_sql_params).fetchone()
            
            # 3. 绿证平台售出-单向挂牌 - 从gzpt_unilateral_listings表
            unilateral_sql_conditions = [
                "project_id IN :project_ids",
                "generate_ym = :month",
                "order_status = '1'"
            ]
            unilateral_sql_params = {'project_ids': tuple(project_ids_list), 'month': month}
            
            # 添加交易时间筛选条件
            if transaction_start_date:
                unilateral_sql_conditions.append("order_time_str >= :transaction_start_date")
                unilateral_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date:
                unilateral_sql_conditions.append("order_time_str <= :transaction_end_date")
                unilateral_sql_params['transaction_end_date'] = transaction_end_date
                
            unilateral_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as unilateral_qty,
                    COALESCE(SUM(CASE WHEN total_quantity != '' AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as unilateral_amt
                FROM gzpt_unilateral_listings 
                WHERE {' AND '.join(unilateral_sql_conditions)}
            """)
            unilateral_result = connection.execute(unilateral_sql, unilateral_sql_params).fetchone()
            
            # 4. 绿证平台售出-双边线下 - 从gzpt_bilateral_offline_trades表
            offline_sql_conditions = [
                "project_id IN :project_ids",
                "generate_ym = :month",
                "order_status = '3'"
            ]
            offline_sql_params = {'project_ids': tuple(project_ids_list), 'month': month}
            
            # 添加交易时间筛选条件
            if transaction_start_date:
                offline_sql_conditions.append("order_time_str >= :transaction_start_date")
                offline_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date:
                offline_sql_conditions.append("order_time_str <= :transaction_end_date")
                offline_sql_params['transaction_end_date'] = transaction_end_date
                
            offline_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as offline_qty,
                    COALESCE(SUM(CASE WHEN total_quantity != '' AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as offline_amt
                FROM gzpt_bilateral_offline_trades 
                WHERE {' AND '.join(offline_sql_conditions)}
            """)
            offline_result = connection.execute(offline_sql, offline_sql_params).fetchone()
            
            # 5. 绿证平台售出-双边线上 - 从gzpt_bilateral_online_trades表
            online_sql_conditions = [
                "project_id IN :project_ids",
                "generate_ym = :month"
            ]
            online_sql_params = {'project_ids': tuple(project_ids_list), 'month': month}
            
            # 添加交易时间筛选条件
            if transaction_start_date:
                online_sql_conditions.append("order_time_str >= :transaction_start_date")
                online_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date:
                online_sql_conditions.append("order_time_str <= :transaction_end_date")
                online_sql_params['transaction_end_date'] = transaction_end_date
                
            online_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as online_qty,
                    COALESCE(SUM(CASE WHEN total_quantity != '' AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as online_amt
                FROM gzpt_bilateral_online_trades 
                WHERE {' AND '.join(online_sql_conditions)}
            """)
            online_result = connection.execute(online_sql, online_sql_params).fetchone()
            
            # 6. 北交平台售出 - 从beijing_power_exchange_trades表
            beijing_sql_conditions = [
                "project_id IN :project_ids",
                "production_year_month = :month"
            ]
            beijing_sql_params = {'project_ids': tuple(project_ids_list), 'month': month}
            
            # 添加交易时间筛选条件
            if transaction_start_date:
                beijing_sql_conditions.append("transaction_time >= :transaction_start_date")
                beijing_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date:
                beijing_sql_conditions.append("transaction_time <= :transaction_end_date")
                beijing_sql_params['transaction_end_date'] = transaction_end_date
                
            beijing_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN transaction_quantity IS NOT NULL AND transaction_quantity != '' THEN CAST(transaction_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as beijing_qty,
                    COALESCE(SUM(CASE WHEN transaction_quantity != '' AND transaction_price != '' THEN CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,2)) ELSE 0 END), 0) as beijing_amt
                FROM beijing_power_exchange_trades 
                WHERE {' AND '.join(beijing_sql_conditions)}
            """)
            beijing_result = connection.execute(beijing_sql, beijing_sql_params).fetchone()
            
            # 7. 广交平台售出 - 从guangzhou_power_exchange_trades表
            guangzhou_sql_conditions = [
                "project_id IN :project_ids",
                "CONCAT(SUBSTRING(product_date, 1, 4), '-', LPAD(SUBSTRING(product_date, 6), 2, '0')) = :month"
            ]
            guangzhou_sql_params = {'project_ids': tuple(project_ids_list), 'month': month}
            
            # 添加交易时间筛选条件
            if transaction_start_date:
                guangzhou_sql_conditions.append("deal_time >= :transaction_start_date")
                guangzhou_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date:
                guangzhou_sql_conditions.append("deal_time <= :transaction_end_date")
                guangzhou_sql_params['transaction_end_date'] = transaction_end_date
                
            guangzhou_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN gpc_certifi_num IS NOT NULL AND gpc_certifi_num != 0 THEN CAST(gpc_certifi_num AS DECIMAL(15,2)) ELSE 0 END), 0) as guangzhou_qty,
                    COALESCE(SUM(CASE WHEN gpc_certifi_num != 0 AND total_cost != 0 THEN CAST(total_cost AS DECIMAL(15,2)) ELSE 0 END), 0) as guangzhou_amt
                FROM guangzhou_power_exchange_trades 
                WHERE {' AND '.join(guangzhou_sql_conditions)}
            """)
            guangzhou_result = connection.execute(guangzhou_sql, guangzhou_sql_params).fetchone()
            
            # 计算均价（保留2位小数）
            unilateral_qty = float(unilateral_result[0] or 0)
            unilateral_amt = float(unilateral_result[1] or 0)
            unilateral_avg = round(unilateral_amt / unilateral_qty, 2) if unilateral_qty > 0 else 0
            
            offline_qty = float(offline_result[0] or 0)
            offline_amt = float(offline_result[1] or 0)
            offline_avg = round(offline_amt / offline_qty, 2) if offline_qty > 0 else 0
            
            online_qty = float(online_result[0] or 0)
            online_amt = float(online_result[1] or 0)
            online_avg = round(online_amt / online_qty, 2) if online_qty > 0 else 0
            
            beijing_qty = float(beijing_result[0] or 0)
            beijing_amt = float(beijing_result[1] or 0)
            beijing_avg = round(beijing_amt / beijing_qty, 2) if beijing_qty > 0 else 0
            
            guangzhou_qty = float(guangzhou_result[0] or 0)
            guangzhou_amt = float(guangzhou_result[1] or 0)
            guangzhou_avg = round(guangzhou_amt / guangzhou_qty, 2) if guangzhou_qty > 0 else 0
            
            # 组装数据
            data.append({
                'production_year_month': month,
                'ordinary_green': float(ledger_result[0] or 0),
                'green_green': float(ledger_result[1] or 0),
                'issued_platform_sold': float(transaction_result[0] or 0),
                'unilateral_qty': unilateral_qty,
                'unilateral_amt': unilateral_amt,
                'unilateral_avg': unilateral_avg,
                'offline_qty': offline_qty,
                'offline_amt': offline_amt,
                'offline_avg': offline_avg,
                'online_qty': online_qty,
                'online_amt': online_amt,
                'online_avg': online_avg,
                'beijing_qty': beijing_qty,
                'beijing_amt': beijing_amt,
                'beijing_avg': beijing_avg,
                'guangzhou_qty': guangzhou_qty,
                'guangzhou_amt': guangzhou_amt,
                'guangzhou_avg': guangzhou_avg
            })
    
    return jsonify(data)


@dashboard_bp.route('/get_transaction_time_data', methods=['POST'])
@login_required
def get_transaction_time_data():
    """根据筛选条件获取交易分析数据，按交易时间聚合"""
    filters = request.json
    query = Project.query
    
    # 获取时间筛选参数
    production_start_month = filters.get('production_start_month')
    production_end_month = filters.get('production_end_month')
    transaction_start_date = filters.get('transaction_start_date')
    transaction_end_date = filters.get('transaction_end_date')
    
    # 应用筛选条件
    for key, value in filters.items():
        if key not in ['projects', 'production_start_month', 'production_end_month', 'transaction_start_date', 'transaction_end_date'] and value and value != '--所有--':
            if key in ['is_uhv_support', 'has_subsidy']:
                query = query.filter(getattr(Project, key) == (value == '1'))
            else:
                query = query.filter(getattr(Project, key) == value)
    
    # 处理项目筛选
    project_ids = filters.get('projects', [])
    if project_ids and '--所有--' not in project_ids:
        query = query.filter(Project.id.in_(project_ids))
    
    # 非管理员只能查看自己的数据
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return jsonify([])
    
    # 执行聚合统计
    with db.engine.connect() as connection:
        # 获取所有交易月份
        main_sql_params = {'project_ids': tuple(project_ids_list)}
        
        # 【修正】为每个表构建独立的、正确的时间筛选条件（只有当参数不为空时才添加）
        time_filters = {
            'bj': '', 'gz': '', 'ul': '', 'ol': '', 'off': ''
        }
        if transaction_start_date and transaction_start_date != '':
            time_filters['bj'] += " AND bj.transaction_time >= :transaction_start_date"
            time_filters['gz'] += " AND gz.deal_time >= :transaction_start_date"
            time_filters['ul'] += " AND ul.order_time_str >= :transaction_start_date"
            time_filters['ol'] += " AND ol.order_time_str >= :transaction_start_date"
            time_filters['off'] += " AND off.order_time_str >= :transaction_start_date"
            main_sql_params['transaction_start_date'] = transaction_start_date
        if transaction_end_date and transaction_end_date != '':
            time_filters['bj'] += " AND bj.transaction_time <= :transaction_end_date"
            time_filters['gz'] += " AND gz.deal_time <= :transaction_end_date"
            time_filters['ul'] += " AND ul.order_time_str <= :transaction_end_date"
            time_filters['ol'] += " AND ol.order_time_str <= :transaction_end_date"
            time_filters['off'] += " AND off.order_time_str <= :transaction_end_date"
            main_sql_params['transaction_end_date'] = transaction_end_date
            
        transaction_months_sql = text(f"""
            SELECT DISTINCT SUBSTRING(bj.transaction_time, 1, 7) as transaction_month
            FROM beijing_power_exchange_trades bj
            WHERE bj.project_id IN :project_ids AND bj.transaction_time IS NOT NULL {time_filters['bj']}
            UNION
            SELECT DISTINCT SUBSTRING(gz.deal_time, 1, 7)
            FROM guangzhou_power_exchange_trades gz
            WHERE gz.project_id IN :project_ids AND gz.deal_time IS NOT NULL {time_filters['gz']}
            UNION
            SELECT DISTINCT SUBSTRING(ul.order_time_str, 1, 7)
            FROM gzpt_unilateral_listings ul
            WHERE ul.project_id IN :project_ids AND ul.order_time_str IS NOT NULL {time_filters['ul']}
            UNION
            SELECT DISTINCT SUBSTRING(ol.order_time_str, 1, 7)
            FROM gzpt_bilateral_online_trades ol
            WHERE ol.project_id IN :project_ids AND ol.order_time_str IS NOT NULL {time_filters['ol']}
            UNION
            SELECT DISTINCT SUBSTRING(off.order_time_str, 1, 7)
            FROM gzpt_bilateral_offline_trades off
            WHERE off.project_id IN :project_ids AND off.order_time_str IS NOT NULL {time_filters['off']}
            ORDER BY transaction_month DESC
        """)
        
        months_result = connection.execute(transaction_months_sql, main_sql_params).fetchall()
        
        data = []
        for month_row in months_result:
            transaction_month = month_row[0]
            if not transaction_month:
                continue
                
            # 单向挂牌数据
            unilateral_sql_conditions = [
                "project_id IN :project_ids",
                "SUBSTRING(order_time_str, 1, 7) = :month",
                "order_status = '1'"
            ]
            unilateral_sql_params = {'project_ids': tuple(project_ids_list), 'month': transaction_month}
            
            # 添加时间筛选条件（只有当参数不为空时才添加）
            if transaction_start_date and transaction_start_date != '':
                unilateral_sql_conditions.append("order_time_str >= :transaction_start_date")
                unilateral_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date and transaction_end_date != '':
                unilateral_sql_conditions.append("order_time_str <= :transaction_end_date")
                unilateral_sql_params['transaction_end_date'] = transaction_end_date
            if production_start_month and production_start_month != '':
                unilateral_sql_conditions.append("generate_ym >= :production_start_month")
                unilateral_sql_params['production_start_month'] = production_start_month
            if production_end_month and production_end_month != '':
                unilateral_sql_conditions.append("generate_ym <= :production_end_month")
                unilateral_sql_params['production_end_month'] = production_end_month
                
            unilateral_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as total_qty,
                    COALESCE(SUM(CASE WHEN total_amount IS NOT NULL AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as total_amt
                FROM gzpt_unilateral_listings
                WHERE {' AND '.join(unilateral_sql_conditions)}
            """)
            
            # 双边线上数据
            online_sql_conditions = [
                "project_id IN :project_ids",
                "SUBSTRING(order_time_str, 1, 7) = :month"
            ]
            online_sql_params = {'project_ids': tuple(project_ids_list), 'month': transaction_month}
            
            # 添加时间筛选条件（只有当参数不为空时才添加）
            if transaction_start_date and transaction_start_date != '':
                online_sql_conditions.append("order_time_str >= :transaction_start_date")
                online_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date and transaction_end_date != '':
                online_sql_conditions.append("order_time_str <= :transaction_end_date")
                online_sql_params['transaction_end_date'] = transaction_end_date
            if production_start_month and production_start_month != '':
                online_sql_conditions.append("generate_ym >= :production_start_month")
                online_sql_params['production_start_month'] = production_start_month
            if production_end_month and production_end_month != '':
                online_sql_conditions.append("generate_ym <= :production_end_month")
                online_sql_params['production_end_month'] = production_end_month
                
            online_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as total_qty,
                    COALESCE(SUM(CASE WHEN total_amount IS NOT NULL AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as total_amt
                FROM gzpt_bilateral_online_trades
                WHERE {' AND '.join(online_sql_conditions)}
            """)
            
            # 双边线下数据
            offline_sql_conditions = [
                "project_id IN :project_ids",
                "SUBSTRING(order_time_str, 1, 7) = :month",
                "order_status = '3'"
            ]
            offline_sql_params = {'project_ids': tuple(project_ids_list), 'month': transaction_month}
            
            # 添加时间筛选条件（只有当参数不为空时才添加）
            if transaction_start_date and transaction_start_date != '':
                offline_sql_conditions.append("order_time_str >= :transaction_start_date")
                offline_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date and transaction_end_date != '':
                offline_sql_conditions.append("order_time_str <= :transaction_end_date")
                offline_sql_params['transaction_end_date'] = transaction_end_date
            if production_start_month and production_start_month != '':
                offline_sql_conditions.append("generate_ym >= :production_start_month")
                offline_sql_params['production_start_month'] = production_start_month
            if production_end_month and production_end_month != '':
                offline_sql_conditions.append("generate_ym <= :production_end_month")
                offline_sql_params['production_end_month'] = production_end_month
                
            offline_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as total_qty,
                    COALESCE(SUM(CASE WHEN total_amount IS NOT NULL AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as total_amt
                FROM gzpt_bilateral_offline_trades
                WHERE {' AND '.join(offline_sql_conditions)}
            """)
            
            # 北京交易中心数据
            beijing_sql_conditions = [
                "project_id IN :project_ids",
                "SUBSTRING(transaction_time, 1, 7) = :month"
            ]
            beijing_sql_params = {'project_ids': tuple(project_ids_list), 'month': transaction_month}
            
            # 添加时间筛选条件（只有当参数不为空时才添加）
            if transaction_start_date and transaction_start_date != '':
                beijing_sql_conditions.append("transaction_time >= :transaction_start_date")
                beijing_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date and transaction_end_date != '':
                beijing_sql_conditions.append("transaction_time <= :transaction_end_date")
                beijing_sql_params['transaction_end_date'] = transaction_end_date
            if production_start_month and production_start_month != '':
                beijing_sql_conditions.append("production_year_month >= :production_start_month")
                beijing_sql_params['production_start_month'] = production_start_month
            if production_end_month and production_end_month != '':
                beijing_sql_conditions.append("production_year_month <= :production_end_month")
                beijing_sql_params['production_end_month'] = production_end_month
                
            beijing_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN transaction_quantity IS NOT NULL AND transaction_quantity != '' THEN CAST(transaction_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as total_qty,
                    COALESCE(SUM(CASE 
                        WHEN transaction_quantity IS NOT NULL AND transaction_quantity != '' AND transaction_price IS NOT NULL AND transaction_price != '' 
                        THEN CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,2))
                        ELSE 0 
                    END), 0) as total_amt
                FROM beijing_power_exchange_trades
                WHERE {' AND '.join(beijing_sql_conditions)}
            """)
            
            # 广州交易中心数据
            guangzhou_sql_conditions = [
                "project_id IN :project_ids",
                "SUBSTRING(deal_time, 1, 7) = :month" 
            ]
            guangzhou_sql_params = {'project_ids': tuple(project_ids_list), 'month': transaction_month}
            
            # 【修正】添加时间筛选条件（只有当参数不为空时才添加）
            if transaction_start_date and transaction_start_date != '':
                # 交易时间筛选应作用于 deal_time 字段
                guangzhou_sql_conditions.append("deal_time >= :transaction_start_date")
                guangzhou_sql_params['transaction_start_date'] = transaction_start_date
            if transaction_end_date and transaction_end_date != '':
                # 交易时间筛选应作用于 deal_time 字段
                guangzhou_sql_conditions.append("deal_time <= :transaction_end_date")
                guangzhou_sql_params['transaction_end_date'] = transaction_end_date
            
            # 生产时间筛选需转换 product_date 格式（只有当参数不为空时才添加）
            production_ym_column = "CONCAT(SUBSTRING(product_date, 1, 4), '-', LPAD(SUBSTRING(product_date, 6), 2, '0'))"
            if production_start_month and production_start_month != '':
                guangzhou_sql_conditions.append(f"{production_ym_column} >= :production_start_month")
                guangzhou_sql_params['production_start_month'] = production_start_month
            if production_end_month and production_end_month != '':
                guangzhou_sql_conditions.append(f"{production_ym_column} <= :production_end_month")
                guangzhou_sql_params['production_end_month'] = production_end_month
                
            guangzhou_sql = text(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN gpc_certifi_num IS NOT NULL AND gpc_certifi_num != 0 THEN CAST(gpc_certifi_num AS DECIMAL(15,2)) ELSE 0 END), 0) as total_qty,
                    COALESCE(SUM(CASE 
                        WHEN gpc_certifi_num IS NOT NULL AND gpc_certifi_num != 0 AND total_cost IS NOT NULL AND total_cost != 0 
                        THEN CAST(total_cost AS DECIMAL(15,2))
                        ELSE 0 
                    END), 0) as total_amt
                FROM guangzhou_power_exchange_trades
                WHERE {' AND '.join(guangzhou_sql_conditions)}
            """)
            
            # 执行查询
            unilateral_result = connection.execute(unilateral_sql, unilateral_sql_params).fetchone()
            
            online_result = connection.execute(online_sql, online_sql_params).fetchone()
            
            offline_result = connection.execute(offline_sql, offline_sql_params).fetchone()
            
            beijing_result = connection.execute(beijing_sql, beijing_sql_params).fetchone()
            
            guangzhou_result = connection.execute(guangzhou_sql, guangzhou_sql_params).fetchone()
            
            # 计算均价（保留2位小数）
            unilateral_qty = float(unilateral_result[0] or 0)
            unilateral_amt = float(unilateral_result[1] or 0)
            unilateral_avg = round(unilateral_amt / unilateral_qty, 2) if unilateral_qty > 0 else 0
            
            offline_qty = float(offline_result[0] or 0)
            offline_amt = float(offline_result[1] or 0)
            offline_avg = round(offline_amt / offline_qty, 2) if offline_qty > 0 else 0
            
            online_qty = float(online_result[0] or 0)
            online_amt = float(online_result[1] or 0)
            online_avg = round(online_amt / online_qty, 2) if online_qty > 0 else 0
            
            beijing_qty = float(beijing_result[0] or 0)
            beijing_amt = float(beijing_result[1] or 0)
            beijing_avg = round(beijing_amt / beijing_qty, 2) if beijing_qty > 0 else 0
            
            guangzhou_qty = float(guangzhou_result[0] or 0)
            guangzhou_amt = float(guangzhou_result[1] or 0)
            guangzhou_avg = round(guangzhou_amt / guangzhou_qty, 2) if guangzhou_qty > 0 else 0
            
            # 组装数据
            data.append({
                'transaction_year_month': transaction_month,
                'unilateral_qty': unilateral_qty,
                'unilateral_amt': unilateral_amt,
                'unilateral_avg': unilateral_avg,
                'offline_qty': offline_qty,
                'offline_amt': offline_amt,
                'offline_avg': offline_avg,
                'online_qty': online_qty,
                'online_amt': online_amt,
                'online_avg': online_avg,
                'beijing_qty': beijing_qty,
                'beijing_amt': beijing_amt,
                'beijing_avg': beijing_avg,
                'guangzhou_qty': guangzhou_qty,
                'guangzhou_amt': guangzhou_amt,
                'guangzhou_avg': guangzhou_avg
            })
    
    return jsonify(data)


@dashboard_bp.route('/transaction-analysis')
@login_required
def transaction_analysis():
    is_admin = current_user.is_admin
    print(f'User: {current_user.username}, Is Admin: {is_admin}')

    # 为筛选下拉菜单准备数据
    secondary_units_query = db.session.query(Project.secondary_unit).distinct().all()
    
    # 使用自定义排序键函数，按照汉字拼音首字母排序
    def get_pinyin_sort_key(s):
        # 简单的拼音首字母排序规则
        # 对于常见的二级单位名称进行硬编码排序
        pinyin_map = {
            '上海电力': 'shanghai',
            '上海能科': 'shanghai',
            '东北公司': 'dongbei',
            '中国电力': 'zhongguo',
            '云南国际': 'yunnan',
            '五凌电力': 'wuling',
            '内蒙古公司': 'neimenggu',
            '北京公司': 'beijing',
            '吉电股份': 'jidian',
            '四川公司': 'sichuan',
            '国能生物': 'guoneng',
            '安徽公司': 'anhui',
            '山东能源': 'shandong',
            '山西公司': 'shanxi',
            '工程公司': 'gongcheng',
            '广东公司': 'guangdong',
            '广西公司': 'guangxi',
            '新疆能源化工': 'xinjiang',
            '江苏公司': 'jiangsu',
            '江西公司': 'jiangxi',
            '河南公司': 'henan',
            '浙江公司': 'zhejiang',
            '海南公司': 'hainan',
            '湖北公司': 'hubei',
            '湖南公司': 'hunan',
            '甘肃公司': 'gansu',
            '福建公司': 'fujian',
            '贵州公司': 'guizhou',
            '重庆公司': 'chongqing',
            '陕西公司': 'shaanxi',
            '黄河公司': 'huanghe',
            '黑龙江公司': 'heilongjiang'
        }
        # 如果在映射表中，直接返回对应的拼音
        if s in pinyin_map:
            return pinyin_map[s]
        # 否则返回原字符串（英文字符会自然排序）
        return s
        
    secondary_units = sorted([r[0] for r in secondary_units_query if r[0]], key=get_pinyin_sort_key)
    provinces = db.session.query(Project.province).distinct().order_by(Project.province).all()
    regions = db.session.query(Project.region).distinct().order_by(Project.region).all()
    power_types = db.session.query(Project.power_type).distinct().order_by(Project.power_type).all()
    investment_scopes = db.session.query(Project.investment_scope).distinct().order_by(Project.investment_scope).all()
    project_natures = db.session.query(Project.project_nature).distinct().order_by(Project.project_nature).all()

    # 查询初始项目列表
    query = Project.query.order_by(Project.project_name)
    if not is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
        print(f'Non-admin user {current_user.username}, filtering by secondary_unit')
    initial_projects = query.all()
    print(f'Initial projects count: {len(initial_projects)}')
    if len(initial_projects) > 0:
        print(f'First project: {initial_projects[0].project_name}')
    else:
        print('No projects found for current user')

    return render_template('transaction_analysis.html',
                           secondary_units=secondary_units,
                           provinces=[r[0] for r in provinces if r[0]],
                           regions=[r[0] for r in regions if r[0]],
                           power_types=[r[0] for r in power_types if r[0]],
                           investment_scopes=[r[0] for r in investment_scopes if r[0]],
                           project_natures=[r[0] for r in project_natures if r[0]],
                           projects=initial_projects)

@dashboard_bp.route('/test', methods=['GET'])
@login_required
def test():
    print('Test request received')
    return jsonify({'status': 'ok'})


@dashboard_bp.route('/customer_analysis')
@login_required
def customer_analysis():
    """重定向到客户成交情况页面"""
    return redirect(url_for('dashboard.customer_transactions'))

@dashboard_bp.route('/customer_analysis/transactions')
@login_required
def customer_transactions():
    """渲染客户分析页面，提供客户维度的交易数据汇总"""
    
    # 获取当前日期，设置默认时间范围
    current_date = datetime.now()
    default_start = f"{current_date.year}-01"
    default_end = f"{current_date.year}-{str(current_date.month).zfill(2)}"
    
    # 获取请求参数
    start_date = request.args.get('start_date', default_start)
    end_date = request.args.get('end_date', default_end)
    
    # 获取交易日期筛选参数
    current_date_str = current_date.strftime('%Y-%m-%d')
    transaction_start_date = request.args.get('transaction_start_date', '')
    transaction_end_date = request.args.get('transaction_end_date', current_date_str)
    
    # 移除客户类型判断逻辑
    
    customer_data = []
    
    # 添加项目权限控制
    query = Project.query
    # 非管理员只能查看自己的数据
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return render_template('customer_analysis.html',
                             start_date=start_date,
                             end_date=end_date,
                             transaction_start_date=transaction_start_date,
                             transaction_end_date=transaction_end_date,
                             customer_data=customer_data)
    
    with db.engine.connect() as connection:
        # 生产期筛选策略：
        # - 初次进入（URL无对应参数）使用默认：当年1月至当前月
        # - 如果用户提交但清空（参数存在但为空字符串），视为不限制生产期（全量），绑定极值范围
        # - 两端都有值时，按用户选择范围
        has_prod_params = ('start_date' in request.args) or ('end_date' in request.args)
        if has_prod_params:
            if start_date and end_date:
                start_month = start_date[:7]
                end_month = end_date[:7]
            else:
                start_month = '0000-01'
                end_month = '9999-12'
        else:
            start_month = default_start
            end_month = default_end
        
        # 构建基础SQL查询，按客户名称分组
        base_sql = """
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
                COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_quantity,
                -- 成交均价
                CASE 
                    WHEN (COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0)) > 0
                    THEN (COALESCE(SUM(CAST(ul.total_amount AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(ol.total_amount AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(off.total_amount AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2)) * CAST(bj.transaction_price AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(gz.total_cost AS DECIMAL(15,2))), 0)) / 
                         (COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                          COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0))
                    ELSE 0 
                END as avg_price
            FROM projects p 
            JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
            LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
            LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym
            LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym
            LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month
            LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND n.production_year_month = gz.product_date
            WHERE p.id IN :project_ids
            AND n.production_year_month >= :start_month
            AND n.production_year_month <= :end_month
        """
        
        # 添加交易时间筛选条件
        transaction_filter = ""
        if transaction_start_date and transaction_end_date:
            transaction_filter = f"""
                AND ((
                    bj.transaction_time >= '{transaction_start_date}' AND bj.transaction_time <= '{transaction_end_date}'
                ) OR (
                    gz.deal_time >= '{transaction_start_date}' AND gz.deal_time <= '{transaction_end_date}'
                ) OR (
                    ul.order_time_str >= '{transaction_start_date}' AND ul.order_time_str <= '{transaction_end_date}'
                ) OR (
                    ol.order_time_str >= '{transaction_start_date}' AND ol.order_time_str <= '{transaction_end_date}'
                ) OR (
                    off.order_time_str >= '{transaction_start_date}' AND off.order_time_str <= '{transaction_end_date}'
                ))
            """
        
        # 移除客户类型筛选条件
        
        # 组合完整SQL
        complete_sql = base_sql + transaction_filter + """
            GROUP BY customer_name
            HAVING total_quantity > 0
            ORDER BY total_quantity DESC
        """
        
        # 执行查询
        result = connection.execute(
            text(complete_sql),
            {"project_ids": project_ids_list, "start_month": start_month, "end_month": end_month}
        )
        
        # 处理查询结果
        for row in result:
            customer_data.append({
                'customer_name': row.customer_name,
                'total_quantity': f"{float(row.total_quantity):.2f}",
                'avg_price': f"{float(row.avg_price):.2f}"
            })
    
    return render_template('customer_analysis.html',
                         start_date=start_date,
                         end_date=end_date,
                         transaction_start_date=transaction_start_date,
                         transaction_end_date=transaction_end_date,
                         customer_data=customer_data)

@dashboard_bp.route('/customer_analysis/info')
@login_required
def customer_info():
    """客户信息页面"""
    from models import Customer
    
    # 获取客户信息表中的客户范围，与customer_transactions页面保持一致
    # 添加项目权限控制
    query = Project.query
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        customers = []
    else:
        with db.engine.connect() as connection:
            # 获取所有有交易记录的客户名称（不限时间）
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
            
            result = connection.execute(
                text(customer_sql),
                {"project_ids": project_ids_list}
            )
            
            customer_names = [row.customer_name for row in result if row.customer_name != '未知客户']
            
            # 获取客户交易量数据用于排序
            customer_volume_sql = """
                SELECT 
                    CASE 
                        WHEN bj.buyer_entity_name IS NOT NULL THEN bj.buyer_entity_name
                        WHEN gz.buyer_entity_name IS NOT NULL THEN gz.buyer_entity_name
                        WHEN ul.member_name IS NOT NULL THEN ul.member_name
                        WHEN ol.member_name IS NOT NULL THEN ol.member_name
                        WHEN off.member_name IS NOT NULL THEN off.member_name
                        ELSE '未知客户'
                    END as customer_name,
                    SUM(
                        COALESCE(ul.total_quantity, 0) + 
                        COALESCE(ol.total_quantity, 0) + 
                        COALESCE(off.total_quantity, 0) + 
                        COALESCE(bj.transaction_quantity, 0) + 
                        COALESCE(gz.gpc_certifi_num, 0)
                    ) as total_quantity
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
                GROUP BY customer_name
                HAVING total_quantity > 0
            """
            
            volume_result = connection.execute(
                text(customer_volume_sql),
                {"project_ids": project_ids_list}
            )
            
            # 创建客户交易量字典
            customer_volumes = {row.customer_name: float(row.total_quantity) for row in volume_result if row.customer_name != '未知客户'}
        
        # 从customer表中获取客户信息
        customers = Customer.query.filter(Customer.customer_name.in_(customer_names)).all()
        
        # 为没有在customer表中的客户创建默认记录
        existing_names = {c.customer_name for c in customers}
        missing_names = set(customer_names) - existing_names
        
        for name in missing_names:
            new_customer = Customer(
                customer_name=name,
                customer_type='未设置',
                province='未设置'
            )
            db.session.add(new_customer)
        
        if missing_names:
            db.session.commit()
            # 重新查询获取完整列表
            customers = Customer.query.filter(Customer.customer_name.in_(customer_names)).all()
        
        # 按客户类型和交易量排序：公司客户在前，个人客户在后，各自按交易量降序排列
        def sort_key(customer):
            volume = customer_volumes.get(customer.customer_name, 0)
            if customer.customer_type == '公司':
                return (0, -volume)  # 公司排在前面，交易量大的在前
            elif customer.customer_type == '个人':
                return (1, -volume)  # 个人排在后面，交易量大的在前
            else:
                return (2, -volume)  # 未设置排在最后
        
        customers.sort(key=sort_key)
    
    # 中国省级行政区列表
    provinces = [
        '北京市', '天津市', '河北省', '山西省', '内蒙古自治区', '辽宁省', '吉林省', '黑龙江省',
        '上海市', '江苏省', '浙江省', '安徽省', '福建省', '江西省', '山东省', '河南省',
        '湖北省', '湖南省', '广东省', '广西壮族自治区', '海南省', '重庆市', '四川省', '贵州省',
        '云南省', '西藏自治区', '陕西省', '甘肃省', '青海省', '宁夏回族自治区', '新疆维吾尔自治区',
        '台湾省', '香港特别行政区', '澳门特别行政区'
    ]
    
    return render_template('customer_info.html', customers=customers, provinces=provinces)

@dashboard_bp.route('/customer_analysis/map')
@login_required
def customer_map():
    """客户成交地图页面"""
    return render_template('transaction_map.html')

@dashboard_bp.route('/api/province_transaction_data')
@login_required
def get_province_transaction_data():
    """获取各省份成交量数据API - 复用customer_transactions的查询逻辑"""
    from models import Customer
    
    # 添加项目权限控制
    query = Project.query
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return jsonify({'province_data': []})
    
    with db.engine.connect() as connection:
        # 使用不限时间的生产期范围
        start_month = '0000-01'
        end_month = '9999-12'
        
        # 复用customer_transactions函数中的正确SQL查询
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
            AND n.production_year_month >= :start_month
            AND n.production_year_month <= :end_month
            GROUP BY customer_name
            HAVING total_quantity > 0
        """
        
        transaction_result = connection.execute(
            text(transaction_sql),
            {
                "project_ids": project_ids_list,
                "start_month": start_month,
                "end_month": end_month
            }
        )
        
        # 按客户名称聚合交易量
        customer_volumes = {}
        for row in transaction_result:
            if row.customer_name and row.customer_name != '未知客户':
                customer_volumes[row.customer_name] = float(row.total_quantity or 0)
    
    # 直接从数据库查询所有客户的省份信息，避免IN子句参数过多
    province_volumes = {}
    
    # 获取所有有省份信息的客户
    all_customers = Customer.query.filter(
        Customer.province.isnot(None),
        Customer.province != '未设置'
    ).all()
    
    # 按省份聚合成交量
    for customer in all_customers:
        if customer.customer_name in customer_volumes:
            province = customer.province
            volume = customer_volumes[customer.customer_name]
            if province not in province_volumes:
                province_volumes[province] = 0
            province_volumes[province] += volume
    
    # 省份名称映射：将完整名称转换为热力图识别的简化名称
    province_name_mapping = {
        '北京市': '北京',
        '天津市': '天津', 
        '河北省': '河北',
        '山西省': '山西',
        '内蒙古自治区': '内蒙古',
        '辽宁省': '辽宁',
        '吉林省': '吉林',
        '黑龙江省': '黑龙江',
        '上海市': '上海',
        '江苏省': '江苏',
        '浙江省': '浙江',
        '安徽省': '安徽',
        '福建省': '福建',
        '江西省': '江西',
        '山东省': '山东',
        '河南省': '河南',
        '湖北省': '湖北',
        '湖南省': '湖南',
        '广东省': '广东',
        '广西壮族自治区': '广西',
        '海南省': '海南',
        '重庆市': '重庆',
        '四川省': '四川',
        '贵州省': '贵州',
        '云南省': '云南',
        '西藏自治区': '西藏',
        '陕西省': '陕西',
        '甘肃省': '甘肃',
        '青海省': '青海',
        '宁夏回族自治区': '宁夏',
        '新疆维吾尔自治区': '新疆',
        '台湾省': '台湾',
        '香港特别行政区': '香港',
        '澳门特别行政区': '澳门'
    }
    
    # 转换为前端需要的格式，并映射省份名称
    province_data = []
    for province, volume in province_volumes.items():
        # 使用映射表转换省份名称，如果没有映射则使用原名称
        mapped_name = province_name_mapping.get(province, province)
        province_data.append({
            'name': mapped_name,
            'value': round(volume, 2)
        })
    
    # 按成交量降序排序
    province_data.sort(key=lambda x: x['value'], reverse=True)
    
    return jsonify({
        'success': True,
        'data': province_data,
        'province_data': province_data  # 保持向后兼容
    })

@dashboard_bp.route('/api/seller_province_transaction_data')
@login_required
def get_seller_province_transaction_data():
    """获取各省份卖方成交量数据API"""
    
    # 添加项目权限控制
    query = Project.query
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return jsonify({'province_data': []})
    
    with db.engine.connect() as connection:
        # 使用不限时间的生产期范围
        start_month = '0000-01'
        end_month = '9999-12'
        
        # 查询卖方成交量数据 - 基于项目的省份信息
        seller_sql = """
            SELECT 
                p.province as seller_province,
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
            AND n.production_year_month >= :start_month
            AND n.production_year_month <= :end_month
            AND p.province IS NOT NULL
            AND p.province != ''
            GROUP BY p.province
            HAVING total_quantity > 0
        """
        
        seller_result = connection.execute(
            text(seller_sql),
            {
                "project_ids": project_ids_list,
                "start_month": start_month,
                "end_month": end_month
            }
        )
        
        # 省份名称映射：将完整名称转换为热力图识别的简化名称
        province_name_mapping = {
            '北京市': '北京',
            '天津市': '天津', 
            '河北省': '河北',
            '山西省': '山西',
            '内蒙古自治区': '内蒙古',
            '辽宁省': '辽宁',
            '吉林省': '吉林',
            '黑龙江省': '黑龙江',
            '上海市': '上海',
            '江苏省': '江苏',
            '浙江省': '浙江',
            '安徽省': '安徽',
            '福建省': '福建',
            '江西省': '江西',
            '山东省': '山东',
            '河南省': '河南',
            '湖北省': '湖北',
            '湖南省': '湖南',
            '广东省': '广东',
            '广西壮族自治区': '广西',
            '海南省': '海南',
            '重庆市': '重庆',
            '四川省': '四川',
            '贵州省': '贵州',
            '云南省': '云南',
            '西藏自治区': '西藏',
            '陕西省': '陕西',
            '甘肃省': '甘肃',
            '青海省': '青海',
            '宁夏回族自治区': '宁夏',
            '新疆维吾尔自治区': '新疆',
            '台湾省': '台湾',
            '香港特别行政区': '香港',
            '澳门特别行政区': '澳门'
        }
        
        # 转换为前端需要的格式，并映射省份名称
        province_data = []
        for row in seller_result:
            if row.seller_province:
                # 使用映射表转换省份名称，如果没有映射则使用原名称
                mapped_name = province_name_mapping.get(row.seller_province, row.seller_province)
                province_data.append({
                    'name': mapped_name,
                    'value': round(float(row.total_quantity or 0), 2)
                })
        
        # 按成交量降序排序
        province_data.sort(key=lambda x: x['value'], reverse=True)
        
        return jsonify({
            'success': True,
            'data': province_data,
            'province_data': province_data  # 保持向后兼容
        })

@dashboard_bp.route('/api/update_customer', methods=['POST'])
@login_required
def update_customer():
    """更新客户信息API"""
    from models import Customer
    
    try:
        data = request.get_json()
        customer_id = data.get('customer_id')
        customer_type = data.get('customer_type')
        province = data.get('province')
        
        if not customer_id:
            return jsonify({'success': False, 'message': '客户ID不能为空'}), 400
        
        customer = Customer.query.filter_by(customer_name=customer_id).first()
        if not customer:
            return jsonify({'success': False, 'message': '客户不存在'}), 404
        
        # 更新客户信息
        if customer_type:
            customer.customer_type = customer_type
        if province:
            customer.province = province
        
        db.session.commit()
        
        return jsonify({
            'success': True, 
            'message': '客户信息更新成功',
            'customer': {
                'customer_name': customer.customer_name,
                'customer_type': customer.customer_type,
                'province': customer.province
            }
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'更新失败: {str(e)}'}), 500


@dashboard_bp.route('/api/customer_details')
@login_required
def get_customer_details():
    """获取指定客户的交易明细数据"""
    # 获取请求参数
    customer_name = request.args.get('customer_name', '')
    if not customer_name:
        return jsonify({'error': '缺少客户名称参数'}), 400
    
    # 获取时间范围参数
    current_date = datetime.now()
    default_start = f"{current_date.year}-01"
    default_end = f"{current_date.year}-{str(current_date.month).zfill(2)}"
    
    start_date = request.args.get('start_date', default_start)
    end_date = request.args.get('end_date', default_end)
    transaction_start_date = request.args.get('transaction_start_date', '')
    transaction_end_date = request.args.get('transaction_end_date', current_date.strftime('%Y-%m-%d'))
    
    # 处理时间范围
    has_prod_params = ('start_date' in request.args) or ('end_date' in request.args)
    if has_prod_params:
        if start_date and end_date:
            start_month = start_date[:7]
            end_month = end_date[:7]
        else:
            start_month = '0000-01'
            end_month = '9999-12'
    else:
        start_month = default_start
        end_month = default_end
    
    # 添加项目权限控制
    query = Project.query
    # 非管理员只能查看自己的数据
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return jsonify({'details': []})
    
    details = []
    
    with db.engine.connect() as connection:
        # 构建SQL查询，获取交易明细
        details_sql = """
            SELECT 
                p.secondary_unit,
                CASE 
                    WHEN bj.transaction_time IS NOT NULL THEN bj.transaction_time
                    WHEN gz.deal_time IS NOT NULL THEN gz.deal_time
                    WHEN ul.order_time_str IS NOT NULL THEN ul.order_time_str
                    WHEN ol.order_time_str IS NOT NULL THEN ol.order_time_str
                    WHEN off.order_time_str IS NOT NULL THEN off.order_time_str
                    ELSE NULL
                END as transaction_time,
                n.production_year_month as production_time,
                CASE
                    WHEN ul.total_quantity IS NOT NULL THEN CAST(ul.total_quantity AS DECIMAL(15,2))
                    WHEN ol.total_quantity IS NOT NULL THEN CAST(ol.total_quantity AS DECIMAL(15,2))
                    WHEN off.total_quantity IS NOT NULL THEN CAST(off.total_quantity AS DECIMAL(15,2))
                    WHEN bj.transaction_quantity IS NOT NULL THEN CAST(bj.transaction_quantity AS DECIMAL(15,2))
                    WHEN gz.gpc_certifi_num IS NOT NULL THEN CAST(gz.gpc_certifi_num AS DECIMAL(15,2))
                    ELSE 0
                END as quantity,
                CASE
                    WHEN ul.total_quantity > 0 THEN CAST(ul.total_amount AS DECIMAL(15,2)) / CAST(ul.total_quantity AS DECIMAL(15,2))
                    WHEN ol.total_quantity > 0 THEN CAST(ol.total_amount AS DECIMAL(15,2)) / CAST(ol.total_quantity AS DECIMAL(15,2))
                    WHEN off.total_quantity > 0 THEN CAST(off.total_amount AS DECIMAL(15,2)) / CAST(off.total_quantity AS DECIMAL(15,2))
                    WHEN bj.transaction_quantity > 0 THEN CAST(bj.transaction_price AS DECIMAL(15,2))
                    WHEN gz.gpc_certifi_num > 0 THEN CAST(gz.total_cost AS DECIMAL(15,2)) / CAST(gz.gpc_certifi_num AS DECIMAL(15,2))
                    ELSE 0
                END as price,
                CASE
                    WHEN bj.record_project_name IS NOT NULL THEN bj.record_project_name
                    WHEN gz.record_project_name IS NOT NULL THEN gz.record_project_name
                    WHEN ul.project_name IS NOT NULL THEN ul.project_name
                    WHEN ol.project_name IS NOT NULL THEN ol.project_name
                    WHEN off.project_name IS NOT NULL THEN off.project_name
                    ELSE p.project_name
                END as project_name
            FROM projects p 
            JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
            LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
            LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym
            LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym
            LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month
            LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND n.production_year_month = gz.product_date
            WHERE p.id IN :project_ids
            AND n.production_year_month >= :start_month
            AND n.production_year_month <= :end_month
            AND (
                (bj.buyer_entity_name = :customer_name) OR
                (gz.buyer_entity_name = :customer_name) OR
                (ul.member_name = :customer_name) OR
                (ol.member_name = :customer_name) OR
                (off.member_name = :customer_name)
            )
        """
        
        # 添加交易时间筛选条件
        transaction_filter = ""
        if transaction_start_date and transaction_end_date:
            transaction_filter = f"""
                AND (
                    (bj.transaction_time IS NULL OR (bj.transaction_time >= '{transaction_start_date}' AND bj.transaction_time <= '{transaction_end_date}')) OR
                    (gz.deal_time IS NULL OR (gz.deal_time >= '{transaction_start_date}' AND gz.deal_time <= '{transaction_end_date}')) OR
                    (ul.order_time_str IS NULL OR (ul.order_time_str >= '{transaction_start_date}' AND ul.order_time_str <= '{transaction_end_date}')) OR
                    (ol.order_time_str IS NULL OR (ol.order_time_str >= '{transaction_start_date}' AND ol.order_time_str <= '{transaction_end_date}')) OR
                    (off.order_time_str IS NULL OR (off.order_time_str >= '{transaction_start_date}' AND off.order_time_str <= '{transaction_end_date}'))
                )
            """
        
        # 组合完整SQL
        complete_details_sql = details_sql + transaction_filter + """
            ORDER BY quantity DESC
        """
        
        # 执行查询
        result = connection.execute(
            text(complete_details_sql),
            {
                "project_ids": project_ids_list, 
                "start_month": start_month, 
                "end_month": end_month,
                "customer_name": customer_name
            }
        )
        
        # 处理查询结果
        for row in result:
            if row.quantity > 0:  # 只返回有效交易
                details.append({
                    'secondary_unit': row.secondary_unit,
                    'transaction_time': row.transaction_time,
                    'production_time': row.production_time,
                    'quantity': f"{float(row.quantity):.2f}",
                    'price': f"{float(row.price):.2f}",
                    'project_name': row.project_name
                })
    
    return jsonify({'details': details})


# 期望价格相关路由
@dashboard_bp.route('/data_submission_time')
@login_required
def data_submission_time():
    """渲染项目数据提交时间页面"""
    is_admin = current_user.is_admin
    selected_unit = request.args.get('secondary_unit', '')
    
    # 获取二级单位列表
    if is_admin:
        secondary_units_query = db.session.query(
            Project.secondary_unit
        ).group_by(Project.secondary_unit).all()
        # 按照拼音首字母排序二级单位
        # 使用自定义排序键函数，按照汉字拼音首字母排序
        def get_pinyin_sort_key(s):
            # 简单的拼音首字母排序规则
            # 对于常见的二级单位名称进行硬编码排序
            pinyin_map = {
                '上海电力': 'shanghai',
                '上海能科': 'shanghai',
                '东北公司': 'dongbei',
                '中国电力': 'zhongguo',
                '云南国际': 'yunnan',
                '五凌电力': 'wuling',
                '内蒙古公司': 'neimenggu',
                '北京公司': 'beijing',
                '吉电股份': 'jidian',
                '四川公司': 'sichuan',
                '国能生物': 'guoneng',
                '安徽公司': 'anhui',
                '山东能源': 'shandong',
                '山西公司': 'shanxi',
                '工程公司': 'gongcheng',
                '广东公司': 'guangdong',
                '广西公司': 'guangxi',
                '新疆能源化工': 'xinjiang',
                '江苏公司': 'jiangsu',
                '江西公司': 'jiangxi',
                '河南公司': 'henan',
                '浙江公司': 'zhejiang',
                '海南公司': 'hainan',
                '湖北公司': 'hubei',
                '湖南公司': 'hunan',
                '甘肃公司': 'gansu',
                '福建公司': 'fujian',
                '贵州公司': 'guizhou',
                '重庆公司': 'chongqing',
                '陕西公司': 'shaanxi',
                '黄河公司': 'huanghe',
                '黑龙江公司': 'heilongjiang'
            }
            # 如果在映射表中，直接返回对应的拼音
            if s in pinyin_map:
                return pinyin_map[s]
            # 否则返回原字符串（英文字符会自然排序）
            return s
            
        secondary_units = sorted([r[0] for r in secondary_units_query if r[0]], key=get_pinyin_sort_key)
        
        # 如果没有选择二级单位，默认选择第一个
        if not selected_unit and secondary_units:
            selected_unit = secondary_units[0]
    else:
        secondary_units = [current_user.username]
        selected_unit = current_user.username
    
    # 构建项目查询
    query_builder = Project.query
    
    # 根据权限和选择的二级单位筛选项目
    if not is_admin:
        query_builder = query_builder.filter(Project.secondary_unit == current_user.username)
    elif selected_unit:
        query_builder = query_builder.filter(Project.secondary_unit == selected_unit)
    
    # 获取当前日期，用于判断数据提交时间是否超过30天
    current_date = datetime.now()
    thirty_days_ago = current_date - timedelta(days=30)
    
    # 获取项目列表并准备数据表格数据
    projects = query_builder.order_by(Project.project_name).all()
    data = []
    for proj in projects:
        # 处理能源局核发平台数据
        nyj_status = 'normal-status'
        nyj_display = proj.data_nyj_updated_at.strftime('%Y-%m-%d %H:%M:%S') if proj.data_nyj_updated_at else '-'
        if proj.data_nyj_updated_at:
            # 判断提交时间是否距离当前时间超过30天
            if proj.data_nyj_updated_at < thirty_days_ago:
                nyj_status = 'mild-warning'
        else:
            nyj_status = 'severe-warning'
        
        # 处理绿证交易平台数据
        lzy_status = 'normal-status'
        lzy_display = proj.data_lzy_updated_at.strftime('%Y-%m-%d %H:%M:%S') if proj.data_lzy_updated_at else '-'
        if proj.data_lzy_updated_at:
            # 判断提交时间是否距离当前时间超过30天
            if proj.data_lzy_updated_at < thirty_days_ago:
                lzy_status = 'mild-warning'
        else:
            # 判断是否未注册
            if not proj.is_green_cert_registered:
                lzy_display = '未注册'
                lzy_status = 'mild-warning'
            elif not proj.has_green_cert_transaction:
                lzy_display = '尚无交易'
                lzy_status = 'mild-warning'
            else:
                lzy_status = 'severe-warning'
        
        # 处理北京电力交易中心数据
        bjdl_status = 'normal-status'
        bjdl_display = proj.data_bjdl_updated_at.strftime('%Y-%m-%d %H:%M:%S') if proj.data_bjdl_updated_at else '-'
        if proj.data_bjdl_updated_at:
            # 判断提交时间是否距离当前时间超过30天
            if proj.data_bjdl_updated_at < thirty_days_ago:
                bjdl_status = 'mild-warning'
        else:
            # 判断是否未注册
            if not proj.is_beijing_registered:
                bjdl_display = '未注册'
                bjdl_status = 'mild-warning'
            elif not proj.has_beijing_transaction:
                bjdl_display = '尚无交易'
                bjdl_status = 'mild-warning'
            else:
                bjdl_status = 'severe-warning'
        
        # 处理广州电力交易中心数据
        gjdl_status = 'normal-status'
        gjdl_display = proj.data_gjdl_updated_at.strftime('%Y-%m-%d %H:%M:%S') if proj.data_gjdl_updated_at else '-'
        if proj.data_gjdl_updated_at:
            # 判断提交时间是否距离当前时间超过30天
            if proj.data_gjdl_updated_at < thirty_days_ago:
                gjdl_status = 'mild-warning'
        else:
            # 判断是否未注册
            if not proj.is_guangzhou_registered:
                gjdl_display = '未注册'
                gjdl_status = 'mild-warning'
            elif not proj.has_guangzhou_transaction:
                gjdl_display = '尚无交易'
                gjdl_status = 'mild-warning'
            else:
                gjdl_status = 'severe-warning'
        
        data.append({
            'project_name': proj.project_name,
            'data_nyj_updated_at': nyj_display,
            'data_nyj_status': nyj_status,
            'data_lzy_updated_at': lzy_display,
            'data_lzy_status': lzy_status,
            'data_bjdl_display': bjdl_display,
            'data_bjdl_status': bjdl_status,
            'data_gjdl_display': gjdl_display,
            'data_gjdl_status': gjdl_status
        })
    
    return render_template('data_submission_time.html',
                           secondary_units=secondary_units,
                           selected_unit=selected_unit,
                           data=data)

@dashboard_bp.route('/expected_price')
@login_required
def expected_price():
    """期望价格页面"""
    return render_template('expected_price.html')


# 期望价格API接口
@dashboard_bp.route('/api/expected_prices', methods=['GET'])
@login_required
def get_expected_prices():
    """获取当前用户的期望价格数据"""
    from models import ExpectedPrice, SystemSetting
    
    # 检查是否允许查看所有单位的价格
    show_all = False
    if not current_user.is_admin:
        setting = SystemSetting.query.filter_by(key='show_all_prices').first()
        if setting and setting.value == 'true':
            show_all = True
    else:
        show_all = True
    
    if show_all:
        # 管理员或设置允许查看所有
        prices = ExpectedPrice.query.all()
    else:
        # 只查看自己单位的
        # 获取用户的二级单位
        user_projects = Project.query.filter_by(secondary_unit=current_user.username).first()
        if not user_projects:
            return jsonify([])
        
        secondary_unit = user_projects.secondary_unit
        prices = ExpectedPrice.query.filter_by(secondary_unit=secondary_unit).all()
    
    result = []
    for price in prices:
        result.append({
            'id': price.id,
            'secondary_unit': price.secondary_unit,
            'production_year': price.production_year,
            'price': float(price.price),
            'created_at': price.created_at.isoformat() if price.created_at else None,
            'updated_at': price.updated_at.isoformat() if price.updated_at else None
        })
    
    return jsonify(result)


@dashboard_bp.route('/api/expected_prices', methods=['POST'])
@login_required
def create_expected_price():
    """创建期望价格"""
    from models import ExpectedPrice
    
    data = request.json
    if not data or 'production_year' not in data or 'price' not in data:
        return jsonify({'error': '缺少必要参数'}), 400
    
    # 获取用户的二级单位
    user_projects = Project.query.filter_by(secondary_unit=current_user.username).first()
    if not user_projects and not current_user.is_admin:
        return jsonify({'error': '未找到您的二级单位信息'}), 400
    
    secondary_unit = current_user.username if current_user.is_admin else user_projects.secondary_unit
    
    # 检查是否已存在该年份的价格
    existing = ExpectedPrice.query.filter_by(
        secondary_unit=secondary_unit,
        production_year=data['production_year']
    ).first()
    
    if existing:
        return jsonify({'error': f'已存在{data["production_year"]}年的期望价格'}), 400
    
    # 创建新记录
    new_price = ExpectedPrice(
        secondary_unit=secondary_unit,
        production_year=data['production_year'],
        price=data['price']
    )
    
    db.session.add(new_price)
    db.session.commit()
    
    return jsonify({
        'id': new_price.id,
        'secondary_unit': new_price.secondary_unit,
        'production_year': new_price.production_year,
        'price': float(new_price.price),
        'created_at': new_price.created_at.isoformat() if new_price.created_at else None,
        'updated_at': new_price.updated_at.isoformat() if new_price.updated_at else None
    }), 201


@dashboard_bp.route('/api/expected_prices/<int:price_id>', methods=['PUT'])
@login_required
def update_expected_price(price_id):
    """更新期望价格"""
    from models import ExpectedPrice
    
    data = request.json
    if not data or 'price' not in data:
        return jsonify({'error': '缺少必要参数'}), 400
    
    price = ExpectedPrice.query.get_or_404(price_id)
    
    # 检查权限
    if not current_user.is_admin and price.secondary_unit != current_user.username:
        user_projects = Project.query.filter_by(secondary_unit=current_user.username).first()
        if not user_projects or price.secondary_unit != user_projects.secondary_unit:
            return jsonify({'error': '无权修改此记录'}), 403
    
    # 更新价格
    price.price = data['price']
    if 'production_year' in data:
        price.production_year = data['production_year']
    
    db.session.commit()
    
    return jsonify({
        'id': price.id,
        'secondary_unit': price.secondary_unit,
        'production_year': price.production_year,
        'price': float(price.price),
        'created_at': price.created_at.isoformat() if price.created_at else None,
        'updated_at': price.updated_at.isoformat() if price.updated_at else None
    })


@dashboard_bp.route('/api/expected_prices/<int:price_id>', methods=['DELETE'])
@login_required
def delete_expected_price(price_id):
    """删除期望价格"""
    from models import ExpectedPrice
    
    price = ExpectedPrice.query.get_or_404(price_id)
    
    # 检查权限
    if not current_user.is_admin and price.secondary_unit != current_user.username:
        user_projects = Project.query.filter_by(secondary_unit=current_user.username).first()
        if not user_projects or price.secondary_unit != user_projects.secondary_unit:
            return jsonify({'error': '无权删除此记录'}), 403
    
    db.session.delete(price)
    db.session.commit()
    
    return jsonify({'message': '删除成功'})


@dashboard_bp.route('/api/expected_prices/all', methods=['GET'])
@login_required
def get_all_expected_prices():
    """管理员获取所有期望价格数据"""
    from models import ExpectedPrice
    
    if not current_user.is_admin:
        return jsonify({'error': '无权访问'}), 403
    
    prices = ExpectedPrice.query.all()
    
    result = []
    for price in prices:
        result.append({
            'id': price.id,
            'secondary_unit': price.secondary_unit,
            'production_year': price.production_year,
            'price': float(price.price),
            'created_at': price.created_at.isoformat() if price.created_at else None,
            'updated_at': price.updated_at.isoformat() if price.updated_at else None
        })
    
    return jsonify(result)


@dashboard_bp.route('/api/settings/show_all_prices', methods=['GET'])
@login_required
def get_show_all_prices_setting():
    """获取是否显示所有价格的设置"""
    from models import SystemSetting
    
    if not current_user.is_admin:
        return jsonify({'error': '无权访问'}), 403
    
    setting = SystemSetting.query.filter_by(key='show_all_prices').first()
    
    if not setting:
        # 默认为false
        setting = SystemSetting(
            key='show_all_prices',
            value='false',
            description='是否允许二级单位查看所有单位的期望价格'
        )
        db.session.add(setting)
        db.session.commit()
    
    return jsonify({
        'key': setting.key,
        'value': setting.value,
        'description': setting.description
    })


@dashboard_bp.route('/api/dashboard_chart_data', methods=['GET'])
@login_required
def get_dashboard_chart_data():
    """获取数据概览页面图表所需的真实数据"""
    # 获取所有项目ID（管理员可以看所有，非管理员只能看自己的）
    query = Project.query
    if not current_user.is_admin:
        query = query.filter(Project.secondary_unit == current_user.username)
    
    projects = query.all()
    project_ids_list = [p.id for p in projects]
    
    if not project_ids_list:
        return jsonify({'volume_data': [], 'price_data': []})
    
    # 定义需要查询的年份和月份
    production_years = [2023, 2024, 2025]
    transaction_months = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08']
    
    volume_data = []
    price_data = []
    
    with db.engine.connect() as connection:
        for month in transaction_months:
            month_volume = {'month': month, 'data_2023': 0, 'data_2024': 0, 'data_2025': 0}
            month_price = {'month': month, 'data_2023': 0, 'data_2024': 0, 'data_2025': 0}
            
            for prod_year in production_years:
                # 构建生产年月范围
                prod_start = f"{prod_year}-01"
                prod_end = f"{prod_year}-12"
                
                # 构建交易时间范围
                trans_start = f"{month}-01 00:00:00"
                if month.endswith('02'):
                    trans_end = f"{month}-28 23:59:59"
                elif month.endswith(('04', '06', '09', '11')):
                    trans_end = f"{month}-30 23:59:59"
                else:
                    trans_end = f"{month}-31 23:59:59"
                
                # 查询各平台数据
                total_qty = 0
                total_amt = 0
                
                # 1. 绿证平台-单向挂牌
                unilateral_sql = text("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as qty,
                        COALESCE(SUM(CASE WHEN total_quantity != '' AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as amt
                    FROM gzpt_unilateral_listings 
                    WHERE project_id IN :project_ids 
                    AND generate_ym BETWEEN :prod_start AND :prod_end
                    AND order_time_str BETWEEN :trans_start AND :trans_end
                    AND order_status = '1'
                """)
                result = connection.execute(unilateral_sql, {
                    'project_ids': tuple(project_ids_list),
                    'prod_start': prod_start,
                    'prod_end': prod_end,
                    'trans_start': trans_start,
                    'trans_end': trans_end
                }).fetchone()
                total_qty += float(result[0] or 0)
                total_amt += float(result[1] or 0)
                
                # 2. 绿证平台-双边线下
                offline_sql = text("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as qty,
                        COALESCE(SUM(CASE WHEN total_quantity != '' AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as amt
                    FROM gzpt_bilateral_offline_trades 
                    WHERE project_id IN :project_ids 
                    AND generate_ym BETWEEN :prod_start AND :prod_end
                    AND order_time_str BETWEEN :trans_start AND :trans_end
                    AND order_status = '3'
                """)
                result = connection.execute(offline_sql, {
                    'project_ids': tuple(project_ids_list),
                    'prod_start': prod_start,
                    'prod_end': prod_end,
                    'trans_start': trans_start,
                    'trans_end': trans_end
                }).fetchone()
                total_qty += float(result[0] or 0)
                total_amt += float(result[1] or 0)
                
                # 3. 绿证平台-双边线上
                online_sql = text("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN total_quantity IS NOT NULL AND total_quantity != '' THEN CAST(total_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as qty,
                        COALESCE(SUM(CASE WHEN total_quantity != '' AND total_amount != '' THEN CAST(total_amount AS DECIMAL(15,2)) ELSE 0 END), 0) as amt
                    FROM gzpt_bilateral_online_trades 
                    WHERE project_id IN :project_ids 
                    AND generate_ym BETWEEN :prod_start AND :prod_end
                    AND order_time_str BETWEEN :trans_start AND :trans_end
                """)
                result = connection.execute(online_sql, {
                    'project_ids': tuple(project_ids_list),
                    'prod_start': prod_start,
                    'prod_end': prod_end,
                    'trans_start': trans_start,
                    'trans_end': trans_end
                }).fetchone()
                total_qty += float(result[0] or 0)
                total_amt += float(result[1] or 0)
                
                # 4. 北京电力交易中心
                beijing_sql = text("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN transaction_quantity IS NOT NULL AND transaction_quantity != '' THEN CAST(transaction_quantity AS DECIMAL(15,2)) ELSE 0 END), 0) as qty,
                        COALESCE(SUM(CASE WHEN transaction_quantity != '' AND transaction_price != '' THEN CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,2)) ELSE 0 END), 0) as amt
                    FROM beijing_power_exchange_trades 
                    WHERE project_id IN :project_ids 
                    AND production_year_month BETWEEN :prod_start AND :prod_end
                    AND transaction_time BETWEEN :trans_start AND :trans_end
                """)
                result = connection.execute(beijing_sql, {
                    'project_ids': tuple(project_ids_list),
                    'prod_start': prod_start,
                    'prod_end': prod_end,
                    'trans_start': trans_start,
                    'trans_end': trans_end
                }).fetchone()
                total_qty += float(result[0] or 0)
                total_amt += float(result[1] or 0)
                
                # 5. 广州电力交易中心
                guangzhou_sql = text("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN gpc_certifi_num IS NOT NULL AND gpc_certifi_num != 0 THEN CAST(gpc_certifi_num AS DECIMAL(15,2)) ELSE 0 END), 0) as qty,
                        COALESCE(SUM(CASE WHEN gpc_certifi_num != 0 AND total_cost != 0 THEN CAST(total_cost AS DECIMAL(15,2)) ELSE 0 END), 0) as amt
                    FROM guangzhou_power_exchange_trades 
                    WHERE project_id IN :project_ids 
                    AND CONCAT(SUBSTRING(product_date, 1, 4), '-', LPAD(SUBSTRING(product_date, 6), 2, '0')) BETWEEN :prod_start AND :prod_end
                    AND deal_time BETWEEN :trans_start AND :trans_end
                """)
                result = connection.execute(guangzhou_sql, {
                    'project_ids': tuple(project_ids_list),
                    'prod_start': prod_start,
                    'prod_end': prod_end,
                    'trans_start': trans_start,
                    'trans_end': trans_end
                }).fetchone()
                total_qty += float(result[0] or 0)
                total_amt += float(result[1] or 0)
                
                # 计算平均价格
                avg_price = round(total_amt / total_qty, 2) if total_qty > 0 else 0
                
                # 存储数据
                month_volume[f'data_{prod_year}'] = total_qty
                month_price[f'data_{prod_year}'] = avg_price
            
            volume_data.append(month_volume)
            price_data.append(month_price)
    
    return jsonify({
        'volume_data': volume_data,
        'price_data': price_data
    })


@dashboard_bp.route('/api/settings/show_all_prices', methods=['PUT'])
@login_required
def update_show_all_prices_setting():
    """更新是否显示所有价格的设置"""
    from models import SystemSetting
    
    if not current_user.is_admin:
        return jsonify({'error': '无权访问'}), 403
    
    data = request.json
    if not data or 'value' not in data:
        return jsonify({'error': '缺少必要参数'}), 400
    
    setting = SystemSetting.query.filter_by(key='show_all_prices').first()
    
    if not setting:
        setting = SystemSetting(
            key='show_all_prices',
            value=data['value'],
            description='是否允许二级单位查看所有单位的期望价格'
        )
        db.session.add(setting)
    else:
        setting.value = data['value']
    
    db.session.commit()
    
    return jsonify({
        'key': setting.key,
        'value': setting.value,
        'description': setting.description
    })



@dashboard_bp.route('/data_audit/export_discrepancies')
@login_required
def export_discrepancies():
    """
    数据审计功能：
    找出在 nyj_green_certificate_ledger 表的 'sold_quantity' 列总和
    不等于在 nyj_transaction_records 表的 'transaction_num' 列总和的项目，
    并将结果导出到 Excel。
    """
    
    # 核心SQL查询，用于查找差异
    sql_query = text("""
        WITH LedgerSummary AS (
            -- 1. 从绿证台账表计算每个项目的已售出总量
            SELECT
                project_id,
                SUM(CAST(sold_quantity AS DECIMAL(15,2))) AS total_sold_ledger
            FROM
                nyj_green_certificate_ledger
            WHERE
                sold_quantity IS NOT NULL AND sold_quantity != ''
            GROUP BY
                project_id
        ),
        TransactionSummary AS (
            -- 2. 从交易记录表计算每个项目的交易数量总量
            SELECT
                project_id,
                SUM(CAST(transaction_num AS DECIMAL(15,2))) AS total_transactions
            FROM
                nyj_transaction_records
            WHERE
                transaction_num IS NOT NULL AND transaction_num != ''
            GROUP BY
                project_id
        )
        -- 3. 主查询：关联项目信息，并找出两个汇总值不相等的项目
        SELECT
            p.id AS "项目ID",
            p.project_name AS "项目名称",
            COALESCE(ls.total_sold_ledger, 0) AS "台账已售出量总和",
            COALESCE(ts.total_transactions, 0) AS "交易记录数量总和"
        FROM
            projects p
        LEFT JOIN
            LedgerSummary ls ON p.id = ls.project_id
        LEFT JOIN
            TransactionSummary ts ON p.id = ts.project_id
        WHERE
            -- 核心筛选条件：找出两个值不相等的记录
            COALESCE(ls.total_sold_ledger, 0) != COALESCE(ts.total_transactions, 0)
        ORDER BY
            p.id;
    """)

    try:
        with db.engine.connect() as connection:
            # 执行查询并将结果直接读入Pandas DataFrame
            df = pd.read_sql_query(sql_query, connection)

        # 如果没有差异，也生成一个空的Excel，并告知用户
        if df.empty:
            flash('数据核对完成，未发现任何差异！')
            # 创建一个空的DataFrame用于生成空的Excel文件
            df = pd.DataFrame(columns=["项目ID", "项目名称", "台账已售出量总和", "交易记录数量总和"])

        # 使用 io.BytesIO 在内存中创建Excel文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Discrepancies')
            # 自动调整列宽
            for column in df:
                column_length = max(df[column].astype(str).map(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                writer.sheets['Discrepancies'].set_column(col_idx, col_idx, column_length + 2)

        output.seek(0)

        # 发送文件供用户下载
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='data_discrepancies.xlsx'
        )

    except Exception as e:
        flash(f'导出数据差异时发生错误: {str(e)}')
        return redirect(url_for('dashboard.overview'))


# FAQ模块路由
@dashboard_bp.route('/faq')
@login_required
def faq():
    """FAQ页面 - 显示所有活跃的FAQ"""
    faqs = FAQ.query.filter_by(is_active=True).order_by(FAQ.created_at.desc()).all()
    return render_template('faq.html', faqs=faqs)

@dashboard_bp.route('/api/faq', methods=['GET'])
@login_required
def get_faqs():
    """获取FAQ列表API"""
    faqs = FAQ.query.filter_by(is_active=True).order_by(FAQ.created_at.desc()).all()
    return jsonify({
        'success': True,
        'data': [{
            'id': faq.id,
            'question': faq.question,
            'answer': faq.answer,
            'created_at': faq.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'created_by': faq.creator.username
        } for faq in faqs]
    })

@dashboard_bp.route('/api/faq', methods=['POST'])
@login_required
def create_faq():
    """创建新FAQ - 仅管理员可用"""
    if not current_user.is_admin:
        return jsonify({'success': False, 'message': '权限不足，仅管理员可以创建FAQ'}), 403
    
    data = request.get_json()
    question = data.get('question', '').strip()
    answer = data.get('answer', '').strip()
    
    if not question or not answer:
        return jsonify({'success': False, 'message': '问题和答案不能为空'}), 400
    
    try:
        faq = FAQ(
            question=question,
            answer=answer,
            created_by=current_user.id
        )
        db.session.add(faq)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'FAQ创建成功',
            'data': {
                'id': faq.id,
                'question': faq.question,
                'answer': faq.answer,
                'created_at': faq.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'created_by': faq.creator.username
            }
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'创建FAQ失败: {str(e)}'}), 500

@dashboard_bp.route('/api/faq/<int:faq_id>', methods=['PUT'])
@login_required
def update_faq(faq_id):
    """更新FAQ - 仅管理员可用"""
    if not current_user.is_admin:
        return jsonify({'success': False, 'message': '权限不足，仅管理员可以编辑FAQ'}), 403
    
    faq = FAQ.query.get_or_404(faq_id)
    data = request.get_json()
    
    question = data.get('question', '').strip()
    answer = data.get('answer', '').strip()
    
    if not question or not answer:
        return jsonify({'success': False, 'message': '问题和答案不能为空'}), 400
    
    try:
        faq.question = question
        faq.answer = answer
        faq.updated_at = datetime.now()
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'FAQ更新成功',
            'data': {
                'id': faq.id,
                'question': faq.question,
                'answer': faq.answer,
                'updated_at': faq.updated_at.strftime('%Y-%m-%d %H:%M:%S')
            }
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'更新FAQ失败: {str(e)}'}), 500

@dashboard_bp.route('/api/faq/<int:faq_id>', methods=['DELETE'])
@login_required
def delete_faq(faq_id):
    """删除FAQ - 仅管理员可用"""
    if not current_user.is_admin:
        return jsonify({'success': False, 'message': '权限不足，仅管理员可以删除FAQ'}), 403
    
    faq = FAQ.query.get_or_404(faq_id)
    
    try:
        # 软删除：设置为非活跃状态
        faq.is_active = False
        faq.updated_at = datetime.now()
        db.session.commit()
        
        return jsonify({'success': True, 'message': 'FAQ删除成功'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'删除FAQ失败: {str(e)}'}), 500