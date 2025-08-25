# 文件: utils.py
# 包含所有辅助函数、数据解析和转换函数

import os
import pandas as pd
from datetime import datetime
import secrets
from decimal import Decimal, InvalidOperation

def parse_lzy_datetime(datetime_str):
    """解析LZY的日期格式，支持两种格式：
    1. 旧格式: 'Mmm+DD,+YYYY,+H:MM:SS AM/PM'
    2. 新格式: '2025-08-04T15:34:28.000+08:00' (ISO 8601)
    """
    if not datetime_str or not isinstance(datetime_str, str):
        return None
    try:
        # 检查是否为ISO格式 (包含'T'和可能的时区信息)
        if 'T' in datetime_str:
            # 处理ISO格式，去除毫秒和时区部分
            iso_parts = datetime_str.split('.')[0]
            dt_object = datetime.strptime(iso_parts, '%Y-%m-%dT%H:%M:%S')
        else:
            # 处理旧格式
            cleaned_str = datetime_str.replace('+', ' ').replace('\u202f', ' ')
            dt_object = datetime.strptime(cleaned_str, '%b %d, %Y, %I:%M:%S %p')
        
        # 格式化为MySQL兼容的字符串
        return dt_object.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError) as e:
        print(f"日期解析错误: {datetime_str}, 错误: {e}")
        return None

def safe_int_cast(value):
    """安全地将值转换为整数，可以处理None和浮点数（如 5182.0）。"""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def generate_random_password(length=12):
    return secrets.token_urlsafe(length)

def update_pwd_excel(username, password):
    file = 'pwd.xlsx'
    if os.path.exists(file):
        df = pd.read_excel(file)
    else:
        df = pd.DataFrame(columns=['username', 'password'])
    if username in df['username'].values:
        df.loc[df['username'] == username, 'password'] = password
    else:
        new_row = pd.DataFrame({'username': [username], 'password': [password]})
        df = pd.concat([df, new_row], ignore_index=True)
    df.to_excel(file, index=False)

def project_to_dict(project):
    return {
        'id': project.id,
        'project_name': project.project_name or '',
        'secondary_unit': project.secondary_unit or '',
        'secondary_unit_contact': project.secondary_unit_contact or '',
        'province': project.province or '',
        'region': project.region or '',
        'company_name': project.company_name or '',
        'investment_scope': project.investment_scope or '',
        'project_nature': project.project_nature or '',
        'power_type': project.power_type or '',
        'capacity_mw': str(project.capacity_mw) if project.capacity_mw is not None else '',
        'production_year': str(project.production_year) if project.production_year is not None else '',
        'production_month': str(project.production_month) if project.production_month is not None else '',
        'last_updated_date': project.last_updated_date.strftime('%Y-%m-%d') if project.last_updated_date else '',
        'is_uhv_support': project.is_uhv_support,
        'has_subsidy': project.has_subsidy,
        'is_filed': project.is_filed,
        'is_beijing_registered': project.is_beijing_registered,
        'is_guangzhou_registered': project.is_guangzhou_registered,
    }

def populate_project_from_form(project, form_data):
    """一个辅助函数，用于从request.form数据填充项目对象"""
    project.project_name = form_data.get('project_name')
    project.secondary_unit = form_data.get('secondary_unit')

    date_str = form_data.get('last_updated_date')
    project.last_updated_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None

    project.secondary_unit_contact = form_data.get('secondary_unit_contact')
    project.province = form_data.get('province')
    project.region = form_data.get('region')
    project.company_name = form_data.get('company_name')
    project.investment_scope = form_data.get('investment_scope')
    project.project_nature = form_data.get('project_nature')
    project.power_type = form_data.get('power_type')

    cap_mw = form_data.get('capacity_mw')
    project.capacity_mw = float(cap_mw) if cap_mw else None

    prod_year = form_data.get('production_year')
    project.production_year = int(prod_year) if prod_year else None

    prod_month = form_data.get('production_month')
    project.production_month = int(prod_month) if prod_month else None

    project.is_uhv_support = 'is_uhv_support' in form_data
    project.has_subsidy = 'has_subsidy' in form_data
    project.is_filed = 'is_filed' in form_data
    project.is_beijing_registered = 'is_beijing_registered' in form_data
    project.is_guangzhou_registered = 'is_guangzhou_registered' in form_data

    return project