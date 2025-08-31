#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
from bs4 import BeautifulSoup

def test_dashboard_api():
    """直接测试dashboard API，获取实际返回的数据"""
    
    BASE_URL = "http://127.0.0.1:5000"
    
    # 创建session
    session = requests.Session()
    
    # 1. 先访问登录页面获取session
    print("正在访问登录页面...")
    session.get(f"{BASE_URL}/login")
    
    # 2. 登录
    print("正在登录...")
    login_data = {
        'username': 'admin',
        'password': 'admin123'
    }
    
    login_response = session.post(f"{BASE_URL}/login", data=login_data, allow_redirects=False)
    
    print(f"登录响应状态码: {login_response.status_code}")
    print(f"登录响应头: {dict(login_response.headers)}")
    
    # 检查是否重定向（成功登录通常会重定向）
    if login_response.status_code in [302, 303, 307, 308]:
        print("登录成功（重定向）")
    elif login_response.status_code == 200:
        # 检查响应内容是否包含错误信息
        if 'error' in login_response.text.lower() or '登录失败' in login_response.text:
            print(f"登录失败: {login_response.text[:200]}")
            return
        else:
            print("登录成功")
    else:
        print(f"登录失败: {login_response.status_code}")
        print(f"响应内容: {login_response.text[:200]}")
        return
    
    # 2. 调用统计API
    print("\n正在调用统计API...")
    
    # 测试参数
    params = {
        'dimension': '省份',
        'start_date': '2024-01',
        'end_date': '2024-12',
        'transaction_start_date': '2025-03-01',
        'transaction_end_date': '2025-03-31'
    }
    
    print(f"请求参数: {params}")
    
    # 调用统计页面
    stats_response = session.get(f"{BASE_URL}/dashboard/statistics", params=params)
    
    if stats_response.status_code != 200:
        print(f"统计页面调用失败: {stats_response.status_code}")
        print(f"响应内容: {stats_response.text}")
        return
        
    print("API调用成功")
    
    # 3. 解析HTML响应
    html_content = stats_response.text
    
    # 保存HTML到文件以便检查
    with open('debug_dashboard_response.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print("已保存HTML响应到 debug_dashboard_response.html")
    
    # 使用BeautifulSoup解析
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 查找表格
    tables = soup.find_all('table')
    print(f"\n找到 {len(tables)} 个表格")
    
    for i, table in enumerate(tables):
        print(f"\n=== 表格 {i+1} ===")
        rows = table.find_all('tr')
        print(f"表格行数: {len(rows)}")
        
        # 查找表头
        headers = []
        if rows:
            header_row = rows[0]
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
            print(f"表头: {headers}")
            
            # 查找包含"售出量(交易平台)"的列
            trading_platform_col = None
            for j, header in enumerate(headers):
                if '售出量' in header and '交易平台' in header:
                    trading_platform_col = j
                    print(f"找到交易平台售出量列: 第{j+1}列")
                    break
            
            if trading_platform_col is not None:
                print("\n交易平台售出量数据:")
                for row_idx, row in enumerate(rows[1:], 1):  # 跳过表头
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > trading_platform_col:
                        value_text = cells[trading_platform_col].get_text().strip()
                        # 移除逗号并转换为数字
                        try:
                            value = int(value_text.replace(',', ''))
                            print(f"  第{row_idx}行: {value} (原文本: '{value_text}')")
                        except ValueError:
                            print(f"  第{row_idx}行: 无法解析 '{value_text}'")
    
    # 4. 尝试用正则表达式查找数据
    print("\n=== 正则表达式查找 ===")
    
    # 查找所有数字（可能是售出量数据）
    number_pattern = r'>(\d{1,3}(?:,\d{3})*)<'
    numbers = re.findall(number_pattern, html_content)
    
    print(f"找到的数字: {numbers[:20]}...")  # 只显示前20个
    
    # 查找可能的售出量数据（大于100000的数字）
    large_numbers = []
    for num_str in numbers:
        try:
            num = int(num_str.replace(',', ''))
            if num > 100000:  # 假设售出量都大于10万
                large_numbers.append(num)
        except ValueError:
            continue
    
    print(f"\n可能的售出量数据（>100000）: {large_numbers}")
    
    # 查找177870这个特定数字
    if '177870' in html_content or '177,870' in html_content:
        print("\n✓ 在HTML中找到177870")
    else:
        print("\n✗ 在HTML中未找到177870")
    
    if '159306' in html_content or '159,306' in html_content:
        print("✓ 在HTML中找到159306")
    else:
        print("✗ 在HTML中未找到159306")

if __name__ == "__main__":
    test_dashboard_api()