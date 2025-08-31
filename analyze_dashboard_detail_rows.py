#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
from bs4 import BeautifulSoup

def analyze_dashboard_details():
    """分析dashboard详细行数据，找出每行对应的省份和数据来源"""
    
    BASE_URL = "http://127.0.0.1:5000"
    
    # 创建session
    session = requests.Session()
    
    try:
        # 1. 登录
        print("正在登录...")
        login_data = {
            'username': 'admin',
            'password': 'biNj5GIVwZ0PMrmX'
        }
        
        login_response = session.post(f"{BASE_URL}/login", data=login_data, allow_redirects=False)
        
        if login_response.status_code not in [200, 302]:
            print(f"登录失败: {login_response.status_code}")
            print(f"响应内容: {login_response.text}")
            return
            
        print("登录成功")
        
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
        
        # 调用统计页面
        stats_response = session.get(f"{BASE_URL}/dashboard/statistics", params=params)
        
        if stats_response.status_code != 200:
            print(f"统计页面调用失败: {stats_response.status_code}")
            return
            
        # 3. 解析HTML响应
        html_content = stats_response.text
        
        # 保存HTML到文件以便调试
        with open('debug_dashboard_detail.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        print("已保存HTML响应到 debug_dashboard_detail.html")
        
        # 使用正则表达式查找数字模式，类似test_statistics_fix_verification.py
        # 查找所有可能的数字（包括带逗号的）
        number_pattern = r'\b\d{1,3}(?:,\d{3})*\b'
        all_numbers = re.findall(number_pattern, html_content)
        
        print(f"\n在HTML中找到的所有数字: {len(all_numbers)}")
        
        # 查找特定的数字模式，特别是我们知道的值
        known_values = [177870, 159306, 117955, 53042, 2312, 3218, 1343]
        found_values = []
        
        for num_str in all_numbers:
            try:
                num = int(num_str.replace(',', ''))
                if num in known_values:
                    found_values.append(num)
            except ValueError:
                continue
        
        print(f"找到的已知值: {found_values}")
        
        # 尝试更精确的模式匹配
        # 查找表格行模式
        table_row_pattern = r'<tr[^>]*>.*?</tr>'
        table_rows = re.findall(table_row_pattern, html_content, re.DOTALL | re.IGNORECASE)
        
        print(f"\n找到 {len(table_rows)} 个表格行")
        
        # 分析包含数字的行
        trading_platform_rows = []
        for i, row in enumerate(table_rows):
            # 查找行中的数字
            row_numbers = re.findall(number_pattern, row)
            if row_numbers:
                # 检查是否包含我们关心的值
                for num_str in row_numbers:
                    try:
                        num = int(num_str.replace(',', ''))
                        if num in known_values:
                            trading_platform_rows.append((i, row, num))
                            break
                    except ValueError:
                        continue
        
        print(f"\n找到包含已知值的行: {len(trading_platform_rows)}")
        
        # 分析每一行
        for row_idx, row_html, value in trading_platform_rows:
            print(f"\n行 {row_idx}: 值 {value}")
            # 尝试提取行中的文本内容
            row_text = re.sub(r'<[^>]+>', ' ', row_html)
            row_text = ' '.join(row_text.split())  # 清理空白字符
            print(f"行内容: {row_text[:100]}...")  # 只显示前100个字符
            
            # 分析这个值的含义
            if value == 177870:
                print("  -> 这是汇总行")
            elif value == 159306:
                print("  -> 这可能是广州电力交易中心的数据")
            elif value == 117955:
                print("  -> 这是第二大的值，可能是某个主要省份")
            elif value in [53042, 2312, 3218, 1343]:
                print(f"  -> 这是较小的值 {value}，可能是其他省份或平台")
        
        # 计算差异分析
        print(f"\n=== 差异分析 ===")
        print(f"Dashboard汇总: 177870")
        print(f"广州电力交易中心: 159306")
        print(f"差异: {177870 - 159306} = 18564")
        
        # 计算其他值的总和
        other_values = [117955, 53042, 2312, 3218, 1343]
        other_sum = sum(v for v in other_values if v != 159306)
        print(f"其他行的值: {other_values}")
        print(f"其他行总和: {other_sum}")
        print(f"159306 + {other_sum} = {159306 + other_sum}")
        
        if 159306 + other_sum == 177870:
            print("✅ 数字加起来等于汇总值")
        else:
            print("❌ 数字加起来不等于汇总值，需要进一步分析")
        
    except Exception as e:
        print(f"异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    analyze_dashboard_details()