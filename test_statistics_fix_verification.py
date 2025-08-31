import requests
import json
from datetime import datetime

# 测试配置
BASE_URL = "http://127.0.0.1:5000"
USERNAME = "admin"
PASSWORD = "biNj5GIVwZ0PMrmX"

def test_statistics_api():
    """测试修改后的统计API是否与query_platform_volumes_and_prices.py结果一致"""
    
    # 创建会话
    session = requests.Session()
    
    try:
        # 1. 登录
        print("正在登录...")
        login_data = {
            'username': USERNAME,
            'password': PASSWORD
        }
        
        login_response = session.post(f"{BASE_URL}/login", data=login_data, allow_redirects=False)
        
        if login_response.status_code not in [200, 302]:
            print(f"登录失败: {login_response.status_code}")
            print(f"响应内容: {login_response.text}")
            return
            
        print("登录成功")
        
        # 2. 测试统计API
        print("\n正在测试统计API...")
        
        # 测试参数
        params = {
            'dimension': '省份',
            'start_date': '2024-01',
            'end_date': '2024-12',
            'transaction_start_date': '2025-03-01',
            'transaction_end_date': '2025-03-31'
        }
        
        # 运行check_duplicate_transactions.py检查跨平台重复情况
        print("\n=== 运行 check_duplicate_transactions.py ===")
        import subprocess
        import sys
        import os
        
        result = subprocess.run([sys.executable, 'check_duplicate_transactions.py'], 
                              capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode == 0:
            output_lines = result.stdout.strip().split('\n')
            unique_total = None
            multiple_platform_count = 0
            
            for line in output_lines:
                if '去重后总交易量:' in line:
                    unique_total = float(line.split(':')[1].strip())
                elif '条记录在多个平台有交易' in line:
                    multiple_platform_count = int(line.split()[1])
            
            print(f"发现 {multiple_platform_count} 条记录在多个平台有交易")
            print(f"去重后总交易量: {unique_total}")
        else:
            print(f"运行check_duplicate_transactions.py失败: {result.stderr}")
            unique_total = None
            multiple_platform_count = 0
        
        # 调用统计页面
        stats_response = session.get(f"{BASE_URL}/dashboard/statistics", params=params)
        
        if stats_response.status_code != 200:
            print(f"统计页面调用失败: {stats_response.status_code}")
            print(f"响应内容: {stats_response.text}")
            return
            
        # 解析HTML响应中的数据
        html_content = stats_response.text
        
        # 查找表格数据（通过正则表达式或BeautifulSoup）
        import re
        
        # 查找售出量(交易平台)的数据
        # 在HTML中查找表格行数据，注意第一行通常是汇总行
        trading_platform_pattern = r'<td[^>]*>([\d,]+)</td>\s*<td[^>]*>[\d.]+%</td>\s*<td[^>]*>[\d.]+%</td>'
        matches = re.findall(trading_platform_pattern, html_content)
        
        if matches:
            print("\n找到的交易平台售出量数据:")
            for i, match in enumerate(matches):
                value = int(match.replace(',', ''))
                print(f"  第{i+1}行: {value}")
            
            # 第一行通常是汇总行，直接使用第一行的数据
            if len(matches) > 0:
                dashboard_total = int(matches[0].replace(',', ''))
                print(f"\n=== 测试结果 ===")
                print(f"Dashboard显示的交易平台售出量（第一行汇总）: {dashboard_total}")
                print(f"query_platform_volumes_and_prices.py计算结果: 159306（仅广州电力交易中心，避免重复计算）")
                print(f"差异: {dashboard_total - 159306}")
                
                if dashboard_total == 159306:
                    print("✅ 数据一致！修改成功")
                else:
                    print("❌ 数据仍不一致，需要进一步调试")
                    print("注意：Dashboard避免重复计算，而query_platform_volumes_and_prices.py查询了所有平台")
                    print(f"\n详细分析:")
                    print(f"  - Dashboard第一行（汇总）: {dashboard_total}")
                    if len(matches) > 1:
                        detail_sum = sum(int(match.replace(',', '')) for match in matches[1:])
                        print(f"  - 其他行总和: {detail_sum}")
                        print(f"  - 是否存在重复计算: {'是' if dashboard_total != detail_sum else '否'}")
            else:
                print("未找到有效的交易平台售出量数据")
        else:
            # 尝试更简单的方法：查找所有数字
            print("\n无法通过正则表达式解析，尝试查找页面中的关键数据...")
            
            # 查找包含"售出量(交易平台)"的表格数据
            if "售出量" in html_content and "交易平台" in html_content:
                print("页面包含交易平台售出量数据，但解析失败")
                print("需要手动检查页面内容")
                
                # 保存HTML到文件以便检查
                with open('debug_statistics_response.html', 'w', encoding='utf-8') as f:
                    f.write(html_content)
                print("已保存HTML响应到 debug_statistics_response.html")
            else:
                print("页面不包含预期的交易平台售出量数据")
            
    except requests.exceptions.RequestException as e:
        print(f"请求异常: {e}")
    except Exception as e:
        print(f"其他异常: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    test_statistics_api()