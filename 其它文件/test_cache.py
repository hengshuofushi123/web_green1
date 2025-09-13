#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试广交绿证价格缓存功能
"""

import requests
import time
import json
from datetime import datetime

def test_green_cert_price_cache():
    """测试绿证价格缓存功能"""
    base_url = "http://127.0.0.1:8000"
    
    # 模拟登录（这里简化处理，实际需要先登录获取session）
    session = requests.Session()
    
    print("=== 广交绿证价格缓存功能测试 ===")
    print(f"测试时间: {datetime.now()}")
    print()
    
    # 第一次请求 - 应该从API获取数据
    print("1. 第一次请求（应该从API获取数据）:")
    try:
        response1 = session.get(f"{base_url}/dashboard/api/green_cert_price")
        if response1.status_code == 200:
            data1 = response1.json()
            print(f"   状态: {data1.get('success')}")
            print(f"   来源: {'缓存' if data1.get('from_cache') else 'API'}")
            print(f"   当月价格: {data1.get('data', {}).get('currentMonthPrice')}")
            print(f"   年累价格: {data1.get('data', {}).get('annualCumulativePrice')}")
        else:
            print(f"   请求失败: {response1.status_code}")
            print(f"   响应: {response1.text}")
    except Exception as e:
        print(f"   错误: {e}")
    
    print()
    
    # 等待1秒后第二次请求 - 应该从缓存获取数据
    print("2. 第二次请求（应该从缓存获取数据）:")
    time.sleep(1)
    try:
        response2 = session.get(f"{base_url}/dashboard/api/green_cert_price")
        if response2.status_code == 200:
            data2 = response2.json()
            print(f"   状态: {data2.get('success')}")
            print(f"   来源: {'缓存' if data2.get('from_cache') else 'API'}")
            print(f"   当月价格: {data2.get('data', {}).get('currentMonthPrice')}")
            print(f"   年累价格: {data2.get('data', {}).get('annualCumulativePrice')}")
        else:
            print(f"   请求失败: {response2.status_code}")
            print(f"   响应: {response2.text}")
    except Exception as e:
        print(f"   错误: {e}")
    
    print()
    print("=== 测试完成 ===")
    print("注意: 如果看到401错误，说明需要先登录。缓存功能本身应该正常工作。")

if __name__ == "__main__":
    test_green_cert_price_cache()