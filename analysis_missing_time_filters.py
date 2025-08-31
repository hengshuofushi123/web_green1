#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚合分析页面缺少时间筛选器问题分析

问题描述：
用户报告聚合分析页面和数据概览页面在相同查询条件下数据不一致：
- 聚合分析页面：生产时间2024年，交易时间2025年3月，售出量166,206张
- 数据概览页面：2025年3月成交2024年生产的绿证159,306张
- 差异：6,900张

根本原因分析：
1. 聚合分析页面(transaction_analysis.html)缺少生产时间和交易时间选择器
2. 后端接口get_analysis_data和get_transaction_time_data不处理时间筛选参数
3. 用户无法指定具体的生产时间范围和交易时间范围
4. 导致聚合分析页面显示的是所有时间的数据，而不是指定时间范围的数据
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def analyze_missing_time_filters():
    """
    分析聚合分析页面缺少时间筛选器的问题
    """
    print("=" * 80)
    print("聚合分析页面缺少时间筛选器问题分析")
    print("=" * 80)
    
    print("\n1. 问题发现：")
    print("   - 用户期望：聚合分析页面能够同时筛选生产时间和交易时间")
    print("   - 实际情况：聚合分析页面只有项目筛选，没有时间筛选器")
    print("   - 对比页面：statistics.html和customer_analysis.html都有完整的时间筛选器")
    
    print("\n2. 代码分析：")
    print("   transaction_analysis.html:")
    print("   - ✗ 缺少生产时间选择器")
    print("   - ✗ 缺少交易时间选择器")
    print("   - ✓ 有按生产月汇总和按交易月汇总的Tab页")
    print("   - ✓ 有项目筛选功能")
    
    print("\n   statistics.html (对比):")
    print("   - ✓ 有电量生产时间选择器 (月范围)")
    print("   - ✓ 有交易时间选择器 (日范围)")
    print("   - ✓ 使用Element Plus日期选择器")
    
    print("\n   customer_analysis.html (对比):")
    print("   - ✓ 有电量生产时间选择器 (月范围)")
    print("   - ✓ 有交易时间选择器 (日范围)")
    print("   - ✓ 使用Element Plus日期选择器")
    
    print("\n3. 后端接口分析：")
    print("   get_analysis_data():")
    print("   - ✗ 不处理生产时间筛选参数")
    print("   - ✗ 不处理交易时间筛选参数")
    print("   - ✓ 按生产年月聚合数据")
    
    print("\n   get_transaction_time_data():")
    print("   - ✗ 不处理生产时间筛选参数")
    print("   - ✗ 不处理交易时间筛选参数")
    print("   - ✓ 按交易时间聚合数据")
    
    print("\n4. 数据不一致原因：")
    print("   - 聚合分析页面显示所有时间的数据汇总")
    print("   - 数据概览页面可以指定具体的生产年份和交易月份")
    print("   - 用户期望的是指定时间范围的数据，但聚合分析页面无法提供")
    
    print("\n5. 解决方案：")
    print("   A. 前端修改 (transaction_analysis.html):")
    print("      1. 添加生产时间选择器 (月范围)")
    print("      2. 添加交易时间选择器 (日范围)")
    print("      3. 在AJAX请求中包含时间筛选参数")
    print("      4. 参考statistics.html的实现")
    
    print("\n   B. 后端修改 (dashboard_routes.py):")
    print("      1. 修改get_analysis_data()函数")
    print("         - 添加start_date, end_date参数处理")
    print("         - 在SQL查询中添加生产时间筛选条件")
    print("      2. 修改get_transaction_time_data()函数")
    print("         - 添加transaction_start_date, transaction_end_date参数处理")
    print("         - 在SQL查询中添加交易时间筛选条件")
    print("         - 同时支持生产时间筛选")
    
    print("\n6. 具体修改点：")
    print("   前端 (transaction_analysis.html):")
    print("   - 在筛选表单中添加时间选择器")
    print("   - 在fetchAnalysisData()和fetchTransactionTimeData()中添加时间参数")
    
    print("\n   后端 (dashboard_routes.py):")
    print("   - get_analysis_data(): 添加生产时间筛选逻辑")
    print("   - get_transaction_time_data(): 添加交易时间和生产时间筛选逻辑")
    
    print("\n7. 预期效果：")
    print("   - 用户可以在聚合分析页面指定生产时间范围和交易时间范围")
    print("   - 聚合分析页面的数据将与数据概览页面在相同条件下保持一致")
    print("   - 解决用户报告的6,900张差异问题")
    
    print("\n" + "=" * 80)
    print("结论：聚合分析页面需要添加时间筛选器功能")
    print("=" * 80)

if __name__ == "__main__":
    analyze_missing_time_filters()