import pymysql
from datetime import datetime

# 连接MySQL数据库
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='root',
    database='green',
    charset='utf8mb4'
)
cursor = conn.cursor()

print('=== 分析吉电股份交易量差异原因 ===')

# 获取吉电股份的项目ID列表
cursor.execute("""
    SELECT id FROM projects WHERE secondary_unit = '吉电股份'
""")
project_ids = [row[0] for row in cursor.fetchall()]
start_time = '2024-01-01'
end_time = '2024-12-31'

print(f'项目数量: {len(project_ids)}')
print(f'测试时间范围: {start_time} 到 {end_time}')

# 1. 详细分析各平台的成交数据
print('\n=== 1. 各平台详细成交数据分析 ===')

# 北京电力交易中心详细分析
print('\n北京电力交易中心:')
cursor.execute("""
    SELECT 
        COUNT(*) as record_count,
        SUM(CAST(transaction_quantity AS DECIMAL(20,2))) as total_volume,
        MIN(transaction_time) as min_time,
        MAX(transaction_time) as max_time,
        COUNT(DISTINCT project_id) as project_count
    FROM beijing_power_exchange_trades bt
    JOIN projects p ON bt.project_id = p.id
    WHERE p.id IN %s
    AND bt.transaction_time >= %s AND bt.transaction_time <= %s
""", (tuple(project_ids), start_time, end_time))
result = cursor.fetchone()
print(f'  记录数: {result[0]}, 总成交量: {result[1]}, 项目数: {result[4]}')
print(f'  时间范围: {result[2]} - {result[3]}')

# 广州电力交易中心详细分析
print('\n广州电力交易中心:')
cursor.execute("""
    SELECT 
        COUNT(*) as record_count,
        SUM(CAST(gpc_certifi_num AS DECIMAL(20,2))) as total_volume,
        MIN(deal_time) as min_time,
        MAX(deal_time) as max_time,
        COUNT(DISTINCT project_id) as project_count
    FROM guangzhou_power_exchange_trades gt
    JOIN projects p ON gt.project_id = p.id
    WHERE p.id IN %s
    AND gt.deal_time >= %s AND gt.deal_time <= %s
""", (tuple(project_ids), start_time, end_time))
result = cursor.fetchone()
print(f'  记录数: {result[0]}, 总成交量: {result[1]}, 项目数: {result[4]}')
print(f'  时间范围: {result[2]} - {result[3]}')

# 2. 详细分析交易记录数据
print('\n=== 2. 交易记录详细分析 ===')
cursor.execute("""
    SELECT 
        COUNT(*) as record_count,
        SUM(CAST(transaction_num AS DECIMAL(20,2))) as total_volume,
        MIN(transaction_time) as min_time,
        MAX(transaction_time) as max_time,
        COUNT(DISTINCT project_id) as project_count,
        COUNT(DISTINCT transaction_type) as type_count
    FROM nyj_transaction_records tr
    JOIN projects p ON tr.project_id = p.id
    WHERE p.id IN %s
    AND tr.transaction_time >= %s AND tr.transaction_time <= %s
""", (tuple(project_ids), start_time, end_time))
result = cursor.fetchone()
print(f'记录数: {result[0]}, 总交易量: {result[1]}, 项目数: {result[4]}')
print(f'时间范围: {result[2]} - {result[3]}')
print(f'交易类型数: {result[5]}')

# 查看交易类型分布
print('\n交易类型分布:')
cursor.execute("""
    SELECT 
        transaction_type,
        COUNT(*) as record_count,
        SUM(CAST(transaction_num AS DECIMAL(20,2))) as total_volume
    FROM nyj_transaction_records tr
    JOIN projects p ON tr.project_id = p.id
    WHERE p.id IN %s
    AND tr.transaction_time >= %s AND tr.transaction_time <= %s
    GROUP BY transaction_type
    ORDER BY total_volume DESC
""", (tuple(project_ids), start_time, end_time))
results = cursor.fetchall()
for row in results:
    print(f'  {row[0]}: {row[1]}条记录, {row[2]}张')

# 3. 按月份对比分析
print('\n=== 3. 按月份对比分析 ===')
print('\n仪表盘数据（按月）:')

# 北京电力交易按月统计
cursor.execute("""
    SELECT 
        DATE_FORMAT(transaction_time, '%%Y-%%m') as month,
        SUM(CAST(transaction_quantity AS DECIMAL(20,2))) as volume
    FROM beijing_power_exchange_trades bt
    JOIN projects p ON bt.project_id = p.id
    WHERE p.id IN %s
    AND bt.transaction_time >= %s AND bt.transaction_time <= %s
    GROUP BY DATE_FORMAT(transaction_time, '%%Y-%%m')
    ORDER BY month
""", (tuple(project_ids), start_time, end_time))
beijing_monthly = cursor.fetchall()
print('北京电力交易中心（按月）:')
for row in beijing_monthly:
    print(f'  {row[0]}: {row[1]}')

# 广州电力交易按月统计
cursor.execute("""
    SELECT 
        DATE_FORMAT(deal_time, '%%Y-%%m') as month,
        SUM(CAST(gpc_certifi_num AS DECIMAL(20,2))) as volume
    FROM guangzhou_power_exchange_trades gt
    JOIN projects p ON gt.project_id = p.id
    WHERE p.id IN %s
    AND gt.deal_time >= %s AND gt.deal_time <= %s
    GROUP BY DATE_FORMAT(deal_time, '%%Y-%%m')
    ORDER BY month
""", (tuple(project_ids), start_time, end_time))
guangzhou_monthly = cursor.fetchall()
print('\n广州电力交易中心（按月）:')
for row in guangzhou_monthly:
    print(f'  {row[0]}: {row[1]}')

# 交易记录按月统计
print('\n交易记录数据（按月）:')
cursor.execute("""
    SELECT 
        DATE_FORMAT(STR_TO_DATE(transaction_time, '%%Y-%%m-%%d %%H:%%i:%%s'), '%%Y-%%m') as month,
        SUM(CAST(transaction_num AS DECIMAL(20,2))) as volume
    FROM nyj_transaction_records tr
    JOIN projects p ON tr.project_id = p.id
    WHERE p.id IN %s
    AND tr.transaction_time >= %s AND tr.transaction_time <= %s
    GROUP BY DATE_FORMAT(STR_TO_DATE(transaction_time, '%%Y-%%m-%%d %%H:%%i:%%s'), '%%Y-%%m')
    ORDER BY month
""", (tuple(project_ids), start_time, end_time))
transaction_monthly = cursor.fetchall()
for row in transaction_monthly:
    print(f'  {row[0]}: {row[1]}')

# 4. 寻找可能的重复或遗漏
print('\n=== 4. 数据匹配分析 ===')

# 检查是否有相同时间和数量的记录
print('\n检查北京电力交易与交易记录的匹配:')
cursor.execute("""
    SELECT 
        bt.project_id,
        bt.transaction_time,
        bt.transaction_quantity,
        tr.transaction_time as tr_time,
        tr.transaction_num as tr_num
    FROM beijing_power_exchange_trades bt
    JOIN projects p ON bt.project_id = p.id
    LEFT JOIN nyj_transaction_records tr ON (
        bt.project_id = tr.project_id 
        AND DATE(bt.transaction_time) = DATE(STR_TO_DATE(tr.transaction_time, '%%Y-%%m-%%d %%H:%%i:%%s'))
        AND bt.transaction_quantity = tr.transaction_num
    )
    WHERE p.id IN %s
    AND bt.transaction_time >= %s AND bt.transaction_time <= %s
    ORDER BY bt.transaction_time
    LIMIT 10
""", (tuple(project_ids), start_time, end_time))
matches = cursor.fetchall()
print('前10条匹配结果:')
for row in matches:
    match_status = '匹配' if row[4] is not None else '未匹配'
    print(f'  项目{row[0]}: {row[1]} {row[2]}张 -> {match_status}')

conn.close()

print('\n=== 分析总结 ===')
print('仪表盘总成交量: 631,195张')
print('聚合售出总量: 532,992张')
print('差异: 98,203张')
print('\n可能原因:')
print('1. 时间范围匹配问题')
print('2. 数据源不完全一致')
print('3. 交易记录可能不包含所有平台的数据')
print('4. 数据同步时间差异')