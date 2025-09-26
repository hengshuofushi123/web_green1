from app import create_app
from app.models import db
from sqlalchemy import text

app = create_app()
app.app_context().push()

with db.engine.connect() as conn:
    # 模拟实际的API调用参数
    customer_name = '广西环保产业投资集团有限公司'
    start_month = '2024-01'  # 修改为实际数据的年份
    end_month = '2024-12'
    transaction_start_date = '2025-01-01'
    transaction_end_date = '2025-09-14'
    
    # 构建交易时间筛选条件
    end_date_for_query = f"{transaction_end_date} 23:59:59"
    transaction_filter_gz = f"AND gz.deal_time BETWEEN '{transaction_start_date}' AND '{end_date_for_query}'"
    
    print("=== 修复后的交易明细查询测试 ===")
    
    # 使用修复后的查询逻辑
    details_sql = f"""
        SELECT
            p.secondary_unit,
            p.project_name,
            gz.deal_time AS transaction_time,
            gz.product_date AS production_time,
            CAST(gz.gpc_certifi_num AS DECIMAL(15,2)) AS quantity,
            CASE WHEN gz.gpc_certifi_num > 0 THEN CAST(gz.total_cost AS DECIMAL(15,2)) / CAST(gz.gpc_certifi_num AS DECIMAL(15,2)) ELSE 0 END AS price
        FROM projects p JOIN guangzhou_power_exchange_trades gz ON p.id = gz.project_id
        WHERE gz.buyer_entity_name = :customer_name
        AND gz.product_date BETWEEN :start_month AND :end_month
        {transaction_filter_gz}
        ORDER BY gz.deal_time DESC
        LIMIT 10
    """
    
    result = conn.execute(
        text(details_sql),
        {
            "customer_name": customer_name,
            "start_month": start_month,
            "end_month": end_month
        }
    )
    
    details = []
    for row in result:
        details.append({
            'secondary_unit': row.secondary_unit,
            'transaction_time': str(row.transaction_time),
            'production_time': row.production_time,
            'quantity': f"{float(row.quantity):.2f}",
            'price': f"{float(row.price):.2f}",
            'project_name': row.project_name
        })
    
    print(f"找到 {len(details)} 条交易明细记录:")
    for i, detail in enumerate(details, 1):
        print(f"{i}. 项目: {detail['project_name']}")
        print(f"   生产时间: {detail['production_time']}")
        print(f"   交易时间: {detail['transaction_time']}")
        print(f"   交易量: {detail['quantity']}")
        print(f"   单价: {detail['price']}")
        print("-" * 50)
    
    if len(details) == 0:
        print("❌ 仍然没有找到交易明细！")
    else:
        print(f"✅ 成功找到 {len(details)} 条交易明细记录！")