# -*- coding: utf-8 -*-
"""
导出聚合分析所需明细：筛选 生产时间 为 2022 年（2022-01 ~ 2022-12），且 交易时间 为 2023 年 的记录，
分别从以下表导出到 Excel 多个工作表：
- 绿证交易平台：单边挂牌 gzpt_unilateral_listings（订单状态=1 成交）
- 绿证交易平台：双边线下 gzpt_bilateral_offline_trades（订单状态=3 成交）
- 绿证交易平台：双边线上 gzpt_bilateral_online_trades
- 北京电力交易中心：beijing_power_exchange_trades
- 广州电力交易中心：guangzhou_power_exchange_trades

说明：
- 生产期按 YYYY-MM 比较：
  * gzpt_* 表使用 generate_ym
  * 北京交易中心使用 production_year_month
  * 广州交易中心使用 product_date
- 交易时间按精确时间（>= 2023-01-01 且 < 2024-01-01）比较：
  * gzpt_* 表使用 order_time_str
  * 北京交易中心使用 transaction_time
  * 广州交易中心使用 deal_time

运行：python export_2022_2023_analysis.py
输出：在项目根目录下 exports/ 聚合分析_生产2022_交易2023.xlsx
"""
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import text

# 复用现有应用与数据库配置
from app import app, db

PROD_START = '2022-01'
PROD_END = '2022-12'
DEAL_START = '2023-01-01 00:00:00'
# 上开区间，便于包含全天
DEAL_END_EXCL = '2024-01-01 00:00:00'

EXPORT_DIR = os.path.join(os.path.dirname(__file__), 'exports')
EXPORT_PATH = os.path.join(EXPORT_DIR, '聚合分析_生产2022_交易2023.xlsx')


def fetch_project_info():
    """获取项目字典，补充导出字段。"""
    sql = text("""
        SELECT id, project_name, province, secondary_unit, power_type, project_nature, region
        FROM projects
    """)
    rows = db.session.execute(sql).fetchall()
    proj = {}
    for r in rows:
        # r.keys() 顺序与 SELECT 一致
        proj[r[0]] = {
            'project_name': r[1],
            'province': r[2],
            'secondary_unit': r[3],
            'power_type': r[4],
            'project_nature': r[5],
            'region': r[6],
        }
    return proj


def query_unilateral(params):
    sql = text("""
        SELECT 
            project_id,
            generate_ym AS production_ym,
            order_time_str AS transaction_time,
            total_quantity AS quantity,
            total_amount AS amount,
            order_status
        FROM gzpt_unilateral_listings
        WHERE generate_ym >= :prod_start AND generate_ym <= :prod_end
          AND order_time_str >= :deal_start AND order_time_str < :deal_end
          AND order_status = '1'
          AND total_quantity IS NOT NULL AND total_quantity != ''
          AND total_amount IS NOT NULL AND total_amount != ''
    """)
    return db.session.execute(sql, params).fetchall()


def query_offline(params):
    sql = text("""
        SELECT 
            project_id,
            generate_ym AS production_ym,
            order_time_str AS transaction_time,
            total_quantity AS quantity,
            total_amount AS amount,
            order_status
        FROM gzpt_bilateral_offline_trades
        WHERE generate_ym >= :prod_start AND generate_ym <= :prod_end
          AND order_time_str >= :deal_start AND order_time_str < :deal_end
          AND order_status = '3'
          AND total_quantity IS NOT NULL AND total_quantity != ''
          AND total_amount IS NOT NULL AND total_amount != ''
    """)
    return db.session.execute(sql, params).fetchall()


def query_online(params):
    sql = text("""
        SELECT 
            project_id,
            generate_ym AS production_ym,
            order_time_str AS transaction_time,
            total_quantity AS quantity,
            total_amount AS amount
        FROM gzpt_bilateral_online_trades
        WHERE generate_ym >= :prod_start AND generate_ym <= :prod_end
          AND order_time_str >= :deal_start AND order_time_str < :deal_end
          AND total_quantity IS NOT NULL AND total_quantity != ''
          AND total_amount IS NOT NULL AND total_amount != ''
    """)
    return db.session.execute(sql, params).fetchall()


def query_beijing(params):
    sql = text("""
        SELECT 
            project_id,
            production_year_month AS production_ym,
            transaction_time AS transaction_time,
            transaction_quantity AS quantity,
            transaction_price,
            (CAST(transaction_quantity AS DECIMAL(15,2)) * CAST(transaction_price AS DECIMAL(15,2))) AS amount
        FROM beijing_power_exchange_trades
        WHERE production_year_month >= :prod_start AND production_year_month <= :prod_end
          AND transaction_time >= :deal_start AND transaction_time < :deal_end
          AND transaction_quantity IS NOT NULL AND transaction_quantity != ''
          AND transaction_price IS NOT NULL AND transaction_price != ''
    """)
    return db.session.execute(sql, params).fetchall()


def query_guangzhou(params):
    sql = text("""
        SELECT 
            project_id,
            product_date AS production_ym,
            deal_time AS transaction_time,
            gpc_certifi_num AS quantity,
            total_cost AS amount
        FROM guangzhou_power_exchange_trades
        WHERE product_date >= :prod_start AND product_date <= :prod_end
          AND deal_time >= :deal_start AND deal_time < :deal_end
          AND gpc_certifi_num IS NOT NULL AND gpc_certifi_num != 0
          AND total_cost IS NOT NULL AND total_cost != 0
    """)
    return db.session.execute(sql, params).fetchall()


def rows_to_df(rows, project_map, extra_cols=None):
    extra_cols = extra_cols or []
    records = []
    for r in rows:
        # 通用前 5 列: project_id, production_ym, transaction_time, quantity, amount
        base = {
            'project_id': r[0],
            'production_ym': r[1],
            'transaction_time': r[2],
            'quantity': r[3],
            'amount': r[4] if len(r) > 4 else None,
        }
        # 附加列（如 order_status、transaction_price 等）
        for idx, col in enumerate(extra_cols, start=5):
            if idx < len(r):
                base[col] = r[idx]
        # 补充项目信息
        p = project_map.get(r[0], {})
        base.update({
            'project_name': p.get('project_name'),
            'province': p.get('province'),
            'secondary_unit': p.get('secondary_unit'),
            'power_type': p.get('power_type'),
            'project_nature': p.get('project_nature'),
            'region': p.get('region'),
        })
        records.append(base)
    df = pd.DataFrame.from_records(records)
    # 调整列顺序更友好
    preferred = ['project_id', 'project_name', 'province', 'secondary_unit', 'power_type', 'project_nature', 'region',
                 'production_ym', 'transaction_time', 'quantity', 'amount'] + extra_cols
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    if df.empty:
        # 返回包含列头的空表
        return pd.DataFrame(columns=cols)
    return df[cols]


def main():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    with app.app_context():
        project_map = fetch_project_info()
        params = {
            'prod_start': PROD_START,
            'prod_end': PROD_END,
            'deal_start': DEAL_START,
            'deal_end': DEAL_END_EXCL,
        }

        unilateral = query_unilateral(params)
        offline = query_offline(params)
        online = query_online(params)
        beijing = query_beijing(params)
        guangzhou = query_guangzhou(params)

        df_unilateral = rows_to_df(unilateral, project_map, extra_cols=['order_status'])
        df_offline = rows_to_df(offline, project_map, extra_cols=['order_status'])
        df_online = rows_to_df(online, project_map)
        df_beijing = rows_to_df(beijing, project_map, extra_cols=['transaction_price'])
        df_guangzhou = rows_to_df(guangzhou, project_map)

        # 写入 Excel
        try:
            with pd.ExcelWriter(EXPORT_PATH, engine='openpyxl') as writer:
                df_unilateral.to_excel(writer, index=False, sheet_name='绿证-单边挂牌')
                df_offline.to_excel(writer, index=False, sheet_name='绿证-双边线下')
                df_online.to_excel(writer, index=False, sheet_name='绿证-双边线上')
                df_beijing.to_excel(writer, index=False, sheet_name='北京交易中心')
                df_guangzhou.to_excel(writer, index=False, sheet_name='广州交易中心')
        except ImportError:
            # 作为兜底尝试 xlsxwriter
            with pd.ExcelWriter(EXPORT_PATH, engine='xlsxwriter') as writer:
                df_unilateral.to_excel(writer, index=False, sheet_name='绿证-单边挂牌')
                df_offline.to_excel(writer, index=False, sheet_name='绿证-双边线下')
                df_online.to_excel(writer, index=False, sheet_name='绿证-双边线上')
                df_beijing.to_excel(writer, index=False, sheet_name='北京交易中心')
                df_guangzhou.to_excel(writer, index=False, sheet_name='广州交易中心')

        print(f"导出完成: {EXPORT_PATH}")


if __name__ == '__main__':
    main()