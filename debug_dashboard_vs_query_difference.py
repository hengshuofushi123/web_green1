from app import app
from models import db
from sqlalchemy import text
import subprocess
import sys
import os

def debug_dashboard_vs_query_difference():
    """调试dashboard显示的177,870与query_platform_volumes_and_prices.py计算出的159,306之间的差异"""
    with app.app_context():
        with db.engine.connect() as connection:
            # 使用与dashboard相同的参数
            production_start = "2024-01"
            production_end = "2024-12"
            transaction_start = "2025-03-01"
            transaction_end = "2025-03-31"
            
            print("=== 调试Dashboard vs Query差异 ===")
            print(f"生产时间: {production_start} - {production_end}")
            print(f"交易时间: {transaction_start} - {transaction_end}")
            
            # 1. 运行query_platform_volumes_and_prices.py获取结果
            print("\n1. 运行query_platform_volumes_and_prices.py")
            result = subprocess.run([sys.executable, 'query_platform_volumes_and_prices.py'], 
                                  capture_output=True, text=True, cwd=os.getcwd())
            
            query_guangzhou_volume = None
            if result.returncode == 0:
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines:
                    if '广州电力交易中心' in line and 'total_quantity' in line:
                        # 解析广州电力交易中心的数量
                        parts = line.split(',')
                        for part in parts:
                            if 'total_quantity' in part:
                                query_guangzhou_volume = float(part.split(':')[1].strip())
                                break
                        break
                print(f"query_platform_volumes_and_prices.py - 广州电力交易中心: {query_guangzhou_volume}")
            else:
                print(f"运行失败: {result.stderr}")
                return
            
            # 2. 获取所有项目ID（模拟管理员权限）
            print("\n2. 获取项目ID列表")
            from models import Project
            projects = Project.query.all()
            project_ids_list = [p.id for p in projects]
            print(f"找到 {len(project_ids_list)} 个项目")
            
            # 3. 模拟Dashboard的SQL查询逻辑（包含项目过滤和交易时间过滤）
            print("\n3. 模拟Dashboard的SQL查询逻辑")
            
            # 构建交易时间过滤条件
            transaction_filter_clauses = {
                'tr': '', 'ul': '', 'ol': '', 'off': '', 'bj': '', 'gz': ''
            }
            end_date_for_query = f"{transaction_end} 23:59:59"
            transaction_filter_clauses['ul'] = f"AND ul.order_time_str BETWEEN '{transaction_start}' AND '{end_date_for_query}'"
            transaction_filter_clauses['ol'] = f"AND ol.order_time_str BETWEEN '{transaction_start}' AND '{end_date_for_query}'"
            transaction_filter_clauses['off'] = f"AND off.order_time_str BETWEEN '{transaction_start}' AND '{end_date_for_query}'"
            transaction_filter_clauses['bj'] = f"AND bj.transaction_time BETWEEN '{transaction_start}' AND '{end_date_for_query}'"
            transaction_filter_clauses['gz'] = f"AND gz.deal_time BETWEEN '{transaction_start}' AND '{end_date_for_query}'"
            
            # 模拟dashboard中trading_platform_sold的计算
            dashboard_sql = text(f"""
                SELECT 
                    -- 交易平台售出量汇总
                    COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) + 
                    COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as trading_platform_sold
                FROM projects p 
                JOIN nyj_green_certificate_ledger n ON p.id = n.project_id
                LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id AND n.production_year_month = ul.generate_ym AND ul.order_status = '1' AND ul.total_quantity IS NOT NULL AND ul.total_quantity > 0 AND ul.total_amount IS NOT NULL AND ul.total_amount > 0 {transaction_filter_clauses['ul']}
                LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id AND n.production_year_month = ol.generate_ym AND ol.order_status = '2' AND ol.total_quantity IS NOT NULL AND ol.total_quantity > 0 AND ol.total_amount IS NOT NULL AND ol.total_amount > 0 {transaction_filter_clauses['ol']}
                LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id AND n.production_year_month = off.generate_ym AND off.order_status = '3' AND off.total_quantity IS NOT NULL AND off.total_quantity > 0 AND off.total_amount IS NOT NULL AND off.total_amount > 0 {transaction_filter_clauses['off']}
                LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id AND n.production_year_month = bj.production_year_month {transaction_filter_clauses['bj']}
                LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id AND CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month {transaction_filter_clauses['gz']}
                WHERE p.id IN :project_ids
                AND n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
            """)
            
            dashboard_result = connection.execute(dashboard_sql, {
                'project_ids': tuple(project_ids_list),
                'production_start': production_start,
                'production_end': production_end
            }).fetchone()
            
            dashboard_total = float(dashboard_result[0]) if dashboard_result[0] else 0
            print(f"Dashboard模拟查询 - 交易平台售出量总计: {dashboard_total}")
            
            # 3. 分别查询各个平台的数据
            print("\n3. 分别查询各个平台的数据")
            
            # 广州电力交易中心
            gz_sql = text("""
                SELECT 
                    COUNT(*) as record_count,
                    COALESCE(SUM(CAST(gz.gpc_certifi_num AS DECIMAL(15,2))), 0) as total_quantity
                FROM nyj_green_certificate_ledger n
                LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id 
                    AND CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month
                    AND (gz.deal_time IS NULL OR (gz.deal_time >= :transaction_start AND gz.deal_time <= :transaction_end))
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND gz.id IS NOT NULL
            """)
            
            gz_result = connection.execute(gz_sql, {
                'production_start': production_start,
                'production_end': production_end,
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchone()
            
            dashboard_gz_volume = float(gz_result[1]) if gz_result[1] else 0
            print(f"Dashboard - 广州电力交易中心: {dashboard_gz_volume} (记录数: {gz_result[0]})")
            
            # 北京电力交易中心
            bj_sql = text("""
                SELECT 
                    COUNT(*) as record_count,
                    COALESCE(SUM(CAST(bj.transaction_quantity AS DECIMAL(15,2))), 0) as total_quantity
                FROM nyj_green_certificate_ledger n
                LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id 
                    AND n.production_year_month = bj.production_year_month
                    AND (bj.transaction_time IS NULL OR (bj.transaction_time >= :transaction_start AND bj.transaction_time <= :transaction_end))
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND bj.id IS NOT NULL
            """)
            
            bj_result = connection.execute(bj_sql, {
                'production_start': production_start,
                'production_end': production_end,
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchone()
            
            dashboard_bj_volume = float(bj_result[1]) if bj_result[1] else 0
            print(f"Dashboard - 北京电力交易中心: {dashboard_bj_volume} (记录数: {bj_result[0]})")
            
            # 绿证交易平台 - 单边挂牌
            ul_sql = text("""
                SELECT 
                    COUNT(*) as record_count,
                    COALESCE(SUM(CAST(ul.total_quantity AS DECIMAL(15,2))), 0) as total_quantity
                FROM nyj_green_certificate_ledger n
                LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id 
                    AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
                    AND (ul.order_time_str IS NULL OR (ul.order_time_str >= :transaction_start AND ul.order_time_str <= :transaction_end))
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND ul.id IS NOT NULL
            """)
            
            ul_result = connection.execute(ul_sql, {
                'production_start': production_start,
                'production_end': production_end,
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchone()
            
            dashboard_ul_volume = float(ul_result[1]) if ul_result[1] else 0
            print(f"Dashboard - 绿证平台单边挂牌: {dashboard_ul_volume} (记录数: {ul_result[0]})")
            
            # 绿证交易平台 - 双边线上
            ol_sql = text("""
                SELECT 
                    COUNT(*) as record_count,
                    COALESCE(SUM(CAST(ol.total_quantity AS DECIMAL(15,2))), 0) as total_quantity
                FROM nyj_green_certificate_ledger n
                LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id 
                    AND n.production_year_month = ol.generate_ym AND ol.order_status = '2'
                    AND (ol.order_time_str IS NULL OR (ol.order_time_str >= :transaction_start AND ol.order_time_str <= :transaction_end))
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND ol.id IS NOT NULL
            """)
            
            ol_result = connection.execute(ol_sql, {
                'production_start': production_start,
                'production_end': production_end,
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchone()
            
            dashboard_ol_volume = float(ol_result[1]) if ol_result[1] else 0
            print(f"Dashboard - 绿证平台双边线上: {dashboard_ol_volume} (记录数: {ol_result[0]})")
            
            # 绿证交易平台 - 双边线下
            off_sql = text("""
                SELECT 
                    COUNT(*) as record_count,
                    COALESCE(SUM(CAST(off.total_quantity AS DECIMAL(15,2))), 0) as total_quantity
                FROM nyj_green_certificate_ledger n
                LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id 
                    AND n.production_year_month = off.generate_ym AND off.order_status = '3'
                    AND (off.order_time_str IS NULL OR (off.order_time_str >= :transaction_start AND off.order_time_str <= :transaction_end))
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND off.id IS NOT NULL
            """)
            
            off_result = connection.execute(off_sql, {
                'production_start': production_start,
                'production_end': production_end,
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchone()
            
            dashboard_off_volume = float(off_result[1]) if off_result[1] else 0
            print(f"Dashboard - 绿证平台双边线下: {dashboard_off_volume} (记录数: {off_result[0]})")
            
            # 4. 对比分析
            print("\n=== 对比分析 ===")
            print(f"query_platform_volumes_and_prices.py - 广州电力交易中心: {query_guangzhou_volume}")
            print(f"Dashboard - 广州电力交易中心: {dashboard_gz_volume}")
            print(f"广州电力交易中心差异: {dashboard_gz_volume - query_guangzhou_volume if query_guangzhou_volume else 'N/A'}")
            
            dashboard_platforms_total = dashboard_gz_volume + dashboard_bj_volume + dashboard_ul_volume + dashboard_ol_volume + dashboard_off_volume
            print(f"\nDashboard各平台分别计算总和: {dashboard_platforms_total}")
            print(f"Dashboard SQL直接查询总计: {dashboard_total}")
            print(f"Dashboard内部一致性检查: {'✓' if abs(dashboard_platforms_total - dashboard_total) < 0.01 else '✗'}")
            
            if query_guangzhou_volume:
                total_difference = dashboard_total - query_guangzhou_volume
                print(f"\n总差异分析:")
                print(f"Dashboard总计: {dashboard_total}")
                print(f"Query脚本(仅广州): {query_guangzhou_volume}")
                print(f"总差异: {total_difference}")
                print(f"其他平台贡献: {dashboard_bj_volume + dashboard_ul_volume + dashboard_ol_volume + dashboard_off_volume}")
                
                if abs(total_difference - (dashboard_bj_volume + dashboard_ul_volume + dashboard_ol_volume + dashboard_off_volume)) < 0.01:
                    print("\n✓ 差异主要来源于其他平台的交易数据")
                else:
                    print("\n✗ 存在其他未知差异来源")

if __name__ == '__main__':
    debug_dashboard_vs_query_difference()