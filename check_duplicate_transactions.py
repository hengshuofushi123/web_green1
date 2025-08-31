from app import app
from models import db
from sqlalchemy import text

def check_duplicate_transactions():
    """检查是否存在同一绿证在多个平台的重复交易"""
    with app.app_context():
        with db.engine.connect() as connection:
            # 检查2024年生产、2025年3月交易的数据
            production_start = "2024-01"
            production_end = "2024-12"
            transaction_start = "2025-03-01"
            transaction_end = "2025-03-31"
            
            print("=== 检查重复交易情况 ===")
            print(f"生产时间: {production_start} - {production_end}")
            print(f"交易时间: {transaction_start} - {transaction_end}")
            
            # 1. 检查广州电力交易中心的数据
            gz_sql = text("""
                SELECT 
                    project_id,
                    product_date,
                    COUNT(*) as record_count,
                    SUM(CAST(gpc_certifi_num AS DECIMAL(15,2))) as total_quantity
                FROM guangzhou_power_exchange_trades
                WHERE deal_time BETWEEN :transaction_start AND :transaction_end
                AND product_date LIKE '____-__'
                AND gpc_certifi_num IS NOT NULL
                GROUP BY project_id, product_date
                HAVING COUNT(*) > 1
                ORDER BY record_count DESC
                LIMIT 10
            """)
            
            gz_result = connection.execute(gz_sql, {
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchall()
            
            print("\n1. 广州电力交易中心重复记录:")
            if gz_result:
                for row in gz_result:
                    print(f"  项目ID {row[0]}, 生产月份 {row[1]}: {row[2]} 条记录, 总量 {row[3]}")
            else:
                print("  无重复记录")
            
            # 2. 检查NYJ交易记录的数据
            nyj_sql = text("""
                SELECT 
                    project_id,
                    CONCAT(production_year, '-', LPAD(production_month, 2, '0')) as production_month,
                    COUNT(*) as record_count,
                    SUM(CAST(transaction_num AS DECIMAL(15,2))) as total_quantity
                FROM nyj_transaction_records
                WHERE transaction_time BETWEEN :transaction_start AND :transaction_end
                AND transaction_num IS NOT NULL
                AND transaction_num != ''
                GROUP BY project_id, production_year, production_month
                HAVING COUNT(*) > 1
                ORDER BY record_count DESC
                LIMIT 10
            """)
            
            nyj_result = connection.execute(nyj_sql, {
                'transaction_start': transaction_start,
                'transaction_end': transaction_end
            }).fetchall()
            
            print("\n2. NYJ交易记录重复记录:")
            if nyj_result:
                for row in nyj_result:
                    print(f"  项目ID {row[0]}, 生产月份 {row[1]}: {row[2]} 条记录, 总量 {row[3]}")
            else:
                print("  无重复记录")
            
            # 3. 检查跨平台重复情况
            cross_platform_sql = text("""
                SELECT 
                    n.project_id,
                    n.production_year_month,
                    CASE WHEN gz.id IS NOT NULL THEN 1 ELSE 0 END as has_gz,
                    CASE WHEN tr.id IS NOT NULL THEN 1 ELSE 0 END as has_nyj,
                    CASE WHEN bj.id IS NOT NULL THEN 1 ELSE 0 END as has_bj,
                    CASE WHEN ul.id IS NOT NULL THEN 1 ELSE 0 END as has_ul,
                    CASE WHEN ol.id IS NOT NULL THEN 1 ELSE 0 END as has_ol,
                    CASE WHEN off.id IS NOT NULL THEN 1 ELSE 0 END as has_off,
                    COALESCE(CAST(gz.gpc_certifi_num AS DECIMAL(15,2)), 0) as gz_qty,
                    COALESCE(CAST(tr.transaction_num AS DECIMAL(15,2)), 0) as nyj_qty
                FROM nyj_green_certificate_ledger n
                LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id 
                    AND CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month
                    AND gz.deal_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN nyj_transaction_records tr ON n.project_id = tr.project_id 
                    AND CONCAT(tr.production_year, '-', LPAD(tr.production_month, 2, '0')) = n.production_year_month
                    AND tr.transaction_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id 
                    AND n.production_year_month = bj.production_year_month
                    AND bj.transaction_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id 
                    AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
                    AND ul.order_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id 
                    AND n.production_year_month = ol.generate_ym AND ol.order_status = '2'
                    AND ol.order_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id 
                    AND n.production_year_month = off.generate_ym AND off.order_status = '3'
                    AND off.order_time BETWEEN :transaction_start AND :transaction_end
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND (gz.id IS NOT NULL OR tr.id IS NOT NULL OR bj.id IS NOT NULL 
                     OR ul.id IS NOT NULL OR ol.id IS NOT NULL OR off.id IS NOT NULL)
                ORDER BY n.project_id, n.production_year_month
                LIMIT 20
            """)
            
            cross_result = connection.execute(cross_platform_sql, {
                'transaction_start': transaction_start,
                'transaction_end': transaction_end,
                'production_start': production_start,
                'production_end': production_end
            }).fetchall()
            
            print("\n3. 跨平台交易情况（前20条）:")
            print("项目ID\t生产月份\t\t广州\tNYJ\t北京\t单向\t双边线上\t双边线下\t广州数量\tNYJ数量")
            
            multiple_platform_count = 0
            for row in cross_result:
                platform_count = row[2] + row[3] + row[4] + row[5] + row[6] + row[7]
                if platform_count > 1:
                    multiple_platform_count += 1
                    
                print(f"{row[0]}\t{row[1]}\t\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}\t{row[6]}\t\t{row[7]}\t\t{row[8]}\t{row[9]}")
            
            print(f"\n发现 {multiple_platform_count} 条记录在多个平台有交易")
            
            # 4. 计算实际的不重复交易量
            unique_sql = text("""
                SELECT 
                    COUNT(DISTINCT CONCAT(n.project_id, '-', n.production_year_month)) as unique_certificates,
                    SUM(CASE 
                        WHEN gz.gpc_certifi_num IS NOT NULL THEN CAST(gz.gpc_certifi_num AS DECIMAL(15,2))
                        WHEN tr.transaction_num IS NOT NULL THEN CAST(tr.transaction_num AS DECIMAL(15,2))
                        WHEN bj.transaction_quantity IS NOT NULL THEN CAST(bj.transaction_quantity AS DECIMAL(15,2))
                        WHEN ul.total_quantity IS NOT NULL THEN CAST(ul.total_quantity AS DECIMAL(15,2))
                        WHEN ol.total_quantity IS NOT NULL THEN CAST(ol.total_quantity AS DECIMAL(15,2))
                        WHEN off.total_quantity IS NOT NULL THEN CAST(off.total_quantity AS DECIMAL(15,2))
                        ELSE 0
                    END) as unique_total_quantity
                FROM nyj_green_certificate_ledger n
                LEFT JOIN guangzhou_power_exchange_trades gz ON n.project_id = gz.project_id 
                    AND CONCAT(SUBSTRING(gz.product_date, 1, 4), '-', LPAD(SUBSTRING(gz.product_date, 6), 2, '0')) = n.production_year_month
                    AND gz.deal_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN nyj_transaction_records tr ON n.project_id = tr.project_id 
                    AND CONCAT(tr.production_year, '-', LPAD(tr.production_month, 2, '0')) = n.production_year_month
                    AND tr.transaction_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN beijing_power_exchange_trades bj ON n.project_id = bj.project_id 
                    AND n.production_year_month = bj.production_year_month
                    AND bj.transaction_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN gzpt_unilateral_listings ul ON n.project_id = ul.project_id 
                    AND n.production_year_month = ul.generate_ym AND ul.order_status = '1'
                    AND ul.order_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN gzpt_bilateral_online_trades ol ON n.project_id = ol.project_id 
                    AND n.production_year_month = ol.generate_ym AND ol.order_status = '2'
                    AND ol.order_time BETWEEN :transaction_start AND :transaction_end
                LEFT JOIN gzpt_bilateral_offline_trades off ON n.project_id = off.project_id 
                    AND n.production_year_month = off.generate_ym AND off.order_status = '3'
                    AND off.order_time BETWEEN :transaction_start AND :transaction_end
                WHERE n.production_year_month >= :production_start
                AND n.production_year_month <= :production_end
                AND (gz.id IS NOT NULL OR tr.id IS NOT NULL OR bj.id IS NOT NULL 
                     OR ul.id IS NOT NULL OR ol.id IS NOT NULL OR off.id IS NOT NULL)
            """)
            
            unique_result = connection.execute(unique_sql, {
                'transaction_start': transaction_start,
                'transaction_end': transaction_end,
                'production_start': production_start,
                'production_end': production_end
            }).fetchone()
            
            print(f"\n4. 去重后的交易统计:")
            print(f"  唯一绿证数量: {unique_result[0]}")
            print(f"  去重后总交易量: {unique_result[1]}")

if __name__ == '__main__':
    check_duplicate_transactions()