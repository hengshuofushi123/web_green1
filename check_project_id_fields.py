from app import app
from models import db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        # 检查各个交易表是否有project_id字段
        tables = [
            'guangzhou_power_exchange_trades',
            'gzpt_unilateral_listings', 
            'gzpt_bilateral_offline_trades',
            'beijing_power_exchange_trades'
        ]
        
        for table in tables:
            try:
                result = conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '%project%'")).fetchall()
                print(f'{table}表中包含project的字段:')
                for r in result:
                    print(f'  - {r[0]} ({r[1]})')
                print()
            except Exception as e:
                print(f'检查{table}表时出错: {e}\n')