from app import app
from models import db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        # 检查广交平台未匹配的卖方
        result = conn.execute(text("""
            SELECT DISTINCT gz.seller_entity_name 
            FROM guangzhou_power_exchange_trades gz 
            LEFT JOIN projects p ON gz.seller_entity_name = p.project_name 
            WHERE p.project_name IS NULL 
            LIMIT 10
        """)).fetchall()
        print('广交平台未匹配的卖方:', [r[0] for r in result])
        
        # 检查绿证平台未匹配的项目
        result2 = conn.execute(text("""
            SELECT DISTINCT ul.project_name 
            FROM gzpt_unilateral_listings ul 
            LEFT JOIN projects p ON ul.project_name = p.project_name 
            WHERE p.project_name IS NULL 
            LIMIT 10
        """)).fetchall()
        print('绿证平台未匹配的项目:', [r[0] for r in result2])
        
        # 检查北交平台未匹配的卖方
        result3 = conn.execute(text("""
            SELECT DISTINCT bj.seller_entity_name 
            FROM beijing_power_exchange_trades bj 
            LEFT JOIN projects p ON bj.seller_entity_name = p.project_name 
            WHERE p.project_name IS NULL 
            LIMIT 10
        """)).fetchall()
        print('北交平台未匹配的卖方:', [r[0] for r in result3])