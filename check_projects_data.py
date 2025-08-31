from app import app
from models import db
from sqlalchemy import text

with app.app_context():
    with db.engine.connect() as conn:
        # 检查projects表的数据结构
        result = conn.execute(text("""
            SELECT project_name, company_name 
            FROM projects 
            LIMIT 5
        """)).fetchall()
        print('项目表样例数据:')
        for r in result:
            print(f'项目名: {r[0]}, 公司名: {r[1]}')
        
        # 检查是否有匹配的公司名
        result2 = conn.execute(text("""
            SELECT DISTINCT gz.seller_entity_name 
            FROM guangzhou_power_exchange_trades gz 
            INNER JOIN projects p ON gz.seller_entity_name = p.company_name 
            LIMIT 5
        """)).fetchall()
        print('\n通过公司名匹配的广交平台卖方:')
        for r in result2:
            print(f'卖方: {r[0]}')