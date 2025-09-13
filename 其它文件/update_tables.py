from app import app, db
from sqlalchemy import text

def add_project_id_column(table_name):
    """为表添加project_id列（如果不存在）"""
    try:
        # 检查列是否存在
        result = db.session.execute(text(f"SHOW COLUMNS FROM {table_name} LIKE 'project_id'"))
        if not result.fetchone():
            # 添加列
            db.session.execute(text(f"ALTER TABLE {table_name} ADD COLUMN project_id INT NOT NULL"))
            print(f"已为 {table_name} 添加 project_id 列")
        else:
            print(f"{table_name} 已有 project_id 列")
    except Exception as e:
        print(f"处理 {table_name} 时出错: {e}")

with app.app_context():
    # 需要修改的表
    tables = ['gzpt_bilateral_offline_trades', 'gzpt_bilateral_online_trades', 'gzpt_unilateral_listings']
    
    for table in tables:
        add_project_id_column(table)
    
    # 提交更改
    db.session.commit()
    print("\n所有表更新完成!")