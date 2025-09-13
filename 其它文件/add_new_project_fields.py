#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移脚本：为projects表添加新字段
- is_green_cert_registered: 是否完成绿证交易平台注册
- has_beijing_transaction: 在北交做过交易
- has_guangzhou_transaction: 在广交做过交易
- has_green_cert_transaction: 在绿证交易平台做过交易

所有新字段默认值为1（True）
"""

from flask import Flask
from models import db
from sqlalchemy import text
import sys

def add_new_project_fields():
    """添加新的项目字段到数据库"""
    try:
        with db.engine.connect() as connection:
            # 检查字段是否已存在
            check_columns = connection.execute(text("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'projects' 
                AND COLUMN_NAME IN ('is_green_cert_registered', 'has_beijing_transaction', 'has_guangzhou_transaction', 'has_green_cert_transaction')
            """)).fetchall()
            
            existing_columns = [row[0] for row in check_columns]
            
            # 添加新字段
            fields_to_add = [
                ('is_green_cert_registered', '是否完成绿证交易平台注册'),
                ('has_beijing_transaction', '在北交做过交易'),
                ('has_guangzhou_transaction', '在广交做过交易'),
                ('has_green_cert_transaction', '在绿证交易平台做过交易')
            ]
            
            for field_name, description in fields_to_add:
                if field_name not in existing_columns:
                    print(f"添加字段: {field_name} ({description})")
                    connection.execute(text(f"""
                        ALTER TABLE projects 
                        ADD COLUMN {field_name} BOOLEAN DEFAULT TRUE
                    """))
                    connection.commit()
                    print(f"✓ 字段 {field_name} 添加成功")
                else:
                    print(f"字段 {field_name} 已存在，跳过")
            
            print("\n数据库迁移完成！")
            
    except Exception as e:
        print(f"迁移过程中发生错误: {e}")
        sys.exit(1)

if __name__ == '__main__':
    # 创建Flask应用上下文
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/green'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_recycle': 280,
        'pool_pre_ping': True
    }
    
    with app.app_context():
        db.init_app(app)
        add_new_project_fields()