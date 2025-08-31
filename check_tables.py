#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3

def check_database_tables():
    """检查数据库中的表"""
    
    conn = sqlite3.connect('green_certificate.db')
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("数据库中的表:")
    for table in tables:
        print(f"  {table[0]}")
    
    conn.close()

if __name__ == '__main__':
    check_database_tables()