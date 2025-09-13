# 更新表字段注释脚本
import pymysql
import sys

# 数据库连接信息
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'db': 'green',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_column_comments(table_name):
    """获取指定表的字段注释"""
    comments = {}
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = f"SHOW FULL COLUMNS FROM {table_name}"
            cursor.execute(sql)
            columns = cursor.fetchall()
            
            for column in columns:
                field_name = column['Field']
                comment = column['Comment']
                if comment:  # 只保存有注释的字段
                    comments[field_name] = comment
    except Exception as e:
        print(f"获取表 {table_name} 的注释时出错: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
    return comments

def update_column_comments(table_name, comments):
    """更新指定表的字段注释"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            for field_name, comment in comments.items():
                # 首先检查字段是否存在
                check_sql = f"SHOW COLUMNS FROM {table_name} LIKE '{field_name}'"
                cursor.execute(check_sql)
                if cursor.fetchone():  # 字段存在
                    # 获取字段类型
                    cursor.execute(f"SHOW FULL COLUMNS FROM {table_name} WHERE Field = '{field_name}'")
                    column_info = cursor.fetchone()
                    column_type = column_info['Type']
                    column_null = 'NULL' if column_info['Null'] == 'YES' else 'NOT NULL'
                    column_default = f"DEFAULT '{column_info['Default']}'" if column_info['Default'] is not None else ''
                    
                    # 更新字段注释
                    alter_sql = f"ALTER TABLE {table_name} MODIFY COLUMN `{field_name}` {column_type} {column_null} {column_default} COMMENT '{comment}'"
                    print(f"执行SQL: {alter_sql}")
                    cursor.execute(alter_sql)
        connection.commit()
        print(f"表 {table_name} 的注释更新成功")
    except Exception as e:
        print(f"更新表 {table_name} 的注释时出错: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()
    finally:
        if 'connection' in locals() and connection:
            connection.close()

def main():
    # 源表和目标表
    source_table = 'gzpt_bilateral_offline_trades'
    target_tables = ['gzpt_bilateral_online_trades', 'gzpt_unilateral_listings']
    
    # 获取源表的字段注释
    comments = get_column_comments(source_table)
    print(f"从表 {source_table} 获取到的注释:")
    for field, comment in comments.items():
        print(f"  {field}: {comment}")
    
    # 更新目标表的字段注释
    for target_table in target_tables:
        print(f"\n正在更新表 {target_table} 的注释...")
        update_column_comments(target_table, comments)

if __name__ == "__main__":
    main()