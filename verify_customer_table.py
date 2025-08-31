from app import app
from models import db, Customer
from sqlalchemy import text

def verify_customer_table():
    """验证客户表的创建和数据"""
    print("验证客户表数据...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 1. 检查表是否存在
            with db.engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES LIKE 'customers'"))
                table_exists = result.fetchone() is not None
                print(f"1. 客户表是否存在: {'是' if table_exists else '否'}")
                
                if not table_exists:
                    print("   客户表不存在，请先运行 create_customer_table.py")
                    return
            
            # 2. 统计总记录数
            total_count = Customer.query.count()
            print(f"2. 客户表总记录数: {total_count}")
            
            # 3. 检查字段结构
            with db.engine.connect() as connection:
                result = connection.execute(text("DESCRIBE customers"))
                columns = result.fetchall()
                print("3. 表结构:")
                for col in columns:
                    print(f"   {col[0]} - {col[1]} - {col[2]} - {col[3]}")
            
            # 4. 显示前20条记录
            print("\n4. 前20条客户记录:")
            print("-" * 60)
            customers = Customer.query.limit(20).all()
            for i, customer in enumerate(customers, 1):
                print(f"{i:2d}. {customer.customer_name}")
                print(f"    客户类型: {customer.customer_type or '未设置'}")
                print(f"    所在省份: {customer.province or '未设置'}")
                print(f"    创建时间: {customer.created_at}")
                print()
            
            # 5. 按客户名称长度统计
            print("5. 客户名称长度分析:")
            print("-" * 40)
            all_customers = Customer.query.all()
            name_lengths = [len(c.customer_name) for c in all_customers]
            print(f"   最短名称长度: {min(name_lengths)} 字符")
            print(f"   最长名称长度: {max(name_lengths)} 字符")
            print(f"   平均名称长度: {sum(name_lengths)/len(name_lengths):.1f} 字符")
            
            # 找出最长的几个名称
            longest_names = sorted(all_customers, key=lambda x: len(x.customer_name), reverse=True)[:5]
            print("\n   最长的5个客户名称:")
            for i, customer in enumerate(longest_names, 1):
                print(f"   {i}. {customer.customer_name} ({len(customer.customer_name)}字符)")
            
            # 6. 检查是否有重复记录
            print("\n6. 数据完整性检查:")
            print("-" * 40)
            
            # 检查空值
            empty_names = Customer.query.filter(Customer.customer_name == '').count()
            null_names = Customer.query.filter(Customer.customer_name.is_(None)).count()
            print(f"   空客户名称: {empty_names} 条")
            print(f"   NULL客户名称: {null_names} 条")
            
            # 检查客户类型和省份的填充情况
            filled_type = Customer.query.filter(Customer.customer_type.isnot(None)).count()
            filled_province = Customer.query.filter(Customer.province.isnot(None)).count()
            print(f"   已填充客户类型: {filled_type} 条 ({filled_type/total_count*100:.1f}%)")
            print(f"   已填充省份信息: {filled_province} 条 ({filled_province/total_count*100:.1f}%)")
            
            print("\n" + "=" * 60)
            print("客户表验证完成!")
            
        except Exception as e:
            print(f"验证失败: {str(e)}")
            raise

if __name__ == '__main__':
    verify_customer_table()