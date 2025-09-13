from app import app
from models import db, Customer
from sqlalchemy import text

def get_all_entity_names():
    """获取所有实体名称（与analyze_entity_names.py中的逻辑相同）"""
    
    def query_beijing_buyer_names(connection):
        """查询beijing_power_exchange_trades表的buyer_entity_name字段"""
        query = text("""
            SELECT DISTINCT buyer_entity_name 
            FROM beijing_power_exchange_trades 
            WHERE buyer_entity_name IS NOT NULL 
            AND buyer_entity_name != ''
        """)
        result = connection.execute(query)
        return [row[0] for row in result.fetchall()]
    
    def query_gzpt_member_names(connection):
        """查询gzpt_bilateral_offline_trades表的member_name字段"""
        query = text("""
            SELECT DISTINCT member_name 
            FROM gzpt_bilateral_offline_trades 
            WHERE member_name IS NOT NULL 
            AND member_name != ''
        """)
        result = connection.execute(query)
        return [row[0] for row in result.fetchall()]
    
    with db.engine.connect() as connection:
        # 1. 查询beijing_power_exchange_trades表的buyer_entity_name
        beijing_buyers = query_beijing_buyer_names(connection)
        print(f"从beijing_power_exchange_trades表获取到 {len(beijing_buyers)} 个buyer_entity_name")
        
        # 2. 查询gzpt_bilateral_offline_trades表的member_name
        gzpt_members = query_gzpt_member_names(connection)
        print(f"从gzpt_bilateral_offline_trades表获取到 {len(gzpt_members)} 个member_name")
        
        # 3. 合并两个列表并去重
        all_entities = list(set(beijing_buyers + gzpt_members))
        print(f"合并后共有 {len(all_entities)} 个不重复的实体名称")
        
        return sorted(all_entities)

def create_customer_table_and_insert_data():
    """创建客户表并插入数据"""
    print("开始创建客户表并插入数据...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 1. 创建表
            print("1. 创建customers表...")
            db.create_all()
            print("   表创建成功")
            
            # 2. 获取所有实体名称
            print("2. 获取所有实体名称...")
            entity_names = get_all_entity_names()
            
            # 3. 检查表中是否已有数据
            existing_count = Customer.query.count()
            print(f"3. 当前表中已有 {existing_count} 条记录")
            
            # 4. 插入数据
            print("4. 开始插入客户数据...")
            inserted_count = 0
            updated_count = 0
            
            for entity_name in entity_names:
                # 检查客户是否已存在
                existing_customer = Customer.query.filter_by(customer_name=entity_name).first()
                
                if existing_customer:
                    # 如果已存在，更新时间戳
                    existing_customer.updated_at = db.func.now()
                    updated_count += 1
                else:
                    # 如果不存在，创建新记录
                    new_customer = Customer(
                        customer_name=entity_name,
                        customer_type=None,  # 待后续填充
                        province=None        # 待后续填充
                    )
                    db.session.add(new_customer)
                    inserted_count += 1
            
            # 提交事务
            db.session.commit()
            
            print(f"   新插入 {inserted_count} 条记录")
            print(f"   更新 {updated_count} 条记录")
            print(f"   表中总计 {Customer.query.count()} 条记录")
            
            # 5. 显示部分插入的数据
            print("\n5. 显示前10条插入的客户数据:")
            print("-" * 40)
            customers = Customer.query.limit(10).all()
            for i, customer in enumerate(customers, 1):
                print(f"{i:2d}. {customer.customer_name}")
            
            if len(entity_names) > 10:
                print(f"    ... 还有 {len(entity_names) - 10} 条记录")
            
            print("\n" + "=" * 60)
            print("客户表创建和数据插入完成!")
            print("\n注意: 客户类型和所在省份字段目前为空，需要后续手动填充或通过其他方式补充。")
            
        except Exception as e:
            print(f"操作失败: {str(e)}")
            db.session.rollback()
            raise

if __name__ == '__main__':
    create_customer_table_and_insert_data()