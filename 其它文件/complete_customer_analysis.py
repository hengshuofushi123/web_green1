from app import app
from models import db, Customer
from sqlalchemy import text

def get_complete_entity_names():
    """
    从所有相关表中获取完整的实体名称列表
    包括：
    - beijing_power_exchange_trades.buyer_entity_name
    - gzpt_bilateral_offline_trades.member_name
    - gzpt_bilateral_online_trades.member_name
    - gzpt_unilateral_listings.member_name
    - guangzhou_power_exchange_trades.buyer_entity_name
    """
    print("从所有相关表中获取完整的实体名称...")
    print("=" * 60)
    
    with app.app_context():
        try:
            with db.engine.connect() as connection:
                all_entities = set()
                
                # 1. beijing_power_exchange_trades 表的 buyer_entity_name
                print("1. 查询 beijing_power_exchange_trades 表...")
                query1 = text("""
                    SELECT DISTINCT buyer_entity_name 
                    FROM beijing_power_exchange_trades 
                    WHERE buyer_entity_name IS NOT NULL 
                    AND buyer_entity_name != ''
                """)
                result1 = connection.execute(query1)
                entities1 = {row[0] for row in result1}
                print(f"   找到 {len(entities1)} 个不重复的 buyer_entity_name")
                all_entities.update(entities1)
                
                # 2. gzpt_bilateral_offline_trades 表的 member_name
                print("2. 查询 gzpt_bilateral_offline_trades 表...")
                query2 = text("""
                    SELECT DISTINCT member_name 
                    FROM gzpt_bilateral_offline_trades 
                    WHERE member_name IS NOT NULL 
                    AND member_name != ''
                """)
                result2 = connection.execute(query2)
                entities2 = {row[0] for row in result2}
                print(f"   找到 {len(entities2)} 个不重复的 member_name")
                all_entities.update(entities2)
                
                # 3. gzpt_bilateral_online_trades 表的 member_name
                print("3. 查询 gzpt_bilateral_online_trades 表...")
                try:
                    query3 = text("""
                        SELECT DISTINCT member_name 
                        FROM gzpt_bilateral_online_trades 
                        WHERE member_name IS NOT NULL 
                        AND member_name != ''
                    """)
                    result3 = connection.execute(query3)
                    entities3 = {row[0] for row in result3}
                    print(f"   找到 {len(entities3)} 个不重复的 member_name")
                    all_entities.update(entities3)
                except Exception as e:
                    print(f"   表不存在或查询失败: {str(e)}")
                    entities3 = set()
                
                # 4. gzpt_unilateral_listings 表的 member_name
                print("4. 查询 gzpt_unilateral_listings 表...")
                try:
                    query4 = text("""
                        SELECT DISTINCT member_name 
                        FROM gzpt_unilateral_listings 
                        WHERE member_name IS NOT NULL 
                        AND member_name != ''
                    """)
                    result4 = connection.execute(query4)
                    entities4 = {row[0] for row in result4}
                    print(f"   找到 {len(entities4)} 个不重复的 member_name")
                    all_entities.update(entities4)
                except Exception as e:
                    print(f"   表不存在或查询失败: {str(e)}")
                    entities4 = set()
                
                # 5. guangzhou_power_exchange_trades 表的 buyer_entity_name
                print("5. 查询 guangzhou_power_exchange_trades 表...")
                query5 = text("""
                    SELECT DISTINCT buyer_entity_name 
                    FROM guangzhou_power_exchange_trades 
                    WHERE buyer_entity_name IS NOT NULL 
                    AND buyer_entity_name != ''
                """)
                result5 = connection.execute(query5)
                entities5 = {row[0] for row in result5}
                print(f"   找到 {len(entities5)} 个不重复的 buyer_entity_name")
                all_entities.update(entities5)
                
                print("\n" + "=" * 60)
                print("汇总统计:")
                print(f"beijing_power_exchange_trades: {len(entities1)} 个实体")
                print(f"gzpt_bilateral_offline_trades: {len(entities2)} 个实体")
                print(f"gzpt_bilateral_online_trades: {len(entities3)} 个实体")
                print(f"gzpt_unilateral_listings: {len(entities4)} 个实体")
                print(f"guangzhou_power_exchange_trades: {len(entities5)} 个实体")
                print(f"\n合并后总计: {len(all_entities)} 个不重复实体")
                
                return sorted(list(all_entities))
                
        except Exception as e:
            print(f"查询失败: {str(e)}")
            raise

def compare_with_current_customers(complete_entities):
    """
    比较完整实体列表与当前客户表
    """
    print("\n" + "=" * 60)
    print("与当前客户表比较:")
    
    with app.app_context():
        try:
            # 获取当前客户表中的所有客户名称
            current_customers = {customer.customer_name for customer in Customer.query.all()}
            
            print(f"当前客户表记录数: {len(current_customers)}")
            print(f"完整实体列表数量: {len(complete_entities)}")
            
            # 找出缺失的实体
            missing_entities = set(complete_entities) - current_customers
            print(f"缺失的实体数量: {len(missing_entities)}")
            
            # 找出多余的实体（客户表中有但完整列表中没有的）
            extra_entities = current_customers - set(complete_entities)
            print(f"多余的实体数量: {len(extra_entities)}")
            
            if missing_entities:
                print("\n缺失的实体 (前20个):")
                print("-" * 40)
                for i, entity in enumerate(sorted(list(missing_entities))[:20], 1):
                    print(f"{i:2d}. {entity}")
                if len(missing_entities) > 20:
                    print(f"    ... 还有 {len(missing_entities) - 20} 个")
            
            if extra_entities:
                print("\n多余的实体:")
                print("-" * 40)
                for i, entity in enumerate(sorted(list(extra_entities)), 1):
                    print(f"{i:2d}. {entity}")
            
            return missing_entities, extra_entities
            
        except Exception as e:
            print(f"比较失败: {str(e)}")
            raise

def update_customer_table(complete_entities):
    """
    更新客户表，添加缺失的实体
    """
    print("\n" + "=" * 60)
    print("更新客户表...")
    
    with app.app_context():
        try:
            # 获取当前客户表中的所有客户名称
            current_customers = {customer.customer_name for customer in Customer.query.all()}
            
            # 找出需要添加的实体
            missing_entities = set(complete_entities) - current_customers
            
            if not missing_entities:
                print("客户表已包含所有实体，无需更新")
                return
            
            print(f"准备添加 {len(missing_entities)} 个新实体...")
            
            # 分析客户类型的函数
            def analyze_customer_type(name):
                # 公司关键词
                company_keywords = [
                    '有限公司', '股份有限公司', '集团', '企业', '公司', '工厂', '厂',
                    '研究院', '院', '银行', '保险', '证券', '基金', '信托',
                    '合作社', '联社', '协会', '学会', '中心', '站', '所',
                    '局', '委', '部', '署', '办', '处'
                ]
                
                # 个人关键词（通常是姓名格式）
                individual_keywords = ['先生', '女士', '小姐']
                
                # 检查公司关键词
                for keyword in company_keywords:
                    if keyword in name:
                        return '公司'
                
                # 检查个人关键词
                for keyword in individual_keywords:
                    if keyword in name:
                        return '个人'
                
                # 检查脱敏公司名称
                if name.endswith(('***司', '***厂', '***院', '***行', '***社', '***店', '***部')):
                    return '公司'
                
                # 根据长度判断（中文姓名通常2-4个字符）
                if len(name) <= 4 and not any(char in name for char in ['公司', '企业', '集团', '工厂']):
                    # 检查是否为中文姓名格式
                    if all('\u4e00' <= char <= '\u9fff' for char in name):
                        return '个人'
                
                # 默认为公司
                return '公司'
            
            # 批量添加新实体
            added_count = 0
            for entity_name in missing_entities:
                customer_type = analyze_customer_type(entity_name)
                
                new_customer = Customer(
                    customer_name=entity_name,
                    customer_type=customer_type,
                    province='未设置'
                )
                
                db.session.add(new_customer)
                added_count += 1
                
                if added_count <= 10:  # 显示前10个添加的实体
                    print(f"{added_count:3d}. {entity_name} -> {customer_type}")
                elif added_count == 11:
                    print("    ... 继续添加中 ...")
            
            # 提交更改
            db.session.commit()
            
            print(f"\n成功添加 {added_count} 个新客户")
            
            # 显示最终统计
            total_customers = Customer.query.count()
            company_customers = Customer.query.filter_by(customer_type='公司').count()
            individual_customers = Customer.query.filter_by(customer_type='个人').count()
            
            print(f"\n更新后统计:")
            print(f"总客户数: {total_customers}")
            print(f"公司客户: {company_customers} ({company_customers/total_customers*100:.1f}%)")
            print(f"个人客户: {individual_customers} ({individual_customers/total_customers*100:.1f}%)")
            
        except Exception as e:
            print(f"更新失败: {str(e)}")
            db.session.rollback()
            raise

if __name__ == '__main__':
    # 1. 获取完整的实体名称列表
    complete_entities = get_complete_entity_names()
    
    # 2. 与当前客户表比较
    missing_entities, extra_entities = compare_with_current_customers(complete_entities)
    
    # 3. 更新客户表
    if missing_entities:
        update_customer_table(complete_entities)
    
    print("\n" + "=" * 60)
    print("完整客户分析完成!")