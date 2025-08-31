from app import app
from models import db, Customer
from sqlalchemy import text

def fix_masked_individual_names():
    """
    修正包含'*'且最后一个字不是'司'的客户名称，将其归类为个人
    """
    print("修正脱敏个人客户名称分类...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 查找所有包含'*'的客户
            customers_with_asterisk = Customer.query.filter(
                Customer.customer_name.contains('*')
            ).all()
            
            print(f"找到 {len(customers_with_asterisk)} 个包含'*'的客户名称")
            
            if not customers_with_asterisk:
                print("没有找到包含'*'的客户名称")
                return
            
            print("\n分析每个包含'*'的客户名称:")
            print("-" * 40)
            
            corrections_made = 0
            individual_corrections = []
            company_kept = []
            
            for customer in customers_with_asterisk:
                customer_name = customer.customer_name
                current_type = customer.customer_type
                
                # 检查最后一个字是否为'司'
                if customer_name.endswith('司'):
                    # 最后一个字是'司'，保持为公司
                    company_kept.append((customer_name, current_type))
                    print(f"保持公司: {customer_name} ({current_type})")
                else:
                    # 最后一个字不是'司'，应归类为个人
                    if current_type != '个人':
                        customer.customer_type = '个人'
                        corrections_made += 1
                        individual_corrections.append((customer_name, current_type, '个人'))
                        print(f"修正为个人: {customer_name} ({current_type} -> 个人)")
                    else:
                        individual_corrections.append((customer_name, current_type, '个人'))
                        print(f"已是个人: {customer_name} ({current_type})")
            
            # 提交更改
            if corrections_made > 0:
                db.session.commit()
                print(f"\n成功修正 {corrections_made} 个客户的分类")
            else:
                print("\n无需修正任何分类")
            
            # 显示统计结果
            print("\n" + "=" * 60)
            print("修正结果统计:")
            print(f"包含'*'的客户总数: {len(customers_with_asterisk)}")
            print(f"保持为公司的客户: {len(company_kept)} 个")
            print(f"归类为个人的客户: {len(individual_corrections)} 个")
            print(f"实际修正的客户: {corrections_made} 个")
            
            # 显示详细分类结果
            if company_kept:
                print("\n保持为公司的客户 (以'司'结尾):")
                print("-" * 40)
                for i, (name, type_) in enumerate(company_kept, 1):
                    print(f"{i:2d}. {name} ({type_})")
            
            if individual_corrections:
                print("\n归类为个人的客户 (不以'司'结尾):")
                print("-" * 40)
                for i, (name, old_type, new_type) in enumerate(individual_corrections, 1):
                    status = "修正" if old_type != new_type else "保持"
                    print(f"{i:2d}. {name} ({old_type} -> {new_type}) [{status}]")
            
            # 显示修正后的总体统计
            print("\n修正后的总体统计:")
            print("-" * 40)
            
            total_customers = Customer.query.count()
            company_customers = Customer.query.filter_by(customer_type='公司').count()
            individual_customers = Customer.query.filter_by(customer_type='个人').count()
            
            print(f"总客户数: {total_customers}")
            print(f"公司客户: {company_customers} ({company_customers/total_customers*100:.1f}%)")
            print(f"个人客户: {individual_customers} ({individual_customers/total_customers*100:.1f}%)")
            
            print("\n" + "=" * 60)
            print("脱敏个人客户名称分类修正完成!")
            
        except Exception as e:
            print(f"修正失败: {str(e)}")
            db.session.rollback()
            raise

if __name__ == '__main__':
    fix_masked_individual_names()