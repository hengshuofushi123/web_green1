from app import app
from models import db, Customer
from sqlalchemy import text

def fix_customer_classification():
    """
    修正客户分类错误
    主要针对被错误分类为个人的脱敏公司名称
    """
    print("修正客户分类错误...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 查找所有被分类为个人的客户
            individuals = Customer.query.filter_by(customer_type='个人').all()
            
            print(f"当前被分类为个人的客户数量: {len(individuals)}")
            print("\n检查每个个人客户:")
            print("-" * 40)
            
            corrections_made = 0
            
            for customer in individuals:
                print(f"客户名称: {customer.customer_name}")
                
                # 检查是否为脱敏的公司名称
                # 特征：以'***司'、'***厂'、'***院'、'***行'、'***社'等结尾
                company_suffixes = ['***司', '***厂', '***院', '***行', '***社', '***店', '***部']
                
                is_likely_company = False
                
                # 检查公司后缀
                for suffix in company_suffixes:
                    if customer.customer_name.endswith(suffix):
                        is_likely_company = True
                        print(f"  -> 发现公司后缀 '{suffix}'，应为公司")
                        break
                
                # 检查其他公司特征
                if not is_likely_company:
                    # 检查是否包含公司关键词（即使被脱敏）
                    company_keywords = ['有限', '股份', '集团', '公司', '企业', '工厂', '研究院', '银行', '保险']
                    for keyword in company_keywords:
                        if keyword in customer.customer_name:
                            is_likely_company = True
                            print(f"  -> 发现公司关键词 '{keyword}'，应为公司")
                            break
                
                # 如果判断为公司，则更新分类
                if is_likely_company:
                    customer.customer_type = '公司'
                    corrections_made += 1
                    print(f"  -> 已修正为公司")
                else:
                    print(f"  -> 保持个人分类")
                
                print()
            
            # 提交更改
            if corrections_made > 0:
                db.session.commit()
                print(f"成功修正 {corrections_made} 个客户的分类")
            else:
                print("无需修正任何分类")
            
            # 显示修正后的统计
            print("\n修正后的统计:")
            print("-" * 40)
            
            total_customers = Customer.query.count()
            company_customers = Customer.query.filter_by(customer_type='公司').count()
            individual_customers = Customer.query.filter_by(customer_type='个人').count()
            
            print(f"总客户数: {total_customers}")
            print(f"公司客户: {company_customers} ({company_customers/total_customers*100:.1f}%)")
            print(f"个人客户: {individual_customers} ({individual_customers/total_customers*100:.1f}%)")
            
            # 显示剩余的个人客户（如果有）
            remaining_individuals = Customer.query.filter_by(customer_type='个人').all()
            if remaining_individuals:
                print("\n剩余个人客户:")
                for customer in remaining_individuals:
                    print(f"  - {customer.customer_name}")
            else:
                print("\n所有客户均已正确分类为公司")
            
            print("\n" + "=" * 60)
            print("客户分类修正完成!")
            
        except Exception as e:
            print(f"修正失败: {str(e)}")
            db.session.rollback()
            raise

if __name__ == '__main__':
    fix_customer_classification()