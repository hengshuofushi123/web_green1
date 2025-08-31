from app import app
from models import db, Customer
from sqlalchemy import text
import re

def verify_complete_customers():
    """
    验证完整客户表的数据质量和分布
    """
    print("验证完整客户表数据...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 1. 总体统计
            total_customers = Customer.query.count()
            company_customers = Customer.query.filter_by(customer_type='公司').count()
            individual_customers = Customer.query.filter_by(customer_type='个人').count()
            
            print("1. 总体统计:")
            print(f"   总客户数: {total_customers}")
            print(f"   公司客户: {company_customers} ({company_customers/total_customers*100:.1f}%)")
            print(f"   个人客户: {individual_customers} ({individual_customers/total_customers*100:.1f}%)")
            
            # 2. 数据来源分析
            print("\n2. 数据来源验证:")
            print("-" * 40)
            
            with db.engine.connect() as connection:
                # 验证各表的数据是否都包含在客户表中
                tables_info = [
                    ('beijing_power_exchange_trades', 'buyer_entity_name'),
                    ('gzpt_bilateral_offline_trades', 'member_name'),
                    ('gzpt_bilateral_online_trades', 'member_name'),
                    ('gzpt_unilateral_listings', 'member_name'),
                    ('guangzhou_power_exchange_trades', 'buyer_entity_name')
                ]
                
                for table_name, column_name in tables_info:
                    try:
                        query = text(f"""
                            SELECT COUNT(DISTINCT {column_name}) as count
                            FROM {table_name}
                            WHERE {column_name} IS NOT NULL AND {column_name} != ''
                        """)
                        result = connection.execute(query)
                        count = result.fetchone()[0]
                        print(f"   {table_name}: {count} 个不重复实体")
                    except Exception as e:
                        print(f"   {table_name}: 查询失败 - {str(e)}")
            
            # 3. 客户名称长度分析
            print("\n3. 客户名称长度分析:")
            print("-" * 40)
            
            # 公司客户名称长度
            companies = Customer.query.filter_by(customer_type='公司').all()
            company_lengths = [len(c.customer_name) for c in companies]
            if company_lengths:
                print(f"   公司客户名称长度: 最短{min(company_lengths)}字符, 最长{max(company_lengths)}字符, 平均{sum(company_lengths)/len(company_lengths):.1f}字符")
            
            # 个人客户名称长度
            individuals = Customer.query.filter_by(customer_type='个人').all()
            individual_lengths = [len(c.customer_name) for c in individuals]
            if individual_lengths:
                print(f"   个人客户名称长度: 最短{min(individual_lengths)}字符, 最长{max(individual_lengths)}字符, 平均{sum(individual_lengths)/len(individual_lengths):.1f}字符")
            
            # 4. 个人客户样本分析
            print("\n4. 个人客户样本分析 (前30个):")
            print("-" * 40)
            
            sample_individuals = Customer.query.filter_by(customer_type='个人').limit(30).all()
            for i, customer in enumerate(sample_individuals, 1):
                print(f"{i:2d}. {customer.customer_name} (长度: {len(customer.customer_name)}字符)")
            
            # 5. 检查可能的分类错误
            print("\n5. 潜在分类错误检查:")
            print("-" * 40)
            
            # 检查长名称但被分类为个人的客户
            long_individuals = Customer.query.filter(
                Customer.customer_type == '个人',
                db.func.length(Customer.customer_name) >= 10
            ).limit(20).all()
            
            if long_individuals:
                print("   名称较长但被分类为个人的客户 (前20个):")
                for customer in long_individuals:
                    print(f"     - {customer.customer_name} ({len(customer.customer_name)}字符)")
            else:
                print("   未发现名称过长的个人客户")
            
            # 检查短名称但被分类为公司的客户
            short_companies = Customer.query.filter(
                Customer.customer_type == '公司',
                db.func.length(Customer.customer_name) <= 5
            ).limit(20).all()
            
            if short_companies:
                print("\n   名称较短但被分类为公司的客户 (前20个):")
                for customer in short_companies:
                    print(f"     - {customer.customer_name} ({len(customer.customer_name)}字符)")
            else:
                print("\n   未发现名称过短的公司客户")
            
            # 6. 特殊字符和格式分析
            print("\n6. 特殊字符和格式分析:")
            print("-" * 40)
            
            # 检查包含数字的客户名称
            customers_with_numbers = []
            all_customers_sample = Customer.query.limit(200).all()
            for customer in all_customers_sample:
                if re.search(r'[0-9]', customer.customer_name):
                    customers_with_numbers.append(customer)
                    if len(customers_with_numbers) >= 10:
                        break
            
            if customers_with_numbers:
                print("   包含数字的客户名称 (前10个):")
                for customer in customers_with_numbers:
                    print(f"     - {customer.customer_name} ({customer.customer_type})")
            
            # 检查包含英文的客户名称
            english_pattern_customers = []
            all_customers_sample = Customer.query.limit(100).all()
            for customer in all_customers_sample:
                if re.search(r'[a-zA-Z]', customer.customer_name):
                    english_pattern_customers.append(customer)
                    if len(english_pattern_customers) >= 10:
                        break
            
            if english_pattern_customers:
                print("\n   包含英文字符的客户名称 (前10个):")
                for customer in english_pattern_customers:
                    print(f"     - {customer.customer_name} ({customer.customer_type})")
            
            # 7. 行业分布分析（仅公司）
            print("\n7. 公司行业分布分析:")
            print("-" * 40)
            
            industry_keywords = {
                '电力/能源': ['电力', '能源', '电厂', '发电', '供电', '配电', '输电', '电网', '新能源', '光伏', '风电'],
                '化工/材料': ['化工', '化学', '材料', '塑料', '橡胶', '涂料', '树脂', '石化'],
                '制造/工业': ['制造', '工业', '机械', '设备', '装备', '重工', '轻工', '生产'],
                '科技/技术': ['科技', '技术', '软件', '信息', '数据', '智能', '电子', '通信'],
                '汽车/交通': ['汽车', '车辆', '交通', '运输', '物流', '货运'],
                '金融/投资': ['金融', '投资', '银行', '保险', '证券', '基金', '信托'],
                '贸易/商业': ['贸易', '商业', '商贸', '销售', '经销', '代理', '进出口'],
                '建筑/房地产': ['建筑', '建设', '房地产', '地产', '工程', '建材'],
                '医疗/健康': ['医疗', '医院', '健康', '药业', '生物', '医药'],
                '教育/研究': ['教育', '学校', '大学', '研究', '学院', '培训']
            }
            
            companies_all = Customer.query.filter_by(customer_type='公司').all()
            for industry, keywords in industry_keywords.items():
                count = 0
                for company in companies_all:
                    if any(keyword in company.customer_name for keyword in keywords):
                        count += 1
                if count > 0:
                    print(f"   {industry}: {count}家公司 ({count/len(companies_all)*100:.1f}%)")
            
            print("\n" + "=" * 60)
            print("完整客户表验证完成!")
            
        except Exception as e:
            print(f"验证失败: {str(e)}")
            raise

if __name__ == '__main__':
    verify_complete_customers()