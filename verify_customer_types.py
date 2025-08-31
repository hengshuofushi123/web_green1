from app import app
from models import db, Customer
from sqlalchemy import text

def verify_customer_types():
    """
    验证客户类型分析结果
    """
    print("验证客户类型分析结果...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 1. 总体统计
            total_customers = Customer.query.count()
            company_customers = Customer.query.filter_by(customer_type='公司').count()
            individual_customers = Customer.query.filter_by(customer_type='个人').count()
            unclassified_customers = Customer.query.filter(Customer.customer_type.is_(None)).count()
            
            print("1. 总体统计:")
            print(f"   总客户数: {total_customers}")
            print(f"   公司客户: {company_customers} ({company_customers/total_customers*100:.1f}%)")
            print(f"   个人客户: {individual_customers} ({individual_customers/total_customers*100:.1f}%)")
            print(f"   未分类客户: {unclassified_customers} ({unclassified_customers/total_customers*100:.1f}%)")
            
            # 2. 显示所有个人客户（因为数量较少）
            print("\n2. 个人客户详情:")
            print("-" * 40)
            individuals = Customer.query.filter_by(customer_type='个人').all()
            if individuals:
                for i, customer in enumerate(individuals, 1):
                    print(f"{i:2d}. {customer.customer_name} (长度: {len(customer.customer_name)}字符)")
            else:
                print("   无个人客户")
            
            # 3. 显示公司客户样本（按名称长度排序）
            print("\n3. 公司客户样本 (按名称长度排序):")
            print("-" * 40)
            companies = Customer.query.filter_by(customer_type='公司').order_by(Customer.customer_name).limit(15).all()
            for i, customer in enumerate(companies, 1):
                print(f"{i:2d}. {customer.customer_name} (长度: {len(customer.customer_name)}字符)")
            
            # 4. 名称长度分析
            print("\n4. 客户名称长度分析:")
            print("-" * 40)
            
            # 公司客户名称长度
            company_lengths = [len(c.customer_name) for c in Customer.query.filter_by(customer_type='公司').all()]
            if company_lengths:
                print(f"   公司客户名称长度: 最短{min(company_lengths)}字符, 最长{max(company_lengths)}字符, 平均{sum(company_lengths)/len(company_lengths):.1f}字符")
            
            # 个人客户名称长度
            individual_lengths = [len(c.customer_name) for c in Customer.query.filter_by(customer_type='个人').all()]
            if individual_lengths:
                print(f"   个人客户名称长度: 最短{min(individual_lengths)}字符, 最长{max(individual_lengths)}字符, 平均{sum(individual_lengths)/len(individual_lengths):.1f}字符")
            
            # 5. 检查可能的误分类
            print("\n5. 潜在误分类检查:")
            print("-" * 40)
            
            # 检查名称很短但被分类为公司的客户
            short_companies = Customer.query.filter(
                Customer.customer_type == '公司',
                db.func.length(Customer.customer_name) <= 6
            ).all()
            
            if short_companies:
                print("   名称较短但被分类为公司的客户:")
                for customer in short_companies:
                    print(f"     - {customer.customer_name} ({len(customer.customer_name)}字符)")
            else:
                print("   未发现名称过短的公司客户")
            
            # 检查名称很长但被分类为个人的客户
            long_individuals = Customer.query.filter(
                Customer.customer_type == '个人',
                db.func.length(Customer.customer_name) >= 8
            ).all()
            
            if long_individuals:
                print("   名称较长但被分类为个人的客户:")
                for customer in long_individuals:
                    print(f"     - {customer.customer_name} ({len(customer.customer_name)}字符)")
            else:
                print("   未发现名称过长的个人客户")
            
            # 6. 按行业关键词统计公司类型
            print("\n6. 公司行业分布分析:")
            print("-" * 40)
            
            industry_keywords = {
                '电力/能源': ['电力', '能源', '电厂', '发电', '供电', '配电', '输电', '电网'],
                '化工/材料': ['化工', '化学', '材料', '塑料', '橡胶', '涂料', '树脂'],
                '制造/工业': ['制造', '工业', '机械', '设备', '装备', '重工', '轻工'],
                '科技/技术': ['科技', '技术', '软件', '信息', '数据', '智能', '电子'],
                '汽车/交通': ['汽车', '车辆', '交通', '运输', '物流', '货运'],
                '金融/投资': ['金融', '投资', '银行', '保险', '证券', '基金', '信托'],
                '贸易/商业': ['贸易', '商业', '商贸', '销售', '经销', '代理', '进出口']
            }
            
            companies_all = Customer.query.filter_by(customer_type='公司').all()
            for industry, keywords in industry_keywords.items():
                count = 0
                for company in companies_all:
                    if any(keyword in company.customer_name for keyword in keywords):
                        count += 1
                if count > 0:
                    print(f"   {industry}: {count}家公司")
            
            print("\n" + "=" * 60)
            print("客户类型验证完成!")
            
        except Exception as e:
            print(f"验证失败: {str(e)}")
            raise

if __name__ == '__main__':
    verify_customer_types()