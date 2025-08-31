from app import app
from models import db, Customer
from sqlalchemy import text
import re

def analyze_customer_type(customer_name):
    """
    根据客户名称分析客户类型
    返回: '公司' 或 '个人'
    """
    # 公司关键词列表
    company_keywords = [
        '有限公司', '股份有限公司', '集团', '公司', '企业', '厂', '中心', '院', '所', 
        '局', '部', '委', '会', '站', '处', '科技', '实业', '投资', '控股', '发展',
        '能源', '电力', '化工', '制造', '贸易', '服务', '咨询', '管理', '建设',
        '工程', '技术', '研究', '开发', '生产', '销售', '运营', '物流', '金融',
        '保险', '银行', '证券', '基金', '信托', '租赁', '担保', '小贷', '典当',
        '拍卖', '评估', '代理', '经纪', '中介', '广告', '传媒', '文化', '教育',
        '培训', '医疗', '健康', '养老', '旅游', '酒店', '餐饮', '零售', '批发',
        '超市', '商场', '市场', '商贸', '进出口', '外贸', '国际', '海外',
        '分公司', '子公司', '办事处', '代表处', '营业部', '门店', '专卖店',
        '4S店', '连锁', '加盟', '特许', '授权', '代销', '经销', '总代理',
        '一级代理', '二级代理', '三级代理', '区域代理', '独家代理', '总经销',
        '一级经销', '二级经销', '三级经销', '区域经销', '独家经销'
    ]
    
    # 个人关键词（通常个人名称不会包含这些，但可以作为排除条件）
    individual_keywords = [
        '个体', '个人', '自然人', '私人'
    ]
    
    # 检查是否包含公司关键词
    for keyword in company_keywords:
        if keyword in customer_name:
            return '公司'
    
    # 检查是否包含个人关键词
    for keyword in individual_keywords:
        if keyword in customer_name:
            return '个人'
    
    # 如果名称很短（通常2-4个字符）且不包含公司关键词，可能是个人
    if len(customer_name) <= 4 and not any(kw in customer_name for kw in company_keywords):
        return '个人'
    
    # 检查是否是纯中文姓名格式（2-4个字符，全是中文）
    if re.match(r'^[\u4e00-\u9fff]{2,4}$', customer_name):
        return '个人'
    
    # 默认情况下，如果无法明确判断，根据长度和复杂度判断
    # 长名称通常是公司，短名称可能是个人
    if len(customer_name) >= 8:
        return '公司'
    elif len(customer_name) <= 3:
        return '个人'
    else:
        # 中等长度的名称，检查是否包含常见的公司结构词
        company_structure_words = ['电力', '能源', '科技', '发展', '投资', '贸易', '实业']
        if any(word in customer_name for word in company_structure_words):
            return '公司'
        else:
            return '个人'

def analyze_and_update_customer_types():
    """
    分析所有客户的类型并更新数据库
    """
    print("开始分析客户类型...")
    print("=" * 60)
    
    with app.app_context():
        try:
            # 获取所有客户
            customers = Customer.query.all()
            print(f"总共需要分析 {len(customers)} 个客户")
            
            company_count = 0
            individual_count = 0
            updated_count = 0
            
            print("\n开始逐个分析...")
            print("-" * 60)
            
            for i, customer in enumerate(customers, 1):
                # 分析客户类型
                customer_type = analyze_customer_type(customer.customer_name)
                
                # 更新客户类型
                if customer.customer_type != customer_type:
                    customer.customer_type = customer_type
                    updated_count += 1
                
                # 统计
                if customer_type == '公司':
                    company_count += 1
                else:
                    individual_count += 1
                
                # 显示分析结果
                print(f"{i:3d}. {customer.customer_name:<30} -> {customer_type}")
            
            # 提交更新
            db.session.commit()
            
            print("\n" + "=" * 60)
            print("分析完成!")
            print(f"总计客户数量: {len(customers)}")
            print(f"公司客户: {company_count} 个 ({company_count/len(customers)*100:.1f}%)")
            print(f"个人客户: {individual_count} 个 ({individual_count/len(customers)*100:.1f}%)")
            print(f"更新记录数: {updated_count}")
            
            # 显示分类详情
            print("\n" + "=" * 60)
            print("分类详情:")
            print("=" * 60)
            
            # 显示公司客户
            companies = Customer.query.filter_by(customer_type='公司').all()
            print(f"\n公司客户 ({len(companies)}个):")
            print("-" * 40)
            for i, customer in enumerate(companies[:20], 1):  # 只显示前20个
                print(f"{i:2d}. {customer.customer_name}")
            if len(companies) > 20:
                print(f"    ... 还有 {len(companies) - 20} 个公司客户")
            
            # 显示个人客户
            individuals = Customer.query.filter_by(customer_type='个人').all()
            print(f"\n个人客户 ({len(individuals)}个):")
            print("-" * 40)
            for i, customer in enumerate(individuals, 1):
                print(f"{i:2d}. {customer.customer_name}")
            
            print("\n" + "=" * 60)
            print("客户类型分析和更新完成!")
            
        except Exception as e:
            print(f"分析失败: {str(e)}")
            db.session.rollback()
            raise

if __name__ == '__main__':
    analyze_and_update_customer_types()