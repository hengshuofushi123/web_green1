from app import app
from models import db, Customer
from sqlalchemy import text

def update_company_provinces():
    """
    对于所有customer表中类型为公司的客户，测试客户名称的前2个字是否与某个省级行政区的前2个字吻合，
    如果吻合则更新'省份'字段为对应的省级行政区
    """
    
    # 定义省级行政区列表（包括省、自治区、直辖市、特别行政区）
    provinces = [
        '北京市', '天津市', '上海市', '重庆市',  # 直辖市
        '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省',  # 省份
        '江苏省', '浙江省', '安徽省', '福建省', '江西省', '山东省',
        '河南省', '湖北省', '湖南省', '广东省', '海南省',
        '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省', '台湾省',
        '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区',  # 自治区
        '香港特别行政区', '澳门特别行政区'  # 特别行政区
    ]
    
    # 创建前2个字到完整省名的映射
    province_mapping = {}
    for province in provinces:
        prefix = province[:2]
        province_mapping[prefix] = province
    
    print("省级行政区前缀映射:")
    for prefix, full_name in province_mapping.items():
        print(f"  {prefix} -> {full_name}")
    print()
    
    with app.app_context():
        try:
            # 查询所有类型为'公司'的客户
            company_customers = Customer.query.filter_by(customer_type='公司').all()
            print(f"找到 {len(company_customers)} 个公司类型的客户")
            
            if not company_customers:
                print("没有找到公司类型的客户")
                return
            
            updated_count = 0
            matched_count = 0
            
            print("\n开始匹配和更新:")
            print("-" * 80)
            
            for customer in company_customers:
                customer_name = customer.customer_name
                if len(customer_name) >= 2:
                    name_prefix = customer_name[:2]
                    
                    if name_prefix in province_mapping:
                        matched_province = province_mapping[name_prefix]
                        matched_count += 1
                        
                        # 检查是否需要更新
                        if customer.province != matched_province:
                            old_province = customer.province or '未设置'
                            customer.province = matched_province
                            updated_count += 1
                            print(f"{updated_count:3d}. {customer_name[:20]:<20} | {name_prefix} -> {matched_province} | 原: {old_province}")
                        else:
                            print(f"     {customer_name[:20]:<20} | {name_prefix} -> {matched_province} | 已正确")
                    else:
                        print(f"     {customer_name[:20]:<20} | {name_prefix} -> 无匹配")
                else:
                    print(f"     {customer_name[:20]:<20} | 名称太短")
            
            # 提交更改
            if updated_count > 0:
                db.session.commit()
                print(f"\n成功更新 {updated_count} 个客户的省份信息")
            else:
                print("\n没有需要更新的客户")
            
            print(f"\n统计结果:")
            print(f"  总公司客户数: {len(company_customers)}")
            print(f"  匹配到省份的客户数: {matched_count}")
            print(f"  实际更新的客户数: {updated_count}")
            print(f"  匹配率: {matched_count/len(company_customers)*100:.1f}%")
            
            # 显示更新后的省份分布
            print("\n更新后的省份分布:")
            print("-" * 40)
            
            province_stats = db.session.query(
                Customer.province, 
                db.func.count(Customer.customer_name)
            ).filter_by(customer_type='公司').group_by(Customer.province).order_by(
                db.func.count(Customer.customer_name).desc()
            ).all()
            
            for province, count in province_stats:
                province_name = province or '未设置'
                print(f"  {province_name:<15}: {count:3d} 个客户")
                
        except Exception as e:
            print(f"更新失败: {str(e)}")
            db.session.rollback()
            raise

if __name__ == '__main__':
    update_company_provinces()