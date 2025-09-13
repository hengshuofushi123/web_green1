# 文件: data_processors.py
# 包含数据处理核心函数、清空和插入逻辑

import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import db, Project
from .utils import parse_lzy_datetime, safe_int_cast

# 创建日志记录器
logger = logging.getLogger(__name__)

def clear_derived_data(project_id, source):
    """根据数据源，清空指定项目在关联表中的旧数据"""
    source_map = {
        "能源局网站": ["nyj_green_certificate_ledger", "nyj_transaction_records"],
        "绿证交易平台": ["gzpt_unilateral_listings", "gzpt_bilateral_online_trades", "gzpt_bilateral_offline_trades"],
        "北京电力交易中心": ["beijing_power_exchange_trades"],
        "广州电力交易中心": ["guangzhou_power_exchange_trades"]
    }
    tables_to_clear = source_map.get(source, [])
    for table_name in tables_to_clear:
        # 使用原生SQL执行删除，更高效
        db.session.execute(db.text(f"DELETE FROM {table_name} WHERE project_id = :pid"), {'pid': project_id})

def process_nyj_data(project):
    """处理能源局数据并准备插入"""
    data = json.loads(project.data_nyj) if project.data_nyj else {}

    # 1. 处理交易记录
    records = []
    for record in data.get("交易记录", []):
        records.append({
            'project_id': project.id, 'city': record.get('city'), 'county': record.get('county'),
            'order_id': record.get('orderId'), 'province': record.get('province'),
            'record_project_name': record.get('projectName'), 'production_year': record.get('productionYear'),
            'transaction_num': record.get('transactionNum'), 'buyer_unique_code': record.get('buyerUniqueCode'),
            'production_month': record.get('productionMonth'), 'transaction_time': record.get('transactionTime'),
            'transaction_type': record.get('transactionType')
        })
    if records:
        db.session.execute(db.text(
            "INSERT INTO nyj_transaction_records (project_id, city, county, order_id, province, record_project_name, production_year, transaction_num, buyer_unique_code, production_month, transaction_time, transaction_type) VALUES (:project_id, :city, :county, :order_id, :province, :record_project_name, :production_year, :transaction_num, :buyer_unique_code, :production_month, :transaction_time, :transaction_type)"),
                           records)

    # 2. 处理绿证台账
    ledgers = []
    for ledger in data.get("绿证台账", []):
        ledgers.append({
            'project_id': project.id, 'city': ledger.get('city'), 'year': ledger.get('year'),
            'month': ledger.get('month'), 'county': ledger.get('county'), 'province': ledger.get('province'),
            'issue_type': ledger.get('issueType'), 'shelf_load': ledger.get('shelfLoad'),
            'project_code': ledger.get('projectCode'), 'record_project_name': ledger.get('projectName'),
            'tra_quantity': ledger.get('traQuantity'), 'unshelf_load': ledger.get('unshelfLoad'),
            'sold_quantity': ledger.get('soldQuantity'), 'gec_unique_code': ledger.get('gecUniqueCode'),
            'green_quantity': ledger.get('greenQuantity'), 'un_tra_quantity': ledger.get('unTraQuantity'),
            'unsold_quantity': ledger.get('unsoldQuantity'), 'release_quantity': ledger.get('releaseQuantity'),
            'ordinary_quantity': ledger.get('ordinaryQuantity'),
            'production_year_month': ledger.get('productionYearMonth')
        })
    if ledgers:
        sql = db.text(
            "INSERT INTO nyj_green_certificate_ledger (project_id, city, `year`, `month`, county, province, issue_type, shelf_load, project_code, record_project_name, tra_quantity, unshelf_load, sold_quantity, gec_unique_code, green_quantity, un_tra_quantity, unsold_quantity, release_quantity, ordinary_quantity, production_year_month) VALUES (:project_id, :city, :year, :month, :county, :province, :issue_type, :shelf_load, :project_code, :record_project_name, :tra_quantity, :unshelf_load, :sold_quantity, :gec_unique_code, :green_quantity, :un_tra_quantity, :unsold_quantity, :release_quantity, :ordinary_quantity, :production_year_month)")
        db.session.execute(sql, ledgers)

def process_gzpt_data(project):
    """处理绿证平台数据并准备插入 - 适配新的API数据结构"""
    data = json.loads(project.data_lzy) if project.data_lzy else {}
    trade_types = {"单向挂牌": "gzpt_unilateral_listings", "双边线上": "gzpt_bilateral_online_trades",
                   "双边线下": "gzpt_bilateral_offline_trades"}

    for trade_key, table_name in trade_types.items():
        records_to_insert = []
        for record in data.get(trade_key, []):
            # 映射新的API数据结构到数据库字段
            records_to_insert.append({
                'project_id': project.id,
                'order_id': record.get('orderId'),
                'item_id': record.get('itemId'),
                'product': record.get('product'),
                'brokers': record.get('brokers'),
                'project_name': record.get('projectName'),
                'project_type': record.get('projectType'),
                'transfer': record.get('transfer'),
                'create_date': parse_lzy_datetime(record.get('createDate')),
                'product_source': record.get('productSource'),
                'project_property': record.get('projectProperty'),
                'sn': record.get('sn'),
                'member_name': record.get('memberName'),
                'seller_name': record.get('sellerName'),
                'generate_ym': record.get('generateYm'),
                'member': safe_int_cast(record.get('member')),
                'seller': safe_int_cast(record.get('seller')),
                'total_quantity': record.get('totalQuantity'),
                'total_amount': record.get('totalAmount'),
                'order_time': parse_lzy_datetime(record.get('orderTime')),
                'payment_method_name': record.get('paymentMethodName'),
                'payment_method': record.get('paymentMethod'),
                'payment_status': record.get('paymentStatus'),
                'order_status': record.get('orderStatus'),
                'order_type': record.get('orderType'),
                'trade_code': record.get('tradeCode'),
                'expire': parse_lzy_datetime(record.get('expire')),
                'pay_time': parse_lzy_datetime(record.get('payTime')),
                'approve_time': parse_lzy_datetime(record.get('approveTime')),
                'rest_time': parse_lzy_datetime(record.get('restTime')),
                'agreement': record.get('agreement'),
                'is_online': record.get('isOnline'),
                'thumbnail': record.get('thumbnail'),
                'approve_reason': record.get('approveReason'),
                'pay_failure_reason': record.get('payFailureReason'),
                'interest': record.get('interest'),
                'province': record.get('province'),
                'payment_sn': record.get('paymentSn'),
                'certificate_honor': record.get('certificateHonor'),
                'platform_type': record.get('platformType'),
                'price': record.get('price'),
                'possessor': record.get('possessor'),
                'center': record.get('center'),
                'is_anonymous': record.get('isAnonymous'),
                'tx_code': record.get('txCode'),
                'quantity': record.get('quantity'),
                'amount': record.get('amount'),
                'order_time_str': record.get('orderTimeStr')
            })
        if records_to_insert:
            # 构建动态SQL插入语句，包含所有字段
            columns = list(records_to_insert[0].keys())
            placeholders = [f":{col}" for col in columns]
            sql = db.text(
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})")
            db.session.execute(sql, records_to_insert)

def process_beijing_trades(project):
    """处理北京交易中心数据并准备插入"""
    data = json.loads(project.data_bjdl) if project.data_bjdl else []
    if not data or len(data) < 2: return
    records_to_insert = []
    for record in data[1:]:
        try:
            records_to_insert.append({
                'project_id': project.id, 'transaction_type': record.get('Unnamed: 1'),
                'seller_entity_name': record.get('Unnamed: 2'), 'seller_province': record.get('Unnamed: 3'),
                'buyer_entity_name': record.get('Unnamed: 4'), 'buyer_province': record.get('Unnamed: 5'),
                'transaction_year': record.get('Unnamed: 6'), 'production_year_month': record.get('Unnamed: 7'),
                'subsidy_type': record.get('Unnamed: 8'), 'certificate_code': record.get('Unnamed: 9'),
                'transaction_price': Decimal(record.get('Unnamed: 10')) if record.get(
                    'Unnamed: 10') is not None else None,
                'transaction_quantity': int(record.get('Unnamed: 11')) if record.get(
                    'Unnamed: 11') is not None else None,
                'transaction_time': record.get('Unnamed: 12'), 'record_project_name': record.get('平价绿证交易结果')
            })
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.warning(f"跳过一条格式错误的bjdl记录: {record}，错误: {e}")
            continue
    if records_to_insert:
        sql = db.text(
            "INSERT INTO beijing_power_exchange_trades (project_id, transaction_type, seller_entity_name, seller_province, buyer_entity_name, buyer_province, transaction_year, production_year_month, subsidy_type, certificate_code, transaction_price, transaction_quantity, transaction_time, record_project_name) VALUES (:project_id, :transaction_type, :seller_entity_name, :seller_province, :buyer_entity_name, :buyer_province, :transaction_year, :production_year_month, :subsidy_type, :certificate_code, :transaction_price, :transaction_quantity, :transaction_time, :record_project_name)")
        db.session.execute(sql, records_to_insert)

def process_guangzhou_trades(project):
    """【已重构】处理来自广州交易中心(GJDL)的数据，包含所有字段"""
    # 确保从JSON加载数据，如果字段为空则视为空列表
    data = json.loads(project.data_gjdl) if project.data_gjdl else []

    def parse_iso_datetime(dt_str):
        """辅助函数，用于解析API返回的日期时间字符串"""
        if not dt_str or not isinstance(dt_str, str):
            return None
        # 处理类似 "2025-03-19T15:34:12" 的格式
        return dt_str.split('.')[0].replace('T', ' ')
    
    def normalize_product_date(date_str):
        """标准化product_date格式为YYYY-MM"""
        if not date_str or not isinstance(date_str, str):
            return date_str
        
        # 如果已经是YYYY-MM格式，直接返回
        if len(date_str) == 7 and date_str.count('-') == 1:
            year, month = date_str.split('-')
            if len(year) == 4 and len(month) == 2:
                return date_str
            elif len(year) == 4 and len(month) == 1:
                # 将YYYY-M格式转换为YYYY-MM
                return f"{year}-{month.zfill(2)}"
        
        return date_str

    records_to_insert = []
    for record in data:
        # 将每一行JSON数据映射到一个字典，键名与数据库列名一致
        records_to_insert.append({
            'project_id': project.id,
            'order_no': record.get('orderNo'),
            'declare_no': record.get('declareNo'),
            'record_project_name': record.get('projectName'),
            'product_date': normalize_product_date(record.get('productDate')),
            'project_type': record.get('projectType'),
            'subsidy_type': record.get('subsidyType'),
            'seller_entity_name': record.get('marketEntityNameSeller'),
            'buyer_entity_name': record.get('marketEntityNameBuyer'),
            'trade_type': record.get('tradeType'),
            'unit_price': record.get('unitPrice'),
            'gpc_certifi_num': record.get('gpcCertifiNum'),
            'total_cost': record.get('totalCost'),
            'pay_status': record.get('payStatus'),
            'distri_status': record.get('distriStatus'),
            'pt_status': record.get('ptStatus'),
            'gcc_wares_id': record.get('gccWaresId'),
            'project_img_atta_id': record.get('projectImgAttaId'),
            'province': record.get('province'),
            'deal_time': parse_iso_datetime(record.get('dealTime')),
            'pay_time': parse_iso_datetime(record.get('payTime')),
            'distri_time': parse_iso_datetime(record.get('distriTime')),
            'pt_time': parse_iso_datetime(record.get('ptTime')),
            'order_detail_id': record.get('orderDetailId'),
            'gpc_creei_status': record.get('gpcCreeiStatus'),
            'market_entity_id_buyer': record.get('marketEntityIdBuyer'),
            'is_show': record.get('isShow'),
            'list_mode': record.get('listMode'),
            'order_status': record.get('orderStatus'),
            'pay_categ': record.get('payCateg'),
            'refund_flag': record.get('refundFlag'),
            'gpc_certifi_cost': record.get('gpcCertifiCost'),
            'fee_cost_buyer': record.get('feeCostBuyer'),
            'fee_cost_seller': record.get('feeCostSeller'),
            'buyer_ss_cost': record.get('buyerSsCost'),
            'seller_ss_cost': record.get('sllerSsCost'),  # 注意：源JSON中可能有拼写错误 'sller'
            'total_fee': record.get('totalFee'),
            'oper_order_type': record.get('operOrderType'),
            'refund_cause_flag': record.get('refundCauseFlag'),
            'market_entity_id_seller': record.get('marketEntityIdSeller'),
            'declare_time': parse_iso_datetime(record.get('declareTime')),
            'pay_type': record.get('payType'),
            'pay_order_total': record.get('payOrderTotal'),
            'pay_sxf_total': record.get('paySxfTotal'),
            'total_fee_seller': record.get('totalFeeSeller'),
            'total_fee_buyer': record.get('totalFeeBuyer'),
            'list_type': record.get('listType'),
            'env_equity': record.get('envEquity'),
            'province_other': record.get('provinceOther'),
            'primary_value': record.get('primaryValue'),
        })

    if records_to_insert:
        # 构建一个包含所有列的、完整的INSERT语句
        # 使用命名参数 (:key) 可以让代码更清晰，并能自动处理None值
        sql = db.text("""
            INSERT INTO guangzhou_power_exchange_trades (
                project_id, order_no, declare_no, record_project_name, product_date, project_type, subsidy_type, 
                seller_entity_name, buyer_entity_name, trade_type, unit_price, gpc_certifi_num, total_cost, 
                pay_status, distri_status, pt_status, gcc_wares_id, project_img_atta_id, province, 
                deal_time, pay_time, distri_time, pt_time, order_detail_id, gpc_creei_status, 
                market_entity_id_buyer, is_show, list_mode, order_status, pay_categ, refund_flag, 
                gpc_certifi_cost, fee_cost_buyer, fee_cost_seller, buyer_ss_cost, seller_ss_cost, 
                total_fee, oper_order_type, refund_cause_flag, market_entity_id_seller, declare_time, 
                pay_type, pay_order_total, pay_sxf_total, total_fee_seller, total_fee_buyer, list_type, 
                env_equity, province_other, primary_value
            ) VALUES (
                :project_id, :order_no, :declare_no, :record_project_name, :product_date, :project_type, :subsidy_type, 
                :seller_entity_name, :buyer_entity_name, :trade_type, :unit_price, :gpc_certifi_num, :total_cost, 
                :pay_status, :distri_status, :pt_status, :gcc_wares_id, :project_img_atta_id, :province, 
                :deal_time, :pay_time, :distri_time, :pt_time, :order_detail_id, :gpc_creei_status, 
                :market_entity_id_buyer, :is_show, :list_mode, :order_status, :pay_categ, :refund_flag, 
                :gpc_certifi_cost, :fee_cost_buyer, :fee_cost_seller, :buyer_ss_cost, :seller_ss_cost, 
                :total_fee, :oper_order_type, :refund_cause_flag, :market_entity_id_seller, :declare_time, 
                :pay_type, :pay_order_total, :pay_sxf_total, :total_fee_seller, :total_fee_buyer, :list_type, 
                :env_equity, :province_other, :primary_value
            )
        """)
        # 使用 executemany 的方式批量插入，效率更高
        db.session.execute(sql, records_to_insert)

def update_derived_tables(project_id, source):
    """核心函数：根据项目ID和数据源，清空并重新填充关联表数据"""
    try:
        clear_derived_data(project_id, source)
        project = Project.query.get(project_id)
        if not project: return

        if source == "能源局网站":
            process_nyj_data(project)
        elif source == "绿证交易平台":
            process_gzpt_data(project)
        elif source == "北京电力交易中心":
            process_beijing_trades(project)
        elif source == "广州电力交易中心":
            process_guangzhou_trades(project)

        logger.info(f"项目 {project_id} 的 {source} 关联数据已更新。")
    except Exception as e:
        logger.error(f"更新关联表时出错 (项目ID: {project_id}, 源: {source}): {e}")
        raise e