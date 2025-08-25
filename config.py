# 文件: config.py
# 包含所有配置常量和设置

import os

# --- API 配置 ---
# 在实际生产环境中，这个Token应该存储在环境变量或安全的配置文件中
# 为了演示，我们将其硬编码在这里
# 任何想要访问API的外部服务都必须提供这个Token
API_ACCESS_TOKEN = 'your-super-secret-and-long-token'

# --- 配置 ---
SECRET_KEY = 'a-very-secret-key'  # 生产环境中请使用更复杂的密钥
SQLALCHEMY_DATABASE_URI = 'mysql://root:root@localhost/green'  # 修改为你的MySQL连接信息
SQLALCHEMY_TRACK_MODIFICATIONS = False

# --- 数据库连接池最终配置 ---
# 彻底解决 (2013, 'Lost connection to server during query') 错误
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_recycle': 280,  # 将连接回收时间设置为 280 秒，以应对短的 wait_timeout
    'pool_pre_ping': True  # 在每次使用连接前进行“ping”测试，确保连接是活动的
}

# --- 数据查看页面表头顺序配置 ---
TABLE_HEADER_ORDERS = {
    'nyj_green_certificate_ledger': ['电量生产年月', '省份', '城市', '核发量', '已上架', '未上架', '已出售', '未出售', '可交易量', '不可交易量', '普通绿证', '绿电绿证'],
    'nyj_green_certificate_transactions': [],
    'gzpt_unilateral_listings': ['订单号', '订单时间', '交易方名称', '电量生产年月', '交易量（张）', '金额', '订单状态', '单价'],
    'gzpt_bilateral_online_trades': ['订单号', '订单时间', '交易方名称', '电量生产年月', '交易量（张）', '金额', '订单状态', '单价'],
    'gzpt_bilateral_offline_trades': ['订单号', '订单时间', '交易方名称', '电量生产年月', '交易量（张）', '金额', '订单状态', '单价'],
    'beijing_power_exchange_trades': ['成交时间', '交易类型', '购方主体名称', '购方省份', '交易年份', '生产年月', '补贴类型', '绿证编码', '成交数量(个)', '成交单价(元/个)'],
    'guangzhou_power_exchange_trades': ['订单编号', '成交时间', '买方名称', '电力生产年月', '交易量（张）', '价格', '交易金额']
}