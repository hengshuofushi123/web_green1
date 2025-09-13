from flask_login import UserMixin
from passlib.hash import sha256_crypt
from datetime import datetime

# 从 app/__init__.py 中导入 db 实例
from . import db

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)

    def set_password(self, password):
        self.password_hash = sha256_crypt.hash(password)

    def check_password(self, password):
        return sha256_crypt.verify(password, self.password_hash)

    @property
    def is_admin(self):
        return self.username == 'admin'

class Project(db.Model):
    __tablename__ = 'projects'
    id = db.Column(db.Integer, primary_key=True)
    last_updated_date = db.Column(db.Date, nullable=True)
    secondary_unit = db.Column(db.String(255), nullable=False)
    secondary_unit_contact = db.Column(db.String(255))
    province = db.Column(db.String(100))
    region = db.Column(db.String(100))
    project_name = db.Column(db.String(255), unique=True, nullable=False)
    company_name = db.Column(db.String(255))
    investment_scope = db.Column(db.String(50))
    project_nature = db.Column(db.String(50))
    power_type = db.Column(db.String(50))
    is_uhv_support = db.Column(db.Boolean, default=False)
    has_subsidy = db.Column(db.Boolean, default=False)
    capacity_mw = db.Column(db.Numeric(10, 2))
    production_year = db.Column(db.Integer)
    production_month = db.Column(db.Integer)
    is_filed = db.Column(db.Boolean, default=False)
    is_beijing_registered = db.Column(db.Boolean, default=False)
    is_guangzhou_registered = db.Column(db.Boolean, default=False)
    is_green_cert_registered = db.Column(db.Boolean, default=True)  # 是否完成绿证交易平台注册
    has_beijing_transaction = db.Column(db.Boolean, default=True)   # 在北交做过交易
    has_guangzhou_transaction = db.Column(db.Boolean, default=True) # 在广交做过交易
    has_green_cert_transaction = db.Column(db.Boolean, default=True) # 在绿证交易平台做过交易

    # --- 新增字段，用于存储JSON数据 ---
    data_nyj = db.Column(db.Text, nullable=True)  # 能源局网站
    data_lzy = db.Column(db.Text, nullable=True)  # 绿证交易平台
    data_bjdl = db.Column(db.Text, nullable=True) # 北京电力交易中心
    data_gjdl = db.Column(db.Text, nullable=True) # 广州电力交易中心

    data_nyj_updated_at = db.Column(db.DateTime, nullable=True)
    data_lzy_updated_at = db.Column(db.DateTime, nullable=True)
    data_bjdl_updated_at = db.Column(db.DateTime, nullable=True)
    data_gjdl_updated_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<Project {self.project_name}>'


class ExpectedPrice(db.Model):
    __tablename__ = 'expected_prices'
    id = db.Column(db.Integer, primary_key=True)
    secondary_unit = db.Column(db.String(255), nullable=False)
    production_year = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Numeric(10, 2), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f'<ExpectedPrice {self.secondary_unit} {self.production_year}: {self.price}>'


class SystemSetting(db.Model):
    __tablename__ = 'system_settings'
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.String(255), nullable=False)
    description = db.Column(db.String(255))
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f'<SystemSetting {self.key}: {self.value}>'

class FAQ(db.Model):
    __tablename__ = 'faqs'
    id = db.Column(db.Integer, primary_key=True)
    question = db.Column(db.Text, nullable=False)
    answer = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    
    # 关联用户
    creator = db.relationship('User', backref='faqs')
    
    def __repr__(self):
        return f'<FAQ {self.id}: {self.question[:50]}...>'


class Customer(db.Model):
    __tablename__ = 'customers'
    customer_name = db.Column(db.String(200), primary_key=True, nullable=False)
    customer_type = db.Column(db.String(100), nullable=True)
    province = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f'<Customer {self.customer_name}>'
