# 文件: app.py
# 主应用文件，负责初始化和运行

import os
import pandas as pd
from flask import Flask, url_for as flask_url_for
from flask_login import LoginManager
from models import db, User, Project
from config import SECRET_KEY, SQLALCHEMY_DATABASE_URI, SQLALCHEMY_TRACK_MODIFICATIONS, SQLALCHEMY_ENGINE_OPTIONS
from web_routes import web
from api_routes import api
from utils import generate_random_password, update_pwd_excel
from dashboard_routes import dashboard_bp

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = SQLALCHEMY_TRACK_MODIFICATIONS
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = SQLALCHEMY_ENGINE_OPTIONS

# --- 初始化 ---
db.init_app(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'web.login'  # 如果未登录，重定向到登录页面

# --- 用户加载回调 ---
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# --- 自定义 url_for 用于 Jinja 模板，自动添加 'web.' 前缀以适配原有模板 ---
def custom_url_for(endpoint, **values):
    # 不对全局静态资源端点进行前缀处理，避免将 'static' 错误地转换为 'web.static'
    if '.' not in endpoint and endpoint != 'static':
        endpoint = 'web.' + endpoint
    return flask_url_for(endpoint, **values)

app.jinja_env.globals['url_for'] = custom_url_for

# --- 命令行函数：创建数据库和管理员用户 ---
@app.cli.command("init-db")
def init_db_command():
    """创建数据库表并添加一个默认管理员."""
    with app.app_context():
        db.create_all()
        if User.query.filter_by(username='admin').first() is None:
            admin = User(username='admin', is_admin=True)
            admin.set_password('    ')
            db.session.add(admin)
            db.session.commit()
            update_pwd_excel('admin', 'biNj5GIVwZ0PMrmX')
            print('数据库已初始化，并创建了管理员账户(admin/biNj5GIVwZ0PMrmX)。')
        else:
            print('管理员账户已存在。')

# 注册蓝图
app.register_blueprint(web)
app.register_blueprint(api)
app.register_blueprint(dashboard_bp)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0',port=8000)