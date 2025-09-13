# 文件: app/__init__.py
# 应用工厂

import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

# 1. 在文件顶部只创建扩展的实例，不要初始化
db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = 'web.login'

def create_app():
    app = Flask(__name__)
    # 从 config.py 加载配置
    app.config.from_object('config')

    # 2. 在工厂函数内部，将 app 和扩展绑定
    db.init_app(app)
    login_manager.init_app(app)

    # 在这里定义 user_loader，因为它依赖 login_manager
    @login_manager.user_loader
    def load_user(user_id):
        # 必须在这里导入 User 模型，避免顶层循环导入
        from .models import User
        return User.query.get(int(user_id))

    # --- 注册蓝图 ---
    from .routes.web import web as web_blueprint
    app.register_blueprint(web_blueprint)

    from .routes.api import api as api_blueprint
    app.register_blueprint(api_blueprint)

    from .routes.dashboard import dashboard_bp as dashboard_blueprint
    app.register_blueprint(dashboard_blueprint)

    # --- 命令行函数 ---
    @app.cli.command("init-db")
    def init_db_command():
        """创建数据库表并添加一个默认管理员."""
        with app.app_context():
            db.create_all()
            if User.query.filter_by(username='admin').first() is None:
                admin = User(username='admin', is_admin=True)
                admin.set_password('your_default_admin_password') # 请替换为您的密码
                db.session.add(admin)
                db.session.commit()
                print('数据库已初始化，并创建了管理员账户。')
            else:
                print('管理员账户已存在。')

    return app