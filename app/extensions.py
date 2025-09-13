# -*- coding: utf-8 -*-
"""
扩展实例化文件
将扩展实例与app创建分离，避免循环导入问题
"""
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

# 只在这里实例化所有需要在模块加载时就被引用的扩展
db = SQLAlchemy()
login_manager = LoginManager()