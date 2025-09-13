# 文件: api_routes.py
# 包含所有API路由，使用蓝图，包括GUI客户端登录、数据提交和外部API

from flask import Blueprint, jsonify, request, current_app, send_from_directory
from flask_login import current_user
from ..models import db, User, Project
import json
import os
import logging
from datetime import datetime
from config import API_ACCESS_TOKEN
from ..data_processors import update_derived_tables

# 创建日志记录器
logger = logging.getLogger(__name__)

api = Blueprint('api', __name__, url_prefix='/api')

# --- 新增的外部API接口 ---
@api.route('/projects', methods=['GET'])
def get_projects_api():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization header is missing or invalid. Expected format: Bearer <token>'}), 401

    token = auth_header.split(' ')[1]
    if token != API_ACCESS_TOKEN:
        return jsonify({'error': 'Invalid or expired token.'}), 403

    try:
        projects = Project.query.with_entities(Project.project_name, Project.secondary_unit).order_by(Project.id).all()
        project_list = [{'project_name': p.project_name, 'secondary_unit': p.secondary_unit} for p in projects]
        return jsonify(project_list)
    except Exception as e:
        return jsonify({'error': 'An internal error occurred.', 'details': str(e)}), 500

# --- 新增的GUI后端API ---
@api.route('/secondary_units', methods=['GET'])
def get_secondary_units():
    """获取所有二级单位的列表用于登录下拉框"""
    try:
        units = db.session.query(User.username).filter(User.username != 'admin').distinct().order_by(
            User.username).all()
        unit_list = [u[0] for u in units]
        return jsonify({'units': unit_list})
    except Exception as e:
        logger.error(f"Failed to fetch units: {e}")
        return jsonify({'error': 'Failed to fetch units', 'details': str(e)}), 500

@api.route('/login', methods=['POST'])
def api_login():
    """处理GUI的登录请求"""
    data = request.get_json()
    if not data or 'username' not in data or 'password' not in data:
        return jsonify({'success': False, 'message': '请求缺少用户名或密码'}), 400

    username = data['username']
    password = data['password']

    user = User.query.filter_by(username=username).first()
    if user and user.check_password(password):
        projects = Project.query.filter_by(secondary_unit=username).with_entities(Project.project_name).order_by(
            Project.project_name).all()
        project_list = [p.project_name for p in projects]
        return jsonify({'success': True, 'message': '登录成功', 'projects': project_list})
    else:
        return jsonify({'success': False, 'message': '无效的用户名或密码'}), 401

@api.route('/submit_data', methods=['POST'])
def api_submit_data():
    """【已重构】接收GUI提交的数据，更新主表、时间戳和关联表"""
    data = request.get_json()
    if not data or 'project_name' not in data or 'source' not in data or 'data' not in data:
        return jsonify({'success': False, 'message': '无效的数据负载'}), 400

    project_name = data['project_name']
    source = data['source']
    scraped_data = data['data']
    
    # 打印接收到的数据结构，用于调试
    print(f"接收到数据源: {source}, 项目: {project_name}")
    print(f"数据结构: {type(scraped_data)}")
    if isinstance(scraped_data, dict):
        print(f"数据键: {scraped_data.keys()}")
    elif isinstance(scraped_data, list):
        print(f"数据长度: {len(scraped_data)}")
    else:
        print(f"数据类型: {type(scraped_data)}")


    # 1. 保存到本地文件 (可选，但保留)
    try:
        safe_project_name = "".join(c for c in project_name if c.isalnum() or c in ('_', '-')).rstrip()
        safe_source = "".join(c for c in source if c.isalnum() or c in ('_', '-')).rstrip()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"apidata_{safe_project_name}_{safe_source}_{timestamp}.txt"

        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "received_data")
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        filepath = os.path.join(data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"保存文件失败，项目: {project_name}: {e}")

    # 2. 在一个事务中完成数据库所有操作
    try:
        project = Project.query.filter_by(project_name=project_name).first()
        if not project:
            return jsonify({'success': False, 'message': f'项目 "{project_name}" 在数据库中未找到'}), 404

        data_json_str = json.dumps(scraped_data, ensure_ascii=False)
        current_time = datetime.now()

        source_to_column_map = {
            "能源局网站": ("data_nyj", "data_nyj_updated_at"),
            "绿证交易平台": ("data_lzy", "data_lzy_updated_at"),
            "北京电力交易中心": ("data_bjdl", "data_bjdl_updated_at"),
            "广州电力交易中心": ("data_gjdl", "data_gjdl_updated_at")
        }

        column_info = source_to_column_map.get(source)
        if not column_info:
            return jsonify({'success': False, 'message': f'未知的数据源: "{source}"'}), 400

        data_col, time_col = column_info

        setattr(project, data_col, data_json_str)
        setattr(project, time_col, current_time)

        update_derived_tables(project.id, source)

        db.session.commit()

        return jsonify({'success': True, 'message': '数据已成功提交并保存到所有相关表'})

    except Exception as e:
        db.session.rollback()
        logger.error(f"数据库保存失败，项目: {project_name}: {e}")
        return jsonify({'success': False, 'message': f'数据库错误，操作已回滚: {str(e)}'}), 500


@api.route('/get_module', methods=['GET'])
def get_module():
    """
    核心功能：提供 m1.py 模块文件给启动器下载。
    Core Function: Serves the m1.py module file for the launcher to download.
    """
    module_directory = os.path.dirname(os.path.abspath(__file__))
    module_filename = "m1.py"
    print(f"请求获取模块文件: {os.path.join(module_directory, module_filename)}")
    
    try:
        # 使用 send_from_directory 可以安全地发送文件
        # Using send_from_directory is a secure way to send files
        return send_from_directory(directory=module_directory, path=module_filename, as_attachment=True, mimetype='text/plain')
        
    except FileNotFoundError:
        print(f"错误: 在目录 {module_directory} 中未找到模块文件 {module_filename}")
        return jsonify({"success": False, "message": "服务器上未找到模块文件。"}), 404


# --- 添加兼容旧版GUI客户端的路由 ---
@api.route('/submit', methods=['POST'])
def api_submit_compat():
    """兼容旧版GUI客户端的路由，重定向到submit_data"""
    print("收到 /api/submit 请求，重定向到 /api/submit_data")
    
    # 获取请求数据
    request_data = request.json
    if not request_data:
        return jsonify({'success': False, 'message': '请求数据为空'}), 400
        
    project_name = request_data.get('project_name')
    source = request_data.get('source')
    scraped_data = request_data.get('data')
    
    if not all([project_name, source, scraped_data]):
        return jsonify({'success': False, 'message': '缺少必要参数: project_name, source, data'}), 400
    
    print(f"处理项目: {project_name}, 数据源: {source}")
    
    # 1. 保存原始数据到文件
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{project_name}_{source}_{timestamp}.json"
        data_dir = os.path.join(current_app.config['DATA_DIR'], 'raw_data')
        
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        filepath = os.path.join(data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"保存文件失败，项目: {project_name}: {e}")

    # 2. 在一个事务中完成数据库所有操作
    try:
        project = Project.query.filter_by(project_name=project_name).first()
        if not project:
            return jsonify({'success': False, 'message': f'项目 "{project_name}" 在数据库中未找到'}), 404

        data_json_str = json.dumps(scraped_data, ensure_ascii=False)
        current_time = datetime.now()

        source_to_column_map = {
            "能源局网站": ("data_nyj", "data_nyj_updated_at"),
            "绿证交易平台": ("data_lzy", "data_lzy_updated_at"),
            "北京电力交易中心": ("data_bjdl", "data_bjdl_updated_at"),
            "广州电力交易中心": ("data_gjdl", "data_gjdl_updated_at")
        }

        column_info = source_to_column_map.get(source)
        if not column_info:
            return jsonify({'success': False, 'message': f'未知的数据源: "{source}"'}), 400

        data_col, time_col = column_info

        setattr(project, data_col, data_json_str)
        setattr(project, time_col, current_time)

        update_derived_tables(project.id, source)

        db.session.commit()

        return jsonify({'success': True, 'message': '数据已成功提交并保存到所有相关表'})

    except Exception as e:
        db.session.rollback()
        logger.error(f"数据库保存失败，项目: {project_name}: {e}")
        return jsonify({'success': False, 'message': f'数据库错误，操作已回滚: {str(e)}'}), 500

# --- 【新增】供GUI查询项目状态的API ---
@api.route('/status/<string:secondary_unit>', methods=['GET'])
def get_projects_by_unit(secondary_unit):
    """根据二级单位名称，查询其所有项目的列表和各数据源的最后更新时间"""
    try:
        projects = Project.query.filter_by(secondary_unit=secondary_unit).order_by(Project.project_name).all()

        if not projects:
            return jsonify([])

        result = []
        for p in projects:
            result.append({
                'project_name': p.project_name,
                'data_nyj_updated_at': p.data_nyj_updated_at.isoformat() if p.data_nyj_updated_at else None,
                'data_lzy_updated_at': p.data_lzy_updated_at.isoformat() if p.data_lzy_updated_at else None,
                'data_bjdl_updated_at': p.data_bjdl_updated_at.isoformat() if p.data_bjdl_updated_at else None,
                'data_gjdl_updated_at': p.data_gjdl_updated_at.isoformat() if p.data_gjdl_updated_at else None,
            })

        return jsonify(result)

    except Exception as e:
        logger.error(f"查询项目状态失败，单位: {secondary_unit}: {e}")
        return jsonify({'error': '查询失败', 'details': str(e)}), 500