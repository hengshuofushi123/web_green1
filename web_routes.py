# 文件: web_routes.py
# 包含所有网页相关的路由，使用蓝图，便于后续更新网页内容和添加仪表盘

from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, abort, jsonify
from flask_login import login_user, logout_user, login_required, current_user
import pandas as pd
import io
import os
from datetime import datetime
from models import db, User, Project
from utils import generate_random_password, update_pwd_excel, project_to_dict, populate_project_from_form

web = Blueprint('web', __name__)

# --- 认证路由 ---
@web.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.overview'))
    users = User.query.order_by(User.username).all()
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for('dashboard.overview'))
        else:
            flash('无效的用户名或密码')
    return render_template('login.html', users=users)

@web.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('web.login'))

# --- 主页重定向 ---
@web.route('/')
@login_required
def index():
    return redirect(url_for('dashboard.overview'))

# --- 搜索和高级筛选 ---
@web.route('/projects')
@login_required
def projects():
    is_admin = current_user.is_admin
    secondary_units = db.session.query(Project.secondary_unit).distinct().order_by(Project.secondary_unit).all()
    provinces = db.session.query(Project.province).distinct().order_by(Project.province).all()
    regions = db.session.query(Project.region).distinct().order_by(Project.region).all()
    investment_scopes = db.session.query(Project.investment_scope).distinct().order_by(Project.investment_scope).all()
    project_natures = db.session.query(Project.project_nature).distinct().order_by(Project.project_nature).all()
    power_types = db.session.query(Project.power_type).distinct().order_by(Project.power_type).all()

    filter_options = {
        'secondary_units': [r[0] for r in secondary_units if r[0]],
        'provinces': [r[0] for r in provinces if r[0]],
        'regions': [r[0] for r in regions if r[0]],
        'investment_scopes': [r[0] for r in investment_scopes if r[0]],
        'project_natures': [r[0] for r in project_natures if r[0]],
        'power_types': [r[0] for r in power_types if r[0]],
    }

    active_filters = {
        'query': request.args.get('query', ''),
        'secondary_unit': request.args.get('secondary_unit', ''),
        'province': request.args.get('province', ''),
        'region': request.args.get('region', ''),
        'investment_scope': request.args.get('investment_scope', ''),
        'project_nature': request.args.get('project_nature', ''),
        'power_type': request.args.get('power_type', ''),
        'is_uhv_support': request.args.get('is_uhv_support', ''),
        'has_subsidy': request.args.get('has_subsidy', ''),
    }

    query_builder = Project.query

    if not is_admin:
        query_builder = query_builder.filter(Project.secondary_unit == current_user.username)

    if active_filters['query']:
        query_builder = query_builder.filter(Project.project_name.like(f"%{active_filters['query']}%"))

    if active_filters['secondary_unit'] and is_admin:
        query_builder = query_builder.filter_by(secondary_unit=active_filters['secondary_unit'])
    if active_filters['province']:
        query_builder = query_builder.filter_by(province=active_filters['province'])
    if active_filters['region']:
        query_builder = query_builder.filter_by(region=active_filters['region'])
    if active_filters['investment_scope']:
        query_builder = query_builder.filter_by(investment_scope=active_filters['investment_scope'])
    if active_filters['project_nature']:
        query_builder = query_builder.filter_by(project_nature=active_filters['project_nature'])
    if active_filters['power_type']:
        query_builder = query_builder.filter_by(power_type=active_filters['power_type'])

    if active_filters['is_uhv_support'] in ['1', '0']:
        query_builder = query_builder.filter_by(is_uhv_support=(active_filters['is_uhv_support'] == '1'))
    if active_filters['has_subsidy'] in ['1', '0']:
        query_builder = query_builder.filter_by(has_subsidy=(active_filters['has_subsidy'] == '1'))

    projects = query_builder.order_by(Project.id).all()

    return render_template('index.html',
                           projects=projects,
                           filter_options=filter_options,
                           active_filters=active_filters,
                           is_admin=is_admin)

# --- 导出项目为Excel ---
@web.route('/export', methods=['GET'])
@login_required
def export_projects():
    is_admin = current_user.is_admin
    active_filters = {
        'query': request.args.get('query', ''),
        'secondary_unit': request.args.get('secondary_unit', ''),
        'province': request.args.get('province', ''),
        'region': request.args.get('region', ''),
        'investment_scope': request.args.get('investment_scope', ''),
        'project_nature': request.args.get('project_nature', ''),
        'power_type': request.args.get('power_type', ''),
        'is_uhv_support': request.args.get('is_uhv_support', ''),
        'has_subsidy': request.args.get('has_subsidy', ''),
    }

    query_builder = Project.query

    if not is_admin:
        query_builder = query_builder.filter(Project.secondary_unit == current_user.username)

    if active_filters['query']:
        query_builder = query_builder.filter(Project.project_name.like(f"%{active_filters['query']}%"))
    if active_filters['secondary_unit'] and is_admin:
        query_builder = query_builder.filter_by(secondary_unit=active_filters['secondary_unit'])
    if active_filters['province']:
        query_builder = query_builder.filter_by(province=active_filters['province'])
    if active_filters['region']:
        query_builder = query_builder.filter_by(region=active_filters['region'])
    if active_filters['investment_scope']:
        query_builder = query_builder.filter_by(investment_scope=active_filters['investment_scope'])
    if active_filters['project_nature']:
        query_builder = query_builder.filter_by(project_nature=active_filters['project_nature'])
    if active_filters['power_type']:
        query_builder = query_builder.filter_by(power_type=active_filters['power_type'])
    if active_filters['is_uhv_support'] in ['1', '0']:
        query_builder = query_builder.filter_by(is_uhv_support=(active_filters['is_uhv_support'] == '1'))
    if active_filters['has_subsidy'] in ['1', '0']:
        query_builder = query_builder.filter_by(has_subsidy=(active_filters['has_subsidy'] == '1'))

    projects = query_builder.order_by(Project.id).all()

    data = []
    for p in projects:
        data.append({
            '项目ID': p.id, '项目名称': p.project_name, '二级单位': p.secondary_unit,
            '二级单位联系人': p.secondary_unit_contact, '项目所在省份': p.province,
            '项目所在区域': p.region, '公司名称': p.company_name, '项目投资口径': p.investment_scope,
            '项目性质': p.project_nature, '电源品种': p.power_type,
            '是否特高压配套电源': '是' if p.is_uhv_support else '否',
            '是否含补贴': '是' if p.has_subsidy else '否',
            '装机容量(万千瓦)': p.capacity_mw, '投产年份': p.production_year,
            '投产月份': p.production_month, '是否建档立卡': '是' if p.is_filed else '否',
            '是否完成北交注册': '是' if p.is_beijing_registered else '否',
            '是否完成广交注册': '是' if p.is_guangzhou_registered else '否',
            '核对及更新日期': p.last_updated_date.strftime('%Y-%m-%d') if p.last_updated_date else ''
        })

    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Projects')
    output.seek(0)

    return send_file(output, as_attachment=True, download_name='projects.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# --- 新增项目 ---
@web.route('/project/add', methods=['GET', 'POST'])
def add_project():
    is_admin = current_user.is_admin
    if request.method == 'POST':
        form_data = request.form.to_dict()
        if not form_data.get('project_name'):
            flash('项目名称是必填项！')
            return render_template('project_form.html', action='Add', project=form_data, is_admin=is_admin)

        secondary_unit = form_data.get('secondary_unit') if is_admin else current_user.username
        if not secondary_unit:
            flash('二级单位是必填项！')
            return render_template('project_form.html', action='Add', project=form_data, is_admin=is_admin)

        existing_project = Project.query.filter_by(project_name=form_data['project_name']).first()
        if existing_project:
            flash('项目名称已存在！')
            return render_template('project_form.html', action='Add', project=form_data, is_admin=is_admin)

        new_project = Project()
        form_data['secondary_unit'] = secondary_unit
        new_project = populate_project_from_form(new_project, form_data)

        db.session.add(new_project)
        db.session.commit()

        if User.query.filter_by(username=secondary_unit).first() is None:
            new_user = User(username=secondary_unit)
            pw = generate_random_password()
            new_user.set_password(pw)
            db.session.add(new_user)
            db.session.commit()
            update_pwd_excel(secondary_unit, pw)

        flash('项目已成功新增！')
        return redirect(url_for('web.projects'))

    return render_template('project_form.html', action='Add', project={}, is_admin=is_admin)

# --- 编辑项目 ---
@web.route('/project/edit/<int:project_id>', methods=['GET', 'POST'])
def edit_project(project_id):
    is_admin = current_user.is_admin
    project = Project.query.get_or_404(project_id)
    if not is_admin and project.secondary_unit != current_user.username:
        flash('您没有权限编辑此项目！')
        return redirect(url_for('web.projects'))

    if request.method == 'POST':
        form_data = request.form.to_dict()
        if not form_data.get('project_name') or not form_data.get('secondary_unit'):
            flash('项目名称和二级单位是必填项！')
            return render_template('project_form.html', action='Edit', project=form_data, is_admin=is_admin)

        old_secondary_unit = project.secondary_unit
        secondary_unit = form_data.get('secondary_unit') if is_admin else current_user.username
        form_data['secondary_unit'] = secondary_unit
        project = populate_project_from_form(project, form_data)
        db.session.commit()

        if secondary_unit != old_secondary_unit:
            if User.query.filter_by(username=secondary_unit).first() is None:
                new_user = User(username=secondary_unit)
                pw = generate_random_password()
                new_user.set_password(pw)
                db.session.add(new_user)
                db.session.commit()
                update_pwd_excel(secondary_unit, pw)

        flash('项目已成功更新！')
        return redirect(url_for('web.projects'))

    project_dict = project_to_dict(project)
    return render_template('project_form.html', project=project_dict, action='Edit', is_admin=is_admin)

# --- 删除项目 ---
@web.route('/project/delete/<int:project_id>', methods=['POST'])
def delete_project(project_id):
    project = Project.query.get_or_404(project_id)
    if not current_user.is_admin and project.secondary_unit != current_user.username:
        flash('您没有权限删除此项目！')
        return redirect(url_for('web.projects'))
    db.session.delete(project)
    db.session.commit()
    flash('项目已成功删除！')
    return redirect(url_for('web.projects'))

# --- Excel 导入 ---
@web.route('/import', methods=['POST'])
@login_required
def import_excel():
    if not current_user.is_admin:
        flash('只有管理员可以导入Excel文件！')
        return redirect(url_for('web.projects'))

    file = request.files.get('file')
    if not file or not file.filename.endswith(('.xlsx', '.xls')):
        flash('请上传有效的Excel文件 (.xlsx, .xls)')
        return redirect(url_for('web.projects'))

    try:
        df = pd.read_excel(file, sheet_name='Sheet1', dtype=str)
        df.columns = df.columns.str.strip()

        column_map = {
            '核对及更新日期': 'last_updated_date', '二级单位': 'secondary_unit',
            '二级单位联系人': 'secondary_unit_contact', '项目所在省份': 'province',
            '项目所在区域': 'region', '项目名称': 'project_name',
            '公司名称': 'company_name', '项目投资口径': 'investment_scope',
            '项目性质': 'project_nature', '电源品种': 'power_type',
            '是否特高压配套电源': 'is_uhv_support', '是否含补贴': 'has_subsidy',
            '装机容量\n（万千瓦）': 'capacity_mw', '装机容量（万千瓦）': 'capacity_mw',
            '（预计）投产年份': 'production_year', '（预计）投产月份': 'production_month',
            '是否建档立卡': 'is_filed', '是否完成北交注册': 'is_beijing_registered',
            '是否完成广交注册': 'is_guangzhou_registered'
        }

        mappable_columns = {k: v for k, v in column_map.items() if k in df.columns}
        df.rename(columns=mappable_columns, inplace=True)

        if 'project_name' not in df.columns or 'secondary_unit' not in df.columns:
            flash('Excel文件中必须包含 "项目名称" 和 "二级单位" 列！')
            return redirect(url_for('web.projects'))

        updated_count = 0
        added_count = 0
        skipped_rows = []
        new_units = set()

        for index, row in df.iterrows():
            def get_value(r, col_name):
                val = r.get(col_name)
                if pd.isna(val) or str(val).strip() == '':
                    return None
                return str(val).strip()

            project_name = get_value(row, 'project_name')
            secondary_unit = get_value(row, 'secondary_unit')

            if not project_name or not secondary_unit:
                skipped_rows.append(f"第 {index + 2} 行: 项目名称或二级单位为空，已跳过。")
                continue

            data = {}
            try:
                date_val = get_value(row, 'last_updated_date')
                data['last_updated_date'] = pd.to_datetime(date_val).date() if date_val else None
            except Exception:
                data['last_updated_date'] = None
            try:
                year_val = get_value(row, 'production_year')
                data['production_year'] = int(float(year_val)) if year_val else None
            except (ValueError, TypeError):
                data['production_year'] = None
            try:
                month_val = get_value(row, 'production_month')
                data['production_month'] = int(float(month_val)) if month_val else None
            except (ValueError, TypeError):
                data['production_month'] = None
            try:
                capacity_val = get_value(row, 'capacity_mw')
                data['capacity_mw'] = float(capacity_val) if capacity_val else None
            except (ValueError, TypeError):
                data['capacity_mw'] = None

            def to_bool(value):
                return value in ['是', 'True', '1', 'true']

            data['is_uhv_support'] = to_bool(get_value(row, 'is_uhv_support'))
            data['has_subsidy'] = to_bool(get_value(row, 'has_subsidy'))
            data['is_filed'] = to_bool(get_value(row, 'is_filed'))
            data['is_beijing_registered'] = to_bool(get_value(row, 'is_beijing_registered'))
            data['is_guangzhou_registered'] = to_bool(get_value(row, 'is_guangzhou_registered'))

            data['secondary_unit_contact'] = get_value(row, 'secondary_unit_contact')
            data['province'] = get_value(row, 'province')
            data['region'] = get_value(row, 'region')
            data['company_name'] = get_value(row, 'company_name')
            data['investment_scope'] = get_value(row, 'investment_scope')
            data['project_nature'] = get_value(row, 'project_nature')
            data['power_type'] = get_value(row, 'power_type')

            try:
                project = Project.query.filter_by(project_name=project_name).first()
                if project:
                    updated_count += 1
                    project.secondary_unit = secondary_unit
                    for key, value in data.items():
                        if key in row.index and row.get(key) is not None:
                            setattr(project, key, value)
                    if project.secondary_unit != secondary_unit:
                        new_units.add(secondary_unit)
                else:
                    added_count += 1
                    data['project_name'] = project_name
                    data['secondary_unit'] = secondary_unit
                    new_project = Project(**data)
                    db.session.add(new_project)
                    new_units.add(secondary_unit)
            except Exception as db_error:
                db.session.rollback()
                skipped_rows.append(f"第 {index + 2} 行 ({project_name}): 数据库操作失败 - {db_error}")

        db.session.commit()

        for unit in new_units:
            if User.query.filter_by(username=unit).first() is None:
                new_user = User(username=unit)
                pw = generate_random_password()
                new_user.set_password(pw)
                db.session.add(new_user)
                db.session.commit()
                update_pwd_excel(unit, pw)

        success_message = f'导入完成！成功处理 {updated_count + added_count} 条记录 (更新: {updated_count}, 新增: {added_count})。'
        if skipped_rows:
            flash(success_message + f"共跳过 {len(skipped_rows)} 行。", 'warning')
            for row_info in skipped_rows[:5]:
                flash(f"跳过详情: {row_info}", "danger")
        else:
            flash(success_message, 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'导入失败，出现严重错误：{e}', 'danger')

    return redirect(url_for('web.projects'))

# --- 管理用户 ---
@web.route('/manage_users')
@login_required
def manage_users():
    if not current_user.is_admin:
        flash('只有管理员可以管理用户！')
        return redirect(url_for('web.projects'))
    users = User.query.filter(User.username != 'admin').order_by(User.username).all()
    return render_template('manage_users.html', users=users)

# --- 重置密码 ---
@web.route('/reset_password/<username>', methods=['POST'])
@login_required
def reset_password(username):
    if not current_user.is_admin:
        abort(403)
    user = User.query.filter_by(username=username).first_or_404()
    if user.username == 'admin':
        abort(403)
    new_pw = generate_random_password()
    user.set_password(new_pw)
    db.session.commit()
    update_pwd_excel(username, new_pw)
    flash(f'{username} 的密码已重置为: {new_pw}')
    return redirect(url_for('web.manage_users'))

# --- 下载密码Excel ---
@web.route('/download_pwd')
@login_required
def download_pwd():
    if not current_user.is_admin:
        abort(403)
    return send_file('pwd.xlsx', as_attachment=True)

# --- 导出全部数据 ---
@web.route('/export_all_data')
@login_required
def export_all_data():
    if not current_user.is_admin:
        abort(403)
    
    try:
        # 定义数据表和对应的工作表名称
        tables_config = {
            '北交': 'beijing_power_exchange_trades',
            '广交': 'guangzhou_power_exchange_trades', 
            '绿证平台双边线下': 'gzpt_bilateral_offline_trades',
            '绿证平台双边线上': 'gzpt_bilateral_online_trades',
            '绿证平台单向挂牌': 'gzpt_unilateral_listings',
            '能源局绿证核发': 'nyj_green_certificate_ledger',
            '能源局绿证交易': 'nyj_transaction_records',
            '项目清单': 'projects'
        }
        
        # 创建Excel文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, table_name in tables_config.items():
                try:
                    # 查询数据表的所有数据
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(query, db.engine)
                    
                    # 写入Excel工作表
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    # 如果某个表查询失败，创建一个错误信息的工作表
                    error_df = pd.DataFrame({'错误信息': [f'无法读取表 {table_name}: {str(e)}']})
                    error_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        output.seek(0)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'绿证平台数据{timestamp}.xlsx'
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        flash(f'导出数据时发生错误: {str(e)}')
        return redirect(url_for('dashboard.overview'))


# --- 独立页面路由（不需要登录） ---
@web.route('/contact-lookup-new')
def contact_lookup_standalone():
    """独立的联系人查询页面，不需要登录权限"""
    return render_template('contact_lookup_standalone.html')


@web.route('/api/contact-lookup-standalone-new', methods=['POST'])
def api_contact_lookup_standalone():
    """独立页面的联系人查询API，不需要登录权限"""
    try:
        # 检查页面是否已失效（8月31日0:00之后）
        from datetime import datetime
        current_time = datetime.now()
        expiry_time = datetime(2025, 8, 31, 0, 0, 0)
        
        if current_time >= expiry_time:
            return jsonify({'error': '页面已失效，请联系管理员！'}), 403
        
        data = request.get_json()
        contact_name = data.get('contact_name', '').strip()
        
        if not contact_name:
            return jsonify({'error': '请输入联系人姓名！'}), 400
        
        # 从projects表中查找匹配的联系人
        projects = Project.query.filter(
            Project.secondary_unit_contact.like(f'%{contact_name}%')
        ).all()
        
        if not projects:
            return jsonify({'error': f'未找到联系人姓名包含"{contact_name}"的记录！'}), 404
        
        results = []
        
        # 读取密码文件
        pwd_file = 'pwd.xlsx'
        pwd_df = None
        if os.path.exists(pwd_file):
            pwd_df = pd.read_excel(pwd_file)
        
        # 获取唯一的二级单位
        unique_units = {}
        for project in projects:
            unit = project.secondary_unit
            contact = project.secondary_unit_contact
            if unit not in unique_units:
                unique_units[unit] = contact
        
        for unit, contact in unique_units.items():
            # 查找用户并重置密码
            user = User.query.filter_by(username=unit).first()
            if user:
                # 生成新密码并更新
                new_password = generate_random_password()
                user.set_password(new_password)
                db.session.commit()
                
                # 更新Excel文件
                update_pwd_excel(unit, new_password)
                
                password = new_password
            else:
                password = '用户不存在'
            
            results.append({
                'secondary_unit': unit,
                'contact_name': contact,
                'password': password
            })
        
        return jsonify({'results': results})
        
    except Exception as e:
        return jsonify({'error': f'查询时发生错误: {str(e)}'}), 500
