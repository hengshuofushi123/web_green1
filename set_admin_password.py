# 文件: set_admin_password.py (修改后，允许粘贴密码)
import sys
from app import create_app, db
from app.models import User

# --- 使用 input() 允许用户粘贴密码 ---
# 警告：使用此方法时，密码将在屏幕上可见
print("--- 管理员密码重置工具 ---")
print("警告：接下来输入的密码将在终端上可见，请在安全的环境下操作。")

# 接收新的密码
new_password = input("请输入或粘贴新的管理员密码: ")

# 确认密码
password_confirm = input("请再次输入或粘贴以确认密码: ")

if new_password != password_confirm:
    print("\n错误：两次输入的密码不一致，操作已取消。")
    sys.exit(1) # 使用 sys.exit 退出脚本

if not new_password:
    print("\n错误：密码不能为空，操作已取消。")
    sys.exit(1)

# 创建 app 实例并进入应用上下文
app = create_app()
with app.app_context():
    # 查找 admin 用户
    admin_user = User.query.filter_by(username='admin').first()
    
    if admin_user:
        # 如果用户存在，设置新密码
        admin_user.set_password(new_password)
        print(f"\n成功：用户 '{admin_user.username}' 的密码已更新。")
    else:
        # 如果用户不存在，创建一个新的 admin 用户
        admin_user = User(username='admin', is_admin=True)
        admin_user.set_password(new_password)
        db.session.add(admin_user)
        print(f"\n注意：用户 'admin' 不存在，已为您创建新用户并设置密码。")
        
    # 提交到数据库
    db.session.commit()
    print("操作成功完成。")