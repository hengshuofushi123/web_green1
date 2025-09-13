# 文件: run.py (修改后)
from waitress import serve
# 从 app 包中导入 create_app 工厂函数
from app import create_app

# 调用工厂函数创建 app 实例
app = create_app()

# 监听 0.0.0.0 的 8000 端口，使其可以被外部访问
# 注意：在生产环境中，确保您的防火墙设置是安全的
serve(app, host='0.0.0.0', port=8000, threads=8) # 增加了线程数以提高性能