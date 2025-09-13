# 文件: run.py (修改后)
from waitress import serve
# 从 app 包中导入 create_app 工厂函数
from app import create_app
# 导入我们修改后的调度器
from app.scheduler import DashboardScheduler

# 调用工厂函数创建 app 实例
app = create_app()

# --- 启动后台缓存调度器 ---
# 确保只在直接运行此文件时才启动调度器
# 这样在使用 "flask" 命令行工具时不会重复启动
if __name__ == '__main__':
    # 1. 创建调度器实例，传入app和刷新间隔
    scheduler = DashboardScheduler(app, interval_minutes=10)
    # 2. 启动调度器
    scheduler.start()

    # 3. 启动Web服务
    # 监听 0.0.0.0 的 8000 端口，使其可以被外部访问
    # 注意：在生产环境中，确保您的防火墙设置是安全的
    serve(app, host='0.0.0.0', port=8000, threads=8)