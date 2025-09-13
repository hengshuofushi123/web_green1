# -*- coding: utf-8 -*-
"""
定时任务模块
用于定期执行dashboard数据计算等后台任务
"""

import threading
import time
from datetime import datetime
# 确保从.dashboard_cache导入，如果scheduler.py在app目录下
from .dashboard_cache import calculate_dashboard_data

class DashboardScheduler:
    """Dashboard数据定时计算器"""
    
    def __init__(self, app, interval_minutes=10):
        self.app = app
        self.interval_minutes = interval_minutes
        self.interval_seconds = interval_minutes * 60
        self.running = False
        self.thread = None
        
    def start(self):
        """启动定时任务"""
        if self.running:
            print("定时任务已在运行中")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        print(f"Dashboard定时任务已启动，每{self.interval_minutes}分钟执行一次")
        
        # 立即在后台执行一次计算，确保应用启动后就有缓存
        print(f"[{datetime.now()}] 应用启动，立即执行第一次dashboard数据计算...")
        threading.Thread(target=self._calculate_with_log, daemon=True).start()
        
    def stop(self):
        """停止定时任务"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        print("Dashboard定时任务已停止")
        
    def _run_scheduler(self):
        """定时任务主循环"""
        while self.running:
            try:
                # 先等待一个周期，避免与启动时的那次计算任务距离太近
                time.sleep(self.interval_seconds)
                if self.running:  # 再次检查，避免在sleep期间被停止
                    self._calculate_with_log()
            except Exception as e:
                print(f"[{datetime.now()}] 定时任务主循环出错: {str(e)}")
                
    def _calculate_with_log(self):
        """带日志的数据计算（在应用上下文中执行）"""
        # 这是关键！使用with self.app.app_context()来确保数据库等操作可以正常工作
        with self.app.app_context():
            try:
                print(f"[{datetime.now()}] 开始周期性计算dashboard数据...")
                calculate_dashboard_data()
                print(f"[{datetime.now()}] 周期性计算dashboard数据完成")
            except Exception as e:
                print(f"[{datetime.now()}] 周期性计算dashboard数据出错: {str(e)}")