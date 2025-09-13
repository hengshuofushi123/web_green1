# -*- coding: utf-8 -*-
"""
定时任务模块
用于定期执行dashboard数据计算等后台任务
"""

import threading
import time
from datetime import datetime
from dashboard_cache import calculate_dashboard_data

class DashboardScheduler:
    """Dashboard数据定时计算器"""
    
    def __init__(self, interval_minutes=10):
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
        
        # 立即执行一次计算
        threading.Thread(target=self._calculate_with_log, daemon=True).start()
        
    def stop(self):
        """停止定时任务"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print("Dashboard定时任务已停止")
        
    def _run_scheduler(self):
        """定时任务主循环"""
        while self.running:
            try:
                time.sleep(self.interval_seconds)
                if self.running:  # 再次检查，避免在sleep期间被停止
                    self._calculate_with_log()
            except Exception as e:
                print(f"[{datetime.now()}] 定时任务执行出错: {str(e)}")
                
    def _calculate_with_log(self):
        """带日志的数据计算"""
        try:
            print(f"[{datetime.now()}] 开始定时计算dashboard数据...")
            calculate_dashboard_data()
            print(f"[{datetime.now()}] 定时计算dashboard数据完成")
        except Exception as e:
            print(f"[{datetime.now()}] 定时计算dashboard数据出错: {str(e)}")
            
    def is_running(self):
        """检查定时任务是否在运行"""
        return self.running
        
    def get_status(self):
        """获取定时任务状态"""
        return {
            'running': self.running,
            'interval_minutes': self.interval_minutes,
            'thread_alive': self.thread.is_alive() if self.thread else False
        }

# 全局调度器实例
dashboard_scheduler = DashboardScheduler(interval_minutes=10)

def start_dashboard_scheduler():
    """启动dashboard定时任务"""
    dashboard_scheduler.start()
    
def stop_dashboard_scheduler():
    """停止dashboard定时任务"""
    dashboard_scheduler.stop()
    
def get_scheduler_status():
    """获取调度器状态"""
    return dashboard_scheduler.get_status()