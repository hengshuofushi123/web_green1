#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份脚本：将当前目录下的所有文件和文件夹（排除指定目录）
复制到 D:\code\backup\{时间戳} 目录中
"""

import os
import shutil
import time
from datetime import datetime
import sys


def create_backup():
    """执行备份操作"""
    # 获取当前工作目录
    source_dir = os.getcwd()
    
    # 创建时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"D:\\code\\backup\\{timestamp}"
    
    # 需要排除的目录和文件
    exclude_dirs = {'venv', '.idea', '__pycache__'}
    exclude_files = {'backup.py'}
    
    try:
        # 创建备份目录
        os.makedirs(backup_dir, exist_ok=True)
        print(f"正在创建备份目录: {backup_dir}")
        
        # 遍历源目录
        for item in os.listdir(source_dir):
            source_path = os.path.join(source_dir, item)
            dest_path = os.path.join(backup_dir, item)
            
            # 跳过排除的目录和文件
            if os.path.isdir(source_path) and item in exclude_dirs:
                print(f"跳过目录: {item}")
                continue
                
            if os.path.isfile(source_path) and item in exclude_files:
                print(f"跳过文件: {item}")
                continue
            
            # 执行复制
            if os.path.isdir(source_path):
                print(f"正在复制目录: {item}")
                shutil.copytree(source_path, dest_path)
            else:
                print(f"正在复制文件: {item}")
                shutil.copy2(source_path, dest_path)
        
        print(f"\n备份完成！")
        print(f"备份路径: {backup_dir}")
        return True
        
    except Exception as e:
        print(f"备份失败: {str(e)}")
        return False


if __name__ == "__main__":
    print("开始执行备份操作...")
    success = create_backup()
    if success:
        print("按任意键退出...")
        input()
    else:
        print("备份过程中出现错误，按任意键退出...")
        input()
        sys.exit(1)