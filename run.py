from waitress import serve
from app import app

# 监听本地环回地址的 8000 端口
serve(app, host='127.0.0.1', port=8000,threads=4)