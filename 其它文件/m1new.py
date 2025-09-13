import sys
import os
import time
import datetime
import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import numpy as np
from json import JSONEncoder
from collections import defaultdict

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTreeWidget, QTreeWidgetItem, QTabWidget, QTableView,
    QTextEdit, QHeaderView, QMessageBox, QDialog, QLabel, QLineEdit,
    QComboBox, QScrollArea, QFormLayout, QInputDialog, QFileDialog, QCheckBox
)
from PySide6.QtCore import (
    QAbstractTableModel, Qt, QThread, Signal, QObject, QTimer
)
from PySide6.QtGui import QTextCursor

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from urllib.parse import unquote
import base64

# --- 常量定义 (Constants) ---
CHROME_PATH = r"Chrome\chrome.exe"
CHROMEDRIVER_PATH = r"Chrome\chromedriver.exe"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "download")
# API_BASE_URL = "http://127.0.0.1:5000" # 后端API地址 (Backend API Address)
API_BASE_URL = "http://john7.xyz:33651"  # 后端API地址 (Backend API Address)


def decode(encrypted_base64):
    """
    使用RSA私钥解密JSEncrypt加密的Base64字符串。
    Decrypts a JSEncrypt-encrypted Base64 string using an RSA private key.
    """
    # 私钥 (值为了隐私已用XXXX替代)
    # Private key (value replaced with XXXX for privacy)
    private_key_pem = """-----BEGIN PRIVATE KEY-----
    MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAOwc1F88UkFR8FIP2yJdprxrSUlOMAA+KBDmn6piwBH+mNpp0Ows7yfhkdNcTdr3TDXUjaUTQQ71m0pjctnwb5+Fh7U9Ay7L9ZWMwyRtlqCLARJdPtzQeh1lUfFnOwF6BC85r+637kab4FTgZSHt9l1qiKw0JlEIzZzHG93gcOZnAgMBAAECgYAlwiyCOGLNGF3uuaNpGQHvktaq9up9N3Nv1HnHJTijCAyIrTBgfIUYYx3PZ6z5rd+NojquoegfDM7zM/kreiRXVzt2Wa3vZtZ2Igp4SWHqjdvkJOSMO5DCddKVG6SrxTPaB2hC0k/WM7scofh3eGXCVx+Mamqeupvw3GSO8Sr9WQJBAPtjFO5YE0gOQBl2hrz3HppdGdZSVD+eZZnfw12jeGHEWUKDCoHQrg2Zs4UoFVsaKYqvr3SpRh9CvQaFoJhCqekCQQDwcf6aBD/UnI2nwJWI/6auw5pR7Sc8hs+7AY1CCMpNyfLhRSQlPtC8EUx8MwkoRTSfTcTAZC9vrOmvhu2ZCIvPAkAFQUr8uIaeqP2aCqpCZQAUxgF2Q35TXiJNlynkWTh5ArvC8i5UDGK3EhF4pR/dKazYo1eNnsRCfwiojD6RMEORAkBsLBzK1ZaR5EymZ7HejIVEoqNOsE6yoEPccfpG9wVssaofRqfYScZGldG/HobEIz5lXOtjUq80oqoPWbiS3JFpAkAnvxz1fp/CvW5WmdCcbBEtQkSrN3l3G+qkaO5aCYd8Fz6dID6AUcYOMlNuID08qoUq69A7wBneQPUECO32YB6H
    -----END PRIVATE KEY-----"""

    try:
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None,
            backend=default_backend()
        )
        encrypted_data = base64.b64decode(encrypted_base64)
        chunk_size = 128
        chunks = [encrypted_data[i:i + chunk_size] for i in range(0, len(encrypted_data), chunk_size)]
        decrypted_bytes = b''
        for chunk in chunks:
            try:
                dec = private_key.decrypt(chunk, padding.PKCS1v15())
                decrypted_bytes += dec
            except ValueError:
                dec = private_key.decrypt(
                    chunk,
                    padding.OAEP(
                        mgf=padding.MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )
                decrypted_bytes += dec
        decrypted_str = unquote(decrypted_bytes.decode('utf-8'))
        return json.loads(decrypted_str)
    except Exception as e:
        print(f"解密过程中出错: {e}")


class CustomJSONEncoder(JSONEncoder):
    """自定义JSON编码器，处理无穷大和NaN值。"""

    def default(self, obj):
        if isinstance(obj, float) and (obj == float('inf') or obj == -float('inf') or obj != obj):
            return None
        return super().default(obj)


# --- 登录对话框 (Login Dialog) ---
class LoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("登录")
        self.setMinimumWidth(350)
        self.projects = []
        self.unit_name = ""

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("二级单位:"))
        self.unit_combo = QComboBox()
        self.unit_combo.setMinimumHeight(30)
        layout.addWidget(self.unit_combo)

        layout.addWidget(QLabel("密码:"))
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setMinimumHeight(30)
        layout.addWidget(self.password_input)

        button_layout = QHBoxLayout()
        self.ok_button = QPushButton("登录")
        self.cancel_button = QPushButton("取消")
        self.ok_button.setMinimumHeight(35)
        self.cancel_button.setMinimumHeight(35)
        button_layout.addStretch()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)

        self.ok_button.clicked.connect(self.handle_login)
        self.cancel_button.clicked.connect(self.reject)
        self.password_input.returnPressed.connect(self.handle_login)

        self._load_units()

    def _load_units(self):
        try:
            response = requests.get(f"{API_BASE_URL}/api/secondary_units")
            response.raise_for_status()
            data = response.json()
            if 'units' in data and data['units']:
                self.unit_combo.addItems(data['units'])
            else:
                QMessageBox.warning(self, "错误", "无法从服务器加载二级单位列表。")
        except requests.exceptions.RequestException as e:
            QMessageBox.critical(self, "网络错误", f"无法连接到服务器: {e}")
            QTimer.singleShot(100, self.reject)

    def handle_login(self):
        self.unit_name = self.unit_combo.currentText()
        password = self.password_input.text()

        if not self.unit_name or not password:
            QMessageBox.warning(self, "输入错误", "请选择二级单位并输入密码。")
            return

        try:
            payload = {"username": self.unit_name, "password": password}
            response = requests.post(f"{API_BASE_URL}/api/login", json=payload)
            data = response.json()
            if response.status_code == 200 and data.get('success'):
                self.projects = data.get('projects', [])
                QMessageBox.information(self, "成功", "登录成功！")
                self.accept()
            else:
                QMessageBox.warning(self, "登录失败", data.get('message', '未知错误'))
        except requests.exceptions.RequestException as e:
            QMessageBox.critical(self, "网络错误", f"登录请求失败: {e}")


# --- 密钥注入对话框 (Key Injection Dialog) ---
class KeyInjectionDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("请提供密钥信息")
        self.setMinimumWidth(400)
        self.key_type = ""
        self.key_info = ""

        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        # 密钥类型
        self.key_type_combo = QComboBox()
        # self.key_type_combo.addItems(["国网CA", "CFCA"])
        self.key_type_combo.addItems(["CFCA"])
        form_layout.addRow("请选择密钥类型:", self.key_type_combo)

        # 密钥信息
        self.key_info_input = QLineEdit()
        self.key_info_input.setPlaceholderText("请粘贴密钥信息")
        form_layout.addRow("密钥信息:", self.key_info_input)

        layout.addLayout(form_layout)

        # 提交按钮
        self.submit_button = QPushButton("提交")
        self.submit_button.setMinimumHeight(35)
        self.submit_button.clicked.connect(self.handle_submit)
        layout.addWidget(self.submit_button, alignment=Qt.AlignRight)

    def handle_submit(self):
        self.key_type = self.key_type_combo.currentText()
        self.key_info = self.key_info_input.text()
        if not self.key_info:
            QMessageBox.warning(self, "输入错误", "请粘贴密钥信息。")
            return
        self.accept()

    def get_key_data(self):
        return self.key_type, self.key_info


# --- 用于QTableView的Pandas模型 (Pandas Model for QTableView) ---
class PandasModel(QAbstractTableModel):
    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid() and role == Qt.DisplayRole:
            return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Vertical:
                return str(self._data.index[section])
        return None


# --- 用于耗时任务的工作线程 (Worker Thread for Time-Consuming Tasks) ---
class Worker(QObject):
    finished = Signal(str, object)
    error = Signal(str)
    progress = Signal(str, bool)

    def __init__(self, driver, source_name):
        super().__init__()
        self.driver = driver
        self.source_name = source_name

    def run(self):
        try:
            self.progress.emit(f"开始从 '{self.source_name}' 获取数据...", False)
            if self.source_name == "北京电力交易中心":
                self._clean_download_dir()

            if self.source_name == "能源局网站":
                data = self._fetch_nyjwz()
            elif self.source_name == "绿证交易平台":
                data = self._fetch_lzy()
            elif self.source_name == "北京电力交易中心":
                data = self._fetch_bjdl()
            elif self.source_name == "广州电力交易中心":
                data = self._fetch_gjdl()
            else:
                raise ValueError("未知的数据源")
            self.finished.emit(self.source_name, data)
        except Exception as e:
            self.error.emit(f"获取 '{self.source_name}' 数据时出错: {e}")

    def _clean_download_dir(self):
        self.progress.emit(f"正在清空下载文件夹: {DOWNLOAD_DIR}", False)
        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)
        for file in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                self.error.emit(f"删除文件 {file_path} 失败: {e}")

    def _fetch_nyjwz(self):
        self.progress.emit("正在从localStorage和cookies获取认证信息...", False)
        admin = self.driver.execute_script("return window.localStorage.getItem('admin.user');")
        if not admin:
            raise ConnectionError("无法获取用户信息，请先在网站上手动登录。")
        accountCode = json.loads(admin)['accountCode']
        cookies = self.driver.get_cookies()
        auth_cookie_val = next((c['value'] for c in cookies if c.get('domain') == 'gec.nea.gov.cn' and c.get('name') == 'Authorization'), None)
        if not auth_cookie_val:
            raise ConnectionError("无法获取Authorization cookie，请先在网站上手动登录。")
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Authorization': auth_cookie_val.replace('%20', ' '),
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        }
        self.progress.emit("正在获取绿证台账数据...", False)
        params_calendar = {'accountCode': accountCode, 'pageNum': '1', 'pageSize': '99999'}
        response_calendar = requests.get('https://gec.nea.gov.cn/api/ordinary/standing/greenCalendar', params=params_calendar, headers=headers)
        response_calendar.raise_for_status()
        df_calendar = pd.DataFrame(response_calendar.json()['rows'])
        self.progress.emit("正在获取交易记录数据...", False)
        params_trades = {'accountCode': accountCode, 'type': '1', 'pageSize': '99999', 'pageNum': '1'}
        response_trades = requests.get('https://gec.nea.gov.cn/api/ordinary/standing/transactionRecord', params=params_trades, headers=headers)
        response_trades.raise_for_status()
        df_trades = pd.DataFrame(response_trades.json()['rows'])
        return {"绿证台账": df_calendar, "交易记录": df_trades}

    def _fetch_lzy(self):
        self.progress.emit("正在从localStorage和cookies获取认证信息...", False)
        vuex_data = self.driver.execute_script("return window.localStorage.getItem('vuex');")
        if not vuex_data:
            raise ConnectionError("无法获取vuex数据，请先在网站上手动登录。")
        vuex_json = json.loads(vuex_data)
        userid = vuex_json.get('user', {}).get('userid')
        token = vuex_json.get('token')
        blocker = vuex_json.get('blocker')
        wzws_sessionid = self.driver.get_cookie('wzws_sessionid')['value']
        if not all([userid, token, blocker, wzws_sessionid]):
            raise ConnectionError("认证信息不完整，请先在网站上手动登录。")
        headers = {'Accept': 'application/json, text/plain, */*', 'Authorization': token, 'blocker': blocker, 'user_id': str(userid),
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
        cookies = {'wzws_sessionid': wzws_sessionid}
        self.progress.emit("正在获取'单向挂牌'数据...", False)
        params_danxiang = {'pageIndex': '1', 'pageSize': '99999', 'orderType': '1'}
        res_danxiang = requests.get('https://www.greenenergy.org.cn/sgcc/sgcc-order/order/sellerOrderList', params=params_danxiang, headers=headers, cookies=cookies)
        res_danxiang.raise_for_status()
        df_danxiang = pd.DataFrame(res_danxiang.json()['data']['rows'])
        self.progress.emit("正在获取'双边线上'数据...", False)
        params_online = {'pageIndex': '1', 'pageSize': '99999', 'orderType': '2', 'isOnline': '0'}
        res_online = requests.get('https://www.greenenergy.org.cn/sgcc/sgcc-order/order/sellerOrderList', params=params_online, headers=headers, cookies=cookies)
        res_online.raise_for_status()
        df_online = pd.DataFrame(res_online.json()['data']['rows'])
        self.progress.emit("正在获取'双边线下'数据...", False)
        params_offline = {'pageIndex': '1', 'pageSize': '99999', 'orderType': '2', 'isOnline': '1'}
        res_offline = requests.get('https://www.greenenergy.org.cn/sgcc/sgcc-order/order/sellerOrderList', params=params_offline, headers=headers, cookies=cookies)
        res_offline.raise_for_status()
        df_offline = pd.DataFrame(res_offline.json()['data']['rows'])
        return {"单向挂牌": df_danxiang, "双边线上": df_online, "双边线下": df_offline}

    def _fetch_bjdl(self):
        self.driver.get('http://103.120.198.33:21004/pxf-green-out/greenCardTrade/resultsTrade')
        self.progress.emit("页面加载完成，正在查找'导出'按钮...", False)
        export_button = WebDriverWait(self.driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'el-button') and contains(., '导出')]")))
        export_button.click()
        self.progress.emit("'导出'按钮已点击，等待文件下载...", False)
        timeout = 60
        start_time = time.time()
        downloaded_file_path = None
        target_files = ["交易结果.xls", "交易结果.xlsx"]
        while time.time() - start_time < timeout:
            files = os.listdir(DOWNLOAD_DIR)
            for file in files:
                if file in target_files:
                    downloaded_file_path = os.path.join(DOWNLOAD_DIR, file)
                    break
            if downloaded_file_path:
                break
            time.sleep(1)
        if downloaded_file_path:
            self.progress.emit(f"文件 '{os.path.basename(downloaded_file_path)}' 下载成功，正在读取...", False)
            df = pd.read_excel(downloaded_file_path, header=0)

            if '平价绿证交易结果' in df.columns:
                df['projectName'] = df['平价绿证交易结果']
            else:
                df['projectName'] = ''
            return df
        else:
            raise FileNotFoundError(f"下载超时，在 {timeout} 秒内未找到目标文件。")

    def _fetch_gjdl(self):
        self.progress.emit("正在获取CAMSID cookie...", False)
        cookie_obj = self.driver.get_cookie('CAMSID')
        if not cookie_obj:
            raise ConnectionError("无法获取CAMSID cookie，请先在网站上手动登录。")
        camsid = cookie_obj['value']
        cookies = {'CAMSID': camsid}
        headers = {'Accept': 'application/json, text/plain, */*', 'Content-Type': 'application/json;charset=UTF-8', 'Origin': 'https://gp.poweremarket.com',
                   'Referer': 'https://gp.poweremarket.com/rept/sr/dmz/gcc/greenTradeListCompany.html',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
        json_data = {'pageNum': 1, 'pageSize': 9999, 'sortType': '01', 'sortOrder': '02'}
        self.progress.emit("正在发送API请求获取数据...", False)
        response = requests.post('https://gp.poweremarket.com/rept/ma/gcc/gcc/seeOrderInfo/selectListPage', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()
        data = response.json()['data']['list']
        df = pd.DataFrame(data)
        return df


# --- 项目映射对话框 (Project Mapping Dialog) ---
class ProjectMappingDialog(QDialog):
    def __init__(self, unique_project_names, server_projects, parent=None):
        super().__init__(parent)
        self.setWindowTitle("项目对应关系")
        self.setMinimumWidth(1200)
        self.mappings = {}
        main_layout = QVBoxLayout(self)

        header_layout = QHBoxLayout()
        header_label1 = QLabel("<b>数据中的项目名称</b>")
        header_label2 = QLabel("<b>提交到以下项目</b>")
        header_label1.setStyleSheet("font-size: 11pt; color:red; font-weight:bold;")
        header_label2.setStyleSheet("font-size: 11pt; color:red; font-weight:bold;")
        header_layout.addWidget(header_label1, 1)
        header_layout.addWidget(header_label2, 1)
        main_layout.addLayout(header_layout)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        content_layout = QVBoxLayout(scroll_content)

        project_options = ["丢弃"] + server_projects
        for name in unique_project_names:
            row_layout = QHBoxLayout()
            label = QLabel(name)
            label.setStyleSheet("font-size: 11pt; padding-top: 5px;")
            label.setWordWrap(True)
            combo = QComboBox()
            combo.addItems(project_options)
            combo.setStyleSheet("font-size: 11pt; padding: 4px;")
            if name in project_options:
                combo.setCurrentText(name)

            row_layout.addWidget(label, 1)
            row_layout.addWidget(combo, 1)
            content_layout.addLayout(row_layout)
            self.mappings[name] = combo

        content_layout.addStretch()
        scroll_area.setWidget(scroll_content)
        main_layout.addWidget(scroll_area)

        button_layout = QHBoxLayout()
        self.ok_button = QPushButton("提交")
        self.cancel_button = QPushButton("取消")
        self.ok_button.setMinimumHeight(35)
        self.cancel_button.setMinimumHeight(35)
        button_layout.addStretch()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        main_layout.addLayout(button_layout)

        self.ok_button.clicked.connect(self.accept)
        self.cancel_button.clicked.connect(self.reject)

    def get_mappings(self):
        result = {}
        for name, combo in self.mappings.items():
            result[name] = combo.currentText()
        return result


# --- 主应用窗口 (Main Application Window) ---
class MainWindow(QMainWindow):
    def __init__(self, unit_name, projects):
        super().__init__()
        self.setWindowTitle(f"数据处理应用 v3.9 - [{unit_name}]")
        self.setGeometry(100, 100, 1200, 800)
        self.logged_in_unit = unit_name
        self.project_list = projects
        self.data_frames = {}
        self.driver = self._init_webdriver()
        if not self.driver:
            sys.exit(1)
        self._init_ui()
        self.thread = None
        self.worker = None
        self.log_status(f"登录成功，单位: {unit_name}。请选择数据源，然后点击'打开网站'或'从文件加载数据'。")

    def _init_webdriver(self):
        try:
            if not os.path.exists(DOWNLOAD_DIR):
                os.makedirs(DOWNLOAD_DIR)
            chrome_options = webdriver.ChromeOptions()
            if os.path.exists(CHROME_PATH):
                chrome_options.binary_location = CHROME_PATH
            prefs = {"download.default_directory": DOWNLOAD_DIR, "download.prompt_for_download": False, "download.directory_upgrade": True, "safebrowsing.enabled": True}
            chrome_options.add_experimental_option("prefs", prefs)
            service = Service(CHROMEDRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            QMessageBox.critical(self, "WebDriver错误", f"初始化Chrome驱动失败: {e}\n请确保Chrome和ChromeDriver路径正确且版本匹配。")
            return None

    def _init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QHBoxLayout(main_widget)
        left_pane = QWidget()
        left_layout = QVBoxLayout(left_pane)
        left_pane.setFixedWidth(400)
        self.tree = QTreeWidget()
        self.tree.setColumnCount(2)
        self.tree.setHeaderLabels(["数据源", "状态"])
        self.tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self.tree.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.tree.setFixedHeight(250)
        self.tree.setStyleSheet("QTreeWidget { font-size: 10pt; } QTreeWidget::item { padding-top: 5px; padding-bottom: 5px; } QTreeWidget::item:selected { background-color: #a8d8ff; color: black; }")
        sources = ["能源局网站", "绿证交易平台", "北京电力交易中心", "广州电力交易中心"]
        for source in sources:
            item = QTreeWidgetItem(self.tree, [source, "待提交"])
            item.setData(0, Qt.UserRole, source)

        # --- 网站操作行 ---
        website_layout = QHBoxLayout()
        self.keyless_checkbox = QCheckBox("北交免密钥")
        self.btn_open_website = QPushButton("打开网站")
        website_layout.addWidget(self.keyless_checkbox)
        website_layout.addWidget(self.btn_open_website)

        # --- 数据操作行 ---
        data_op_layout = QHBoxLayout()
        self.btn_get_data = QPushButton("获取数据")
        self.btn_load_file = QPushButton("从文件加载数据")
        data_op_layout.addWidget(self.btn_get_data)
        data_op_layout.addWidget(self.btn_load_file)

        self.btn_submit_data = QPushButton("提交数据")
        for btn in [self.btn_open_website, self.btn_get_data, self.btn_load_file, self.btn_submit_data]:
            btn.setMinimumHeight(45)

        self.status_box = QTextEdit()
        self.status_box.setReadOnly(True)

        left_layout.addWidget(self.tree)
        left_layout.addLayout(website_layout)
        left_layout.addLayout(data_op_layout)

        left_layout.addWidget(self.btn_submit_data)
        left_layout.addWidget(self.status_box)
        self.tabs = QTabWidget()
        main_layout.addWidget(left_pane)
        main_layout.addWidget(self.tabs)

        # --- 信号连接 ---
        self.btn_open_website.clicked.connect(self.open_website)
        self.btn_get_data.clicked.connect(self.get_data)
        self.btn_load_file.clicked.connect(self.load_data_from_file)
        self.btn_submit_data.clicked.connect(self.submit_data)

    def log_status(self, message, is_progress=False):
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        formatted_message = f"[Progress] {message}" if is_progress else f"[{timestamp}] {message}"
        current_text = self.status_box.toPlainText()
        lines = current_text.split('\n')
        if is_progress:
            new_lines = [formatted_message if line.startswith('[Progress]') else line for line in lines]
            if not any(line.startswith('[Progress]') for line in lines):
                new_lines.insert(0, formatted_message)
            self.status_box.setPlainText('\n'.join(new_lines))
        else:
            self.status_box.moveCursor(QTextCursor.Start)
            self.status_box.insertPlainText(f"{formatted_message}\n")
        self.status_box.moveCursor(QTextCursor.Start)

    def get_selected_source(self):
        selected_items = self.tree.selectedItems()
        if not selected_items:
            self.log_status("错误：请先在左侧选择一个数据源。")
            QMessageBox.warning(self, "提示", "请先在左侧选择一个数据源。")
            return None
        return selected_items[0].data(0, Qt.UserRole)

    def open_website(self):
        source = self.get_selected_source()
        if not source: return
        try:
            urls = {"能源局网站": 'https://gec.nea.gov.cn/#/login', "绿证交易平台": 'https://www.greenenergy.org.cn/', "北京电力交易中心": 'http://103.120.198.33:21004/pxf-green-out/home',
                    "广州电力交易中心": 'https://gp.poweremarket.com/rept/sr/mp/portaladmin/login.html#/'}
            url = urls.get(source)
            if not url:
                self.log_status(f"错误: 未找到 '{source}' 的URL。")
                return

            # 新增逻辑：处理“北交免密钥”
            if source == "北京电力交易中心" and self.keyless_checkbox.isChecked():
                key_dialog = KeyInjectionDialog(self)
                if key_dialog.exec() == QDialog.Accepted:
                    key_type, key_info = key_dialog.get_key_data()
                    self.log_status(f"获取到密钥信息: 类型='{key_type}', 信息长度='{len(key_info)}'")
                    # TODO: 在此处实现密钥注入逻辑 (Implement key injection logic here)
                    if key_type == 'CFCA':
                        js_code = """
                                                            (function() {
                                                                const originalWebSocket = window.WebSocket;
                                                                window.WebSocket = function(url, protocols) {
                                                                    if (url.startsWith('wss://127.0.0.1:7693')) {
                                                                        console.log('[Interceptor] WebSocket connection intercepted: ', url);
                                                                        const ws = new originalWebSocket(url, protocols);

                                                                        // 预设响应
                                                                        const responses = {
                                                                            'SetCSPList': {"function": "SetCSPList", "errorcode": 0, "result": true},
                                                                            'SelectSignCertificate': {
                                                                                "function": "SelectSignCertificate",
                                                                                "errorcode": 0,
                                                                                "result": "CN=PMOS02@临县公司@Z91141124MA0K2KMD7C@6, OU=Organizational-1, OU=PMOS02, O=OCA1RSA, C=CN"
                                                                            },
                                                                            'SignMessage': {"function": "SignMessage", "errorcode": 0, "result": "xxxxxx"}
                                                                        };

                                                                        // 保存原始方法
                                                                        let originalOnMessage = null;
                                                                        const originalSend = ws.send;

                                                                        // 拦截 onmessage
                                                                        ws.onmessage = function(event) {
                                                                            console.log('[Interceptor] Original message received: ', event.data);
                                                                            // 不处理服务器消息
                                                                        };

                                                                        // 拦截 send 并发送预设响应
                                                                        ws.send = function(data) {
                                                                            console.log('[Interceptor] Message sent: ', data);
                                                                            try {
                                                                                const sentData = JSON.parse(data);
                                                                                const functionName = sentData.function;
                                                                                const response = responses[functionName];
                                                                                if (response) {
                                                                                    console.log('[Interceptor] Returning preset response: ', response);
                                                                                    const fakeEvent = new MessageEvent('message', {
                                                                                        data: JSON.stringify(response),
                                                                                        origin: 'wss://127.0.0.1:7693'
                                                                                    });

                                                                                    // 添加延迟，确保客户端回调已准备好
                                                                                    setTimeout(() => {
                                                                                        if (originalOnMessage) {
                                                                                            originalOnMessage.call(ws, fakeEvent);
                                                                                            console.log('[Interceptor] Triggered original onmessage');
                                                                                        } else {
                                                                                            console.log('[Interceptor] No original onmessage, dispatching event');
                                                                                            ws.dispatchEvent(fakeEvent);
                                                                                        }
                                                                                    }, 10); // 10ms 延迟
                                                                                } else {
                                                                                    console.warn('[Interceptor] No matching response for function: ', functionName);
                                                                                }
                                                                            } catch (e) {
                                                                                console.error('[Interceptor] Error parsing sent data: ', e, data);
                                                                            }
                                                                        };

                                                                        // 监听 onmessage 设置
                                                                        Object.defineProperty(ws, 'onmessage', {
                                                                            get() {
                                                                                return originalOnMessage;
                                                                            },
                                                                            set(newHandler) {
                                                                                console.log('[Interceptor] onmessage handler updated');
                                                                                originalOnMessage = newHandler;
                                                                            }
                                                                        });

                                                                        // 添加调试信息
                                                                        ws.onopen = function() {
                                                                            console.log('[Interceptor] WebSocket connection opened');
                                                                        };
                                                                        ws.onerror = function(error) {
                                                                            console.error('[Interceptor] WebSocket error: ', error);
                                                                        };
                                                                        ws.onclose = function() {
                                                                            console.log('[Interceptor] WebSocket connection closed');
                                                                        };

                                                                        return ws;
                                                                    }
                                                                    return new originalWebSocket(url, protocols);
                                                                };
                                                                console.log('[Interceptor] WebSocket interception initialized');
                                                            })();
                                                            """.replace('xxxxxx', key_info)
                        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                            "source": js_code
                        })
                        print(js_code)
                    # self.log_status("密钥注入占位符 - 准备打开网站...")
                else:
                    # self.log_status("取消了密钥信息输入。")
                    return

            self.log_status(f"正在打开网站: {source}")

            # 针对北京电力交易中心的特殊处理
            if source == "北京电力交易中心":
                self.log_status("检测到北京电力交易平台，正在导航并清空浏览器会话数据...")
                time.sleep(2)
                self.driver.get(url)
                # self.driver.delete_all_cookies()
                # self.driver.execute_script("window.localStorage.clear();")
                # self.driver.execute_script("window.sessionStorage.clear();")
                # self.driver.refresh()
                self.log_status("会话数据已清空，页面已刷新。")
            else:
                self.driver.get(url)

            self.log_status(f"网站 '{source}' 已打开。请手动完成登录。")

        except Exception as e:
            self.log_status(f"打开网站时出错: {e}")

    def get_data(self):
        source = self.get_selected_source()
        if not source: return
        self.thread = QThread()
        self.worker = Worker(self.driver, source)
        self.worker.moveToThread(self.thread)
        self.worker.progress.connect(self.log_status)
        self.worker.error.connect(self.on_fetch_error)
        self.worker.finished.connect(self.on_fetch_finished)
        self.thread.started.connect(self.worker.run)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.start()
        self.btn_get_data.setEnabled(False)
        self.btn_submit_data.setEnabled(False)
        self.log_status(f"正在为 '{source}' 启动数据获取任务...")

    def load_data_from_file(self):
        source = self.get_selected_source()
        if not source:
            return

        file_path, _ = QFileDialog.getOpenFileName(self, "选择数据文件", "", "Text Files (*.txt)")
        if not file_path:
            self.log_status("文件选择已取消。")
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            self.log_status(f"成功从 '{os.path.basename(file_path)}' 读取文件。")

            data = json.loads(content)

            processed_data = {}
            if isinstance(data, list):
                if source == "北京电力交易中心":
                    df = pd.DataFrame(data)
                    if '平价绿证交易结果' in df.columns:
                        df['projectName'] = df['平价绿证交易结果']
                    else:
                        df['projectName'] = ''
                    processed_data = df
                else:
                    df = pd.DataFrame(data)
                    processed_data = df
            elif isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, list):
                        processed_data[key] = pd.DataFrame(value)
                    else:
                        self.log_status(f"警告: 文件中 '{key}' 的数据格式无法识别，已跳过。")
                if not processed_data:
                    raise ValueError("JSON 字典中未找到可识别的列表数据。")
            else:
                raise ValueError("不支持的JSON顶层结构。")

            self.on_fetch_finished(source, processed_data)

        except Exception as e:
            self.log_status(f"加载文件时发生错误: {e}")
            QMessageBox.critical(self, "错误", f"加载文件时发生错误: {e}")

    def on_fetch_error(self, error_message):
        self.log_status(f"错误: {error_message}")
        if self.thread:
            self.thread.quit()
        self.btn_get_data.setEnabled(True)
        self.btn_submit_data.setEnabled(True)

    def on_fetch_finished(self, source_name, data):
        self.log_status(f"成功获取到 '{source_name}' 的数据。")
        self.data_frames[source_name] = data
        self.display_data(source_name, data)
        if self.thread:
            self.thread.quit()
        self.btn_get_data.setEnabled(True)
        self.btn_submit_data.setEnabled(True)

    def display_data(self, source_name, data):
        self.tabs.clear()
        if isinstance(data, dict):
            for tab_name, df in data.items():
                if not isinstance(df, pd.DataFrame) or df.empty:
                    self.log_status(f"警告: '{tab_name}' 的数据为空或格式不正确，跳过显示。")
                    continue
                view = QTableView()
                model = PandasModel(df)
                view.setModel(model)
                view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
                self.tabs.addTab(view, tab_name)
        elif isinstance(data, pd.DataFrame):
            if data.empty:
                self.log_status(f"警告: '{source_name}' 的数据为空，不显示表格。")
                return
            view = QTableView()
            display_df = data.copy()
            if source_name == "北京电力交易中心":
                display_df.columns = display_df.iloc[0]
                display_df = display_df.iloc[1:].drop(columns=['projectName'], errors='ignore')
                display_df.reset_index(drop=True, inplace=True)

            model = PandasModel(display_df)
            view.setModel(model)
            view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
            self.tabs.addTab(view, source_name)
        self.log_status("数据已更新到右侧表格。")

    def submit_data(self):
        source = self.get_selected_source()
        if not source: return
        if source not in self.data_frames:
            self.log_status(f"错误：没有找到 '{source}' 的本地缓存数据。请先获取数据。")
            QMessageBox.warning(self, "操作失败", f"没有找到 '{source}' 的本地缓存数据。请先获取数据。")
            return
        original_data = self.data_frames[source]
        data_to_process = {}
        if isinstance(original_data, dict):
            for k, v in original_data.items():
                data_to_process[k] = v.copy()
        elif isinstance(original_data, pd.DataFrame):
            data_to_process = {"main": original_data.copy()}
        unique_project_names = set()
        has_project_name_column = False

        project_name_col_map = {
            "北京电力交易中心": "projectName",  # 临时添加的列
            "能源局网站": "projectName",
            "绿证交易平台": "projectName",
            "广州电力交易中心": "projectName",
        }

        project_name_col = ''
        for name, df in data_to_process.items():
            # 确定当前数据源使用哪个列作为项目名称
            if source == "北京电力交易中心":
                project_name_col = "projectName"  # 我们后台创建的临时列
                # 北京电力交易中心的标题行中，项目名称列也叫“项目名称”，需要过滤
                title_row_value = "项目名称"
            else:
                project_name_col = "projectName"  # 其他平台都叫这个
                title_row_value = None  # 其他平台没有标题行

            if project_name_col in df.columns:
                has_project_name_column = True
                df[project_name_col] = df[project_name_col].astype(str).str.strip().str.replace(r'[\r\n]+', '', regex=True)

                # 过滤掉标题行、空值和nan
                mask = (df[project_name_col].notna()) & \
                       (df[project_name_col] != '') & \
                       (df[project_name_col].str.lower() != 'nan')
                if title_row_value:
                    mask &= (df[project_name_col] != title_row_value)

                valid_projects = df.loc[mask, project_name_col].unique()
                unique_project_names.update(valid_projects)

        if has_project_name_column:
            if not unique_project_names:
                self.log_status(f"警告: '{source}' 数据中未找到有效的项目名称。无法进行提交。")
                QMessageBox.warning(self, "无项目数据", f"'{source}' 数据中未找到有效的项目名称。")
                return
            dialog = ProjectMappingDialog(sorted(list(unique_project_names)), self.project_list, self)
            if dialog.exec() == QDialog.Accepted:
                mappings = dialog.get_mappings()
                self.log_status("开始分项目提交数据...")
                self.process_mapped_submission(source, data_to_process, mappings, project_name_col_map.get(source))
            else:
                self.log_status("提交操作已取消。")
        else:
            self.log_status(f"'{source}' 数据源没有项目列，将为整个数据集选择一个目标项目。")
            project_name, ok = QInputDialog.getItem(self, "选择提交项目", f"请为 '{source}' 的所有数据选择一个提交项目:", self.project_list, 0, False)
            if ok and project_name:
                mappings = {"__ENTIRE_DATASET__": project_name}
                self.process_mapped_submission(source, data_to_process, mappings, None)
            else:
                self.log_status("提交操作已取消。")

    def process_mapped_submission(self, source, data_to_process, mappings, project_name_col):
        self.btn_submit_data.setEnabled(False)
        overall_success = True
        grouped_by_dest = defaultdict(list)
        for source_project, dest_project in mappings.items():
            if dest_project != "丢弃":
                grouped_by_dest[dest_project].append(source_project)
        if not grouped_by_dest:
            self.log_status("没有选择任何项目进行提交。")
            self.btn_submit_data.setEnabled(True)
            return
        for dest_project, source_projects in grouped_by_dest.items():
            self.log_status(f"正在准备提交到项目 '{dest_project}' (源: {', '.join(source_projects)})...")
            try:
                if source == "北京电力交易中心":
                    original_df = data_to_process['main']
                    data_rows = original_df[original_df[project_name_col].isin(source_projects)]
                    header_row = original_df.iloc[[0]]
                    df_to_submit = pd.concat([header_row, data_rows], ignore_index=True)
                    if 'projectName' in df_to_submit.columns:
                        df_to_submit = df_to_submit.drop(columns=['projectName'])
                    cleaned_df = df_to_submit.replace([np.inf, -np.inf], np.nan).astype(object).where(pd.notna(df_to_submit), None)
                    json_data = cleaned_df.to_dict(orient='records')
                else:
                    combined_payload_data = {}
                    is_entire_dataset = "__ENTIRE_DATASET__" in source_projects
                    if is_entire_dataset:
                        combined_payload_data = data_to_process
                    else:
                        for name, df in data_to_process.items():
                            if project_name_col in df.columns:
                                filtered_df = df[df[project_name_col].isin(source_projects)]
                                if not filtered_df.empty:
                                    combined_payload_data[name] = filtered_df
                    if not combined_payload_data:
                        self.log_status(f"警告: 过滤后目标项目 '{dest_project}' 无数据，跳过提交。")
                        continue

                    def clean_df(df):
                        # if 'projectName' in df.columns:
                            # df = df.drop(columns=['projectName'])
                        df_no_inf = df.replace([np.inf, -np.inf], np.nan)
                        return df_no_inf.astype(object).where(pd.notna(df_no_inf), None)

                    json_data = {}
                    if len(combined_payload_data) == 1 and "main" in combined_payload_data:
                        cleaned_df = clean_df(combined_payload_data["main"])
                        json_data = cleaned_df.to_dict(orient='records')
                    else:
                        for name, df in combined_payload_data.items():
                            cleaned_df = clean_df(df)
                            json_data[name] = cleaned_df.to_dict(orient='records')

                api_payload = {"project_name": dest_project, "source": source, "data": json_data}
                response = requests.post(f"{API_BASE_URL}/api/submit_data", json=api_payload)
                response.raise_for_status()
                response_data = response.json()
                if response_data.get("success"):
                    self.log_status(f"成功: 数据已提交到 '{dest_project}'. 服务器: {response_data.get('message')}")
                else:
                    overall_success = False
                    error_msg = response_data.get('message', '未知服务器错误')
                    self.log_status(f"失败: 提交到 '{dest_project}'. 服务器: {error_msg}")
                    QMessageBox.critical(self, f"提交失败: {dest_project}", f"服务器返回错误: {error_msg}")
            except requests.exceptions.RequestException as e:
                overall_success = False
                error_details = f"服务器响应: {e.response.text}" if hasattr(e, 'response') and e.response else ""
                self.log_status(f"网络错误: 提交到 '{dest_project}'. {e}. {error_details}")
                QMessageBox.critical(self, "网络错误", f"无法连接到服务器提交到 '{dest_project}': {e}\n{error_details}")
            except Exception as e:
                overall_success = False
                import traceback
                error_details = traceback.format_exc()
                self.log_status(f"未知错误: 提交到 '{dest_project}'. {e}\n{error_details}")
                QMessageBox.critical(self, "未知错误", f"处理到 '{dest_project}' 的数据时发生意外错误: {e}")
        if overall_success:
            self.log_status("所有选择的数据均已成功提交。")
            QMessageBox.information(self, "成功", "所有选定项目的数据已成功提交。")
            selected_item = self.tree.selectedItems()[0]
            selected_item.setText(1, "已提交")
        else:
            self.log_status("部分或全部数据提交失败，请检查日志。")
            QMessageBox.warning(self, "提交完成", "部分或全部数据提交失败，请检查状态日志获取详细信息。")
        self.btn_submit_data.setEnabled(True)

    def closeEvent(self, event):
        self.log_status("正在关闭应用...")
        if self.driver:
            self.driver.quit()
        event.accept()


def main():
    """
    应用程序的主入口点。
    """
    app = QApplication(sys.argv)
    login_dialog = LoginDialog()
    if login_dialog.exec() == QDialog.Accepted:
        projects = login_dialog.projects
        unit_name = login_dialog.unit_name
        window = MainWindow(unit_name=unit_name, projects=projects)
        if window.driver:
            window.show()
            sys.exit(app.exec())
    else:
        sys.exit(0)
