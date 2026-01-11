import streamlit as st
import pandas as pd
import io
import json
import time
import calendar
import smtplib
import random
import string
import hashlib
import urllib.parse 
from email.mime.text import MIMEText
from datetime import date, datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ==========================================
# 1. ตั้งค่า Page & CSS Styles
# ==========================================
st.set_page_config(page_title="JST Hybrid System", layout="wide", page_icon="📦")

# CSS สำหรับปรับแต่ง Radio Button ให้หน้าตาเหมือน Tabs และตาราง
st.markdown("""
<style>
    .block-container { padding-top: 1rem !important; padding-bottom: 2rem !important; }
    
    /* Custom Table Header */
    [data-testid="stDataFrame"] th { 
        text-align: center !important; 
        background-color: #1e3c72 !important; 
        color: white !important; 
        vertical-align: middle !important; 
        min-height: 60px; 
        font-size: 14px; 
        border-bottom: 2px solid #ffffff !important; 
    }
    
    [data-testid="stDataFrame"] th:first-child { border-top-left-radius: 8px; }
    [data-testid="stDataFrame"] th:last-child { border-top-right-radius: 8px; }
    [data-testid="stDataFrame"] td { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px; }
    
    .stButton button { width: 100%; }
    
    /* Hide number input arrows */
    button[data-testid="stNumberInputStepDown"], button[data-testid="stNumberInputStepUp"] { display: none !important; }
    div[data-testid="stNumberInput"] input { text-align: left; }

    /* Custom Navigation (Radio as Tabs) */
    div[role="radiogroup"] > label {
        background-color: #262730;
        border: 1px solid #4a4a4a;
        padding: 10px 20px;
        border-radius: 8px;
        margin-right: 10px;
        transition: all 0.3s;
    }
    div[role="radiogroup"] > label:hover {
        border-color: #ff4b4b;
        color: #ff4b4b;
    }
    div[role="radiogroup"] > label[data-checked="true"] {
        background-color: #ff4b4b;
        color: white;
        border-color: #ff4b4b;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. Config & Google Cloud Connection
# ==========================================
MASTER_SHEET_ID = "1SC_Dpq2aiMWsS3BGqL_Rdf7X4qpTFkPA0wPV6mqqosI"
TAB_NAME_STOCK = "MASTER"
TAB_NAME_PO = "PO_DATA"
FOLDER_ID_DATA_SALE = "12jyMKgFHoc9-_eRZ-VN9QLsBZ31ZJP4T"

@st.cache_resource
def get_credentials():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" in st.secrets:
        creds_dict = json.loads(st.secrets["gcp_service_account"]) if isinstance(st.secrets["gcp_service_account"], str) else dict(st.secrets["gcp_service_account"])
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        return service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)
    return service_account.Credentials.from_service_account_file("credentials.json", scopes=scope)

# ==========================================
# 3. ระบบ AUTHENTICATION
# ==========================================

def create_token(email):
    salt = "jst_secret_salt" 
    raw = f"{email}{salt}{date.today()}"
    return hashlib.md5(raw.encode()).hexdigest()

def send_otp_email(receiver_email, otp_code):
    try:
        sender_email = st.secrets["email"]["sender"]
        sender_password = st.secrets["email"]["password"]
    except KeyError:
        st.error("❌ ไม่พบการตั้งค่า Email ใน st.secrets")
        return False
    
    subject = "รหัสยืนยันตัวตน (OTP) - JST Hybrid System"
    body = f"รหัสเข้าใช้งานของคุณคือ: {otp_code}\n\n(รหัสนี้ใช้สำหรับการเข้าสู่ระบบครั้งนี้เท่านั้น)"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        return True
    except Exception as e:
        st.error(f"❌ ส่งอีเมลไม่สำเร็จ: {e}")
        return False

def log_login_activity(email):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        try: ws = sh.worksheet("LOGIN_LOG")
        except:
            ws = sh.add_worksheet(title="LOGIN_LOG", rows="1000", cols="2")
            ws.append_row(["Timestamp", "Email"])
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([timestamp, email])
    except Exception as e:
        print(f"Login Log Error: {e}")

# --- Initialize Session State ---
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'otp_sent' not in st.session_state: st.session_state.otp_sent = False
if 'generated_otp' not in st.session_state: st.session_state.generated_otp = None
if 'user_email' not in st.session_state: st.session_state.user_email = ""
if 'current_page' not in st.session_state: st.session_state.current_page = "📅 สรุปยอดขายรายวัน"
if "target_edit_data" not in st.session_state: st.session_state.target_edit_data = {}

# --- AUTO LOGIN LOGIC ---
url_token = st.query_params.get("token", None)

if not st.session_state.logged_in and url_token:
    try:
        allowed_users = st.secrets["access"]["allowed_users"]
        for user in allowed_users:
            if create_token(user) == url_token:
                st.session_state.logged_in = True
                st.session_state.user_email = user
                st.toast(f"🔙 กลับเข้าสู่ระบบอัตโนมัติ: {user}", icon="👋")
                break
    except: pass

if st.session_state.logged_in:
    current_token = create_token(st.session_state.user_email)
    if url_token != current_token:
        st.query_params["token"] = current_token

# --- LOGIN FORM ---
if not st.session_state.logged_in:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 🔐 JST Hybrid System Login")
        with st.container(border=True):
            if not st.session_state.otp_sent:
                st.info("กรุณากรอกอีเมลเพื่อรับรหัส OTP")
                email_input = st.text_input("📧 อีเมล (Gmail)", placeholder="example@gmail.com")
                
                if st.button("ส่งรหัสยืนยัน (Send OTP)", type="primary"):
                    try: allowed_users = st.secrets["access"]["allowed_users"]
                    except KeyError:
                        st.error("❌ ไม่พบการตั้งค่า allowed_users")
                        st.stop()

                    if email_input.strip() in allowed_users:
                        otp = ''.join(random.choices(string.digits, k=6))
                        st.session_state.generated_otp = otp
                        st.session_state.user_email = email_input.strip()
                        
                        with st.spinner("⏳ กำลังส่งรหัสไปยังอีเมลของคุณ..."):
                            if send_otp_email(email_input.strip(), otp):
                                st.session_state.otp_sent = True
                                st.toast("✅ ส่งรหัสเรียบร้อยแล้ว! โปรดเช็คอีเมล", icon="📧")
                                st.rerun()
                    else:
                        st.error("⛔️ อีเมลนี้ไม่ได้รับอนุญาตให้เข้าใช้งาน")
            else:
                st.success(f"รหัสถูกส่งไปที่: **{st.session_state.user_email}**")
                otp_input = st.text_input("🔑 กรอกรหัส 6 หลัก", max_chars=6, type="password")
                
                c_btn1, c_btn2 = st.columns(2)
                if c_btn1.button("ยืนยันรหัส (Verify)", type="primary"):
                    if otp_input == st.session_state.generated_otp:
                        st.session_state.logged_in = True
                        log_login_activity(st.session_state.user_email)
                        token = create_token(st.session_state.user_email)
                        st.query_params["token"] = token
                        st.toast("ยินดีต้อนรับ!", icon="🎉")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ รหัสไม่ถูกต้อง กรุณาลองใหม่")
                
                if c_btn2.button("ยกเลิก / ส่งใหม่"):
                    st.session_state.otp_sent = False
                    st.session_state.generated_otp = None
                    st.rerun()
    st.stop()

# ==========================================
# 4. ฟังก์ชันจัดการข้อมูล (Data Functions)
# ==========================================

def highlight_negative(val):
    if isinstance(val, (int, float)) and val < 0:
        return 'color: #ff4b4b; font-weight: bold;'
    return ''

@st.cache_data(ttl=300)
def get_stock_from_sheet():
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_STOCK)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        df.columns = df.columns.astype(str).str.strip()
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'รหัส': 'Product_ID', 'ID': 'Product_ID',
            'ชื่อสินค้า': 'Product_Name', 'ชื่อ': 'Product_Name', 'Name': 'Product_Name',
            'รูป': 'Image', 'รูปภาพ': 'Image', 'Link รูป': 'Image',
            'Stock': 'Initial_Stock', 'จำนวน': 'Initial_Stock', 'สต็อก': 'Initial_Stock', 'คงเหลือ': 'Initial_Stock',
            'Min_Limit': 'Min_Limit', 'Min': 'Min_Limit', 'จุดเตือน': 'Min_Limit',
            'Type': 'Product_Type', 'หมวดหมู่': 'Product_Type', 'Category': 'Product_Type', 'กลุ่ม': 'Product_Type'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
        if 'Initial_Stock' not in df.columns: df['Initial_Stock'] = 0
        if 'Product_ID' not in df.columns: df['Product_ID'] = "Unknown"
        if 'Product_Name' not in df.columns: df['Product_Name'] = df['Product_ID']
        if 'Product_Type' not in df.columns: df['Product_Type'] = "ทั่วไป"
        
        df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล Master Stock ไม่ได้: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_po_data():
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'เลข PO': 'PO_Number', 'ขนส่ง': 'Transport_Type',
            'วันที่สั่งซื้อ': 'Order_Date', 
            'Expected_Date': 'Expected_Date', 'วันที่คาดว่าจะได้รับ': 'Expected_Date', 'วันที่คาดการณ์': 'Expected_Date',
            'วันที่ได้รับ': 'Received_Date', 
            'จำนวน': 'Qty_Ordered',          
            'จำนวนที่ได้รับ': 'Qty_Received', 
            'ราคา/ชิ้น': 'Price_Unit_NoVAT', 'ราคา (หยวน)': 'Total_Yuan', 'เรทเงิน': 'Yuan_Rate',
            'เรทค่าขนส่ง': 'Ship_Rate', 'ขนาด (คิว)': 'CBM', 'ค่าส่ง': 'Ship_Cost', 'น้ำหนัก / KG': 'Transport_Weight',
            'SHOPEE': 'Shopee_Price', 'LAZADA': 'Lazada_Price', 'TIKTOK': 'TikTok_Price', 'หมายเหตุ': 'Note',
            'ราคา (บาท)': 'Total_THB', 'Link_Shop': 'Link', 'WeChat': 'WeChat'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})

        if not df.empty:
            df['Sheet_Row_Index'] = range(2, len(df) + 2)
            for col in ['Qty_Ordered', 'Qty_Received', 'Total_Yuan', 'Yuan_Rate']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'Qty_Received' not in df.columns: df['Qty_Received'] = 0
            if 'Expected_Date' not in df.columns: df['Expected_Date'] = None
                 
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล PO ไม่ได้: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_sale_from_folder():
    try:
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(q=f"'{FOLDER_ID_DATA_SALE}' in parents and trashed=false", orderBy='modifiedTime desc', pageSize=100, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items: return pd.DataFrame()
        
        all_dfs = [] 
        for item in items:
            if not item['name'].endswith(('.xlsx', '.xls')): continue
            try:
                request = service.files().get_media(fileId=item['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False: status, done = downloader.next_chunk()
                fh.seek(0)
                temp_df = pd.read_excel(fh)
                col_map = {'รหัสสินค้า':'Product_ID', 'จำนวน':'Qty_Sold', 'ร้านค้า':'Shop', 'เวลาสั่งซื้อ':'Order_Time'}
                temp_df = temp_df.rename(columns={k:v for k,v in col_map.items() if k in temp_df.columns})
                
                if 'Qty_Sold' in temp_df.columns: 
                    temp_df['Qty_Sold'] = pd.to_numeric(temp_df['Qty_Sold'], errors='coerce').fillna(0).astype(int)
                if 'Order_Time' in temp_df.columns:
                    temp_df['Order_Time'] = pd.to_datetime(temp_df['Order_Time'], errors='coerce')
                    temp_df['Date_Only'] = temp_df['Order_Time'].dt.date
                
                if not temp_df.empty: all_dfs.append(temp_df)
            except: continue

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ อ่านไฟล์ Excel Sale ไม่ทัน: {e}")
        return pd.DataFrame()

# --- Functions: Save Data ---
def save_po_edit_split(row_index, current_row_data, new_row_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_curr.append("")
            else: formatted_curr.append(item)
        
        range_name = f"A{row_index}:X{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        formatted_new = []
        for item in new_row_data:
            if isinstance(item, (date, datetime)): formatted_new.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_new.append("")
            else: formatted_new.append(item)
            
        ws.append_row(formatted_new)
        
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Split ไม่สำเร็จ: {e}")
        return False

def save_po_edit_update(row_index, current_row_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_curr.append("")
            else: formatted_curr.append(item)
        
        range_name = f"A{row_index}:X{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Update ไม่สำเร็จ: {e}")
        return False

def save_po_batch_to_sheet(rows_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        ws.append_rows(rows_data)
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Batch ไม่สำเร็จ: {e}")
        return False
def delete_po_row_from_sheet(row_index):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        # ลบแถวตาม Index (Google Sheet เริ่มนับแถว 1, ข้อมูลเริ่มแถว 2)
        ws.delete_rows(int(row_index))
        
        st.cache_data.clear() # ล้าง Cache เพื่อให้ข้อมูลอัปเดตทันที
        return True
    except Exception as e:
        st.error(f"❌ ลบข้อมูลไม่สำเร็จ: {e}")
        return False


def update_master_limits(df_edited):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_STOCK)
        headers = ws.row_values(1)
        target_col_name = "Min_Limit"
        if target_col_name not in headers:
            ws.update_cell(1, len(headers) + 1, target_col_name)
            col_index = len(headers) + 1
        else:
            col_index = headers.index(target_col_name) + 1
        
        all_rows = ws.get_all_values()
        if len(all_rows) < 2: return
        header_row = all_rows[0]
        pid_idx = -1
        for i, h in enumerate(header_row):
            if h in ['รหัสสินค้า', 'รหัส', 'ID', 'Product_ID']:
                pid_idx = i
                break
        if pid_idx == -1: return
        
        limit_map = df_edited.set_index('Product_ID')['Min_Limit'].to_dict()
        values_to_update = []
        for row in all_rows[1:]:
            pid = str(row[pid_idx]) if len(row) > pid_idx else ""
            old_val = 10
            if len(row) >= col_index:
                try: old_val = int(row[col_index-1])
                except: old_val = 10
            
            if pid in limit_map: values_to_update.append([int(limit_map[pid])])
            else: values_to_update.append([old_val])

        range_name = f"{gspread.utils.rowcol_to_a1(2, col_index)}:{gspread.utils.rowcol_to_a1(len(values_to_update)+1, col_index)}"
        ws.update(range_name, values_to_update)
        st.toast("✅ บันทึกจุดเตือนเรียบร้อยแล้ว!", icon="💾")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"❌ บันทึกจุดเตือนไม่สำเร็จ: {e}")

# ==========================================
# 5. Main App & Data Loading
# ==========================================
st.sidebar.markdown(f"👤 **ผู้ใช้งาน:** {st.session_state.user_email}")
if st.sidebar.button("🚪 ออกจากระบบ"):
    st.session_state.logged_in = False
    st.session_state.otp_sent = False
    st.query_params.clear() 
    st.rerun()

st.title("📊 JST Hybrid Management System")

# --- 2. Sidebar ---
with st.sidebar:
    if st.button("🔄 รีเฟรชข้อมูลล่าสุด", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    st.subheader("📂 เมนูจัดการไฟล์")
    st.link_button("📂 ไฟล์ยอดขาย JST (Drive)", "https://drive.google.com/drive/folders/12jyMKgFHoc9-_eRZ-VN9QLsBZ31ZJP4T", use_container_width=True)
    st.link_button("📦 ไฟล์คลังสินค้าคงเหลือ JST (Drive)", "https://drive.google.com/drive/folders/1-hXu2RG2gNKMkW3ZFBFfhjQEhTacVYzk", use_container_width=True)
    st.divider()
    st.subheader("⚙️ ตั้งค่าระบบ")
    st.link_button("🔗 เพิ่ม SKU / Master", "https://docs.google.com/spreadsheets/d/1SC_Dpq2aiMWsS3BGqL_Rdf7X4qpTFkPA0wPV6mqqosI/edit?gid=0#gid=0", type="secondary", use_container_width=True)

# --- 3. Session State (Dialogs) ---
if "active_dialog" not in st.session_state: st.session_state.active_dialog = None 
if "selected_product_history" not in st.session_state: st.session_state.selected_product_history = None
if 'po_temp_cart' not in st.session_state: st.session_state.po_temp_cart = []

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    if not df_master.empty: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty: df_po['Product_ID'] = df_po['Product_ID'].astype(str)
    if not df_sale.empty: df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

recent_sales_map = {}
latest_date_str = "ไม่พบข้อมูล"
if not df_sale.empty and 'Date_Only' in df_sale.columns:
    max_date = df_sale['Date_Only'].max()
    latest_date_str = max_date.strftime("%d/%m/%Y")
    df_latest_sale = df_sale[df_sale['Date_Only'] == max_date]
    recent_sales_map = df_latest_sale.groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()

# ==========================================
# DIALOGS
# ==========================================

@st.dialog("📋 รายละเอียดข้อมูล", width="small")
def show_info_dialog(text_val):
    st.info("💡 สามารถกดปุ่ม Copy มุมขวาบนของกล่องข้อความได้เลย")
    st.code(text_val, language="text") 
    
    if st.button("❌ ปิดหน้าต่าง", type="primary", use_container_width=True):
        if "view_info" in st.query_params: del st.query_params["view_info"]
        if "t" in st.query_params: del st.query_params["t"]
        if "token" not in st.query_params and st.session_state.logged_in:
             st.query_params["token"] = create_token(st.session_state.user_email)
        st.rerun()

@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    st.markdown("""
    <style>
        div[data-testid="stDialog"] { width: 98vw !important; min-width: 98vw !important; max-width: 98vw !important; left: 1vw !important; margin: 0 !important; }
        div[data-testid="stDialog"] > div { width: 100% !important; max-width: 100% !important; }
    </style>
    """, unsafe_allow_html=True)
    
    selected_pid = fixed_product_id
    if not selected_pid:
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty: return
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox("🔍 ค้นหาสินค้า", options=product_options, index=None)
        if selected_product: selected_pid = selected_product.split(" : ")[0]
    
    if selected_pid:
        if not df_po.empty:
            df_history = df_po[df_po['Product_ID'] == selected_pid].copy()
            if not df_history.empty:
                df_history['Product_ID'] = df_history['Product_ID'].astype(str)
                df_master_t = df_master.copy()
                df_master_t['Product_ID'] = df_master_t['Product_ID'].astype(str)
                cols_to_use = ['Product_ID', 'Product_Name', 'Image', 'Product_Type']
                valid_cols = [c for c in cols_to_use if c in df_master_t.columns]
                df_final = pd.merge(df_history, df_master_t[valid_cols], on='Product_ID', how='left')
                
                for col in ['Order_Date', 'Received_Date', 'Expected_Date']:
                    if col in df_final.columns:
                        df_final[col] = pd.to_datetime(df_final[col], errors='coerce')

                def get_status_hist(row):
                    qty_ord = float(row.get('Qty_Ordered', 0))
                    qty_recv = float(row.get('Qty_Received', 0))
                    if qty_recv >= qty_ord and qty_ord > 0: return "เรียบร้อย", "#d4edda", "#155724"
                    if qty_recv > 0 and qty_recv < qty_ord: return "สินค้าไม่ครบ", "#fff3cd", "#856404"
                    exp_date = row.get('Expected_Date')
                    if pd.notna(exp_date):
                        today_date = pd.Timestamp.today().normalize()
                        diff_days = (exp_date - today_date).days
                        if 0 <= diff_days <= 4: return "สินค้าใกล้ถึง", "#cce5ff", "#004085"
                    return "รอจัดส่ง", "#f8f9fa", "#333333"

                status_results = df_final.apply(get_status_hist, axis=1)
                df_final['Status_Text'] = status_results.apply(lambda x: x[0])
                df_final['Status_BG'] = status_results.apply(lambda x: x[1])
                df_final['Status_Color'] = status_results.apply(lambda x: x[2])
                df_final = df_final.sort_values(by=['Order_Date', 'PO_Number', 'Received_Date'], ascending=[False, False, True])

                st.markdown("""
                <style>
                    .po-table-container { overflow: auto; max-height: 75vh; }
                    .custom-po-table { width: 100%; border-collapse: separate; font-size: 12px; color: #e0e0e0; min-width: 2000px; }
                    .custom-po-table th { background-color: #1e3c72; color: white; padding: 10px; text-align: center; border-bottom: 2px solid #fff; border-right: 1px solid #4a4a4a; position: sticky; top: 0; z-index: 10; white-space: nowrap; vertical-align: middle; }
                    .custom-po-table td { padding: 8px 5px; border-bottom: 1px solid #111; border-right: 1px solid #444; vertical-align: middle; text-align: center; }
                    .td-merged { border-right: 2px solid #666 !important; background-color: inherit; }
                    .status-badge { padding: 4px 8px; border-radius: 12px; font-weight: bold; font-size: 12px; display: inline-block; width: 100px;}
                </style>
                """, unsafe_allow_html=True)

                table_html = """<div class="po-table-container"><table class="custom-po-table"><thead><tr>
                    <th>รหัสสินค้า</th><th>รูปสินค้า</th><th>สถานะ</th><th>เลข PO</th><th>ประเภทการนำเข้า</th>
                    <th style="background-color: #5f00bf;">วันที่สั่งซื้อ</th><th style="background-color: #5f00bf;">วันคาดการณ์</th><th style="background-color: #5f00bf;">วันที่ได้รับ</th><th style="background-color: #5f00bf;">ระยะเวลา</th><th style="background-color: #5f00bf;">จำนวนที่ได้รับ</th>
                    <th style="background-color: #00bf00;">จำนวนสั่งซื้อ</th><th style="background-color: #00bf00;">ต้นทุน/ชิ้น (฿)</th><th>ยอดเงินหยวน (¥)</th><th>ยอดเงินบาทที่ใช้ (฿)</th><th>เรทเงิน</th><th>เรทค่าขนส่ง</th><th>ขนาด (คิว)</th><th>ค่าส่ง</th><th>น้ำหนัก / KG</th><th>ราคา / ชิ้น (หยวน)</th>
                    <th style="background-color: #ff6600;">SHOPEE</th><th>LAZADA</th><th style="background-color: #000000;">TIKTOK</th><th>หมายเหตุ</th><th>ร้านค้า</th>
                </tr></thead><tbody>"""

                def fmt_num(val, decimals=2):
                    try: return f"{float(val):,.{decimals}f}"
                    except: return "0.00"
                def fmt_date(d):
                    if pd.isna(d) or str(d) == 'NaT': return "-"
                    return d.strftime("%d/%m/%Y")

                grouped = df_final.groupby(['PO_Number', 'Product_ID'], sort=False)
                for group_idx, ((po, pid), group) in enumerate(grouped):
                    row_count = len(group)
                    first_row = group.iloc[0]
                    is_internal = (str(first_row.get('Transport_Type', '')).strip() == "สินค้าภายใน")
                    total_order_qty = group['Qty_Ordered'].sum()
                    if total_order_qty == 0: total_order_qty = 1 
                    total_yuan = group['Total_Yuan'].sum()
                    total_ship_cost = group['Ship_Cost'].sum()
                    calc_total_thb_used = 0
                    if is_internal: calc_total_thb_used = group['Total_THB'].sum()
                    else:
                        for _, r in group.iterrows(): calc_total_thb_used += (float(r.get('Total_Yuan',0)) * float(r.get('Yuan_Rate',0)))
                    cost_per_unit_thb = (calc_total_thb_used + total_ship_cost) / total_order_qty if total_order_qty > 0 else 0
                    price_per_unit_yuan = total_yuan / total_order_qty if total_order_qty > 0 else 0
                    rate = float(first_row.get('Yuan_Rate', 0))
                    bg_color = "#222222" if group_idx % 2 == 0 else "#2e2e2e"
                    s_text, s_bg, s_col = first_row['Status_Text'], first_row['Status_BG'], first_row['Status_Color']

                    for idx, (i, row) in enumerate(group.iterrows()):
                        table_html += f'<tr style="background-color: {bg_color};">'
                        if idx == 0:
                            table_html += f'<td rowspan="{row_count}" class="td-merged"><b>{row["Product_ID"]}</b><br><small>{row.get("Product_Name","")[:15]}..</small></td>'
                            img_src = row.get('Image', '')
                            img_html = f'<img src="{img_src}" width="50" height="50">' if str(img_src).startswith('http') else ''
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{img_html}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged"><span class="status-badge" style="background-color:{s_bg}; color:{s_col};">{s_text}</span></td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{row["PO_Number"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Transport_Type", "-")}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_date(row["Order_Date"])}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_date(row.get("Expected_Date"))}</td>'

                        recv_d = fmt_date(row['Received_Date'])
                        table_html += f'<td>{recv_d}</td>'
                        wait_val = "-"
                        if pd.notna(row['Received_Date']) and pd.notna(row['Order_Date']):
                            wait_val = f"{(row['Received_Date'] - row['Order_Date']).days} วัน"
                        table_html += f'<td>{wait_val}</td>'
                        qty_recv = int(row.get('Qty_Received', 0))
                        q_style = "color: #ff4b4b; font-weight:bold;" if (qty_recv > 0 and qty_recv != int(row.get('Qty_Ordered', 0))) else "font-weight:bold;"
                        table_html += f'<td style="{q_style}">{qty_recv:,}</td>'

                        if idx == 0:
                            table_html += f'<td rowspan="{row_count}" class="td-merged" style="color:#AED6F1; font-weight:bold;">{int(total_order_qty):,}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(cost_per_unit_thb)}</td>'
                            val_yuan = "-" if is_internal else fmt_num(total_yuan)
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_yuan}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(calc_total_thb_used)}</td>'
                            val_rate = "-" if is_internal else fmt_num(rate)
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_rate}</td>'
                            val_ship_rate = "-" if is_internal else fmt_num(row.get("Ship_Rate",0))
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_ship_rate}</td>'
                            val_cbm = "-" if is_internal else fmt_num(row.get("CBM",0), 4)
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_cbm}</td>'
                            val_ship_cost = "-" if is_internal else fmt_num(total_ship_cost)
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_ship_cost}</td>'
                            val_weight = "-" if is_internal else fmt_num(row.get("Transport_Weight",0))
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_weight}</td>'
                            val_unit_yuan = "-" if is_internal else fmt_num(price_per_unit_yuan)
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{val_unit_yuan}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("Shopee_Price",0))}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("Lazada_Price",0))}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("TikTok_Price",0))}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Note","")}</td>'
                            
                            link_val = str(row.get("Link", "")).strip()
                            wechat_val = str(row.get("WeChat", "")).strip()
                            icons_html = []
                            import urllib.parse
                            
                            curr_token = st.query_params.get("token", "")
                            
                            if link_val and link_val.lower() not in ['nan', 'none', '']:
                                safe_link = urllib.parse.quote(link_val)
                                icons_html.append(f"""<a href="?view_info={safe_link}&token={curr_token}" target="_self" style="text-decoration:none; font-size:16px; margin-right:5px; color:#007bff;">🔗</a>""")
                            if wechat_val and wechat_val.lower() not in ['nan', 'none', '']:
                                safe_wechat = urllib.parse.quote(wechat_val)
                                icons_html.append(f"""<a href="?view_info={safe_wechat}&token={curr_token}" target="_self" style="text-decoration:none; font-size:16px; color:#25D366;">💬</a>""")
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{"".join(icons_html) if icons_html else "-"}</td>'
                        table_html += "</tr>"
                table_html += "</tbody></table></div>"
                st.markdown(table_html, unsafe_allow_html=True)
            else: st.warning("❌ ไม่พบประวัติการสั่งซื้อสำหรับสินค้านี้")
        else: st.warning("❌ ยังไม่มีข้อมูล PO ในระบบ")

@st.dialog("📝 บันทึกรับของ / แก้ไข PO", width="large")
def po_edit_dialog_v2(pre_selected_po=None, pre_selected_pid=None):
    selected_row, row_index = None, None
    po_map = {}
    po_map_key = {}
    
    # --- 1. เตรียมข้อมูลสำหรับค้นหา ---
    if not df_po.empty:
        for idx, row in df_po.iterrows():
            qty_ord = int(row.get('Qty_Ordered', 0))
            recv_date = str(row.get('Received_Date', '')).strip()
            is_received = (recv_date != '' and recv_date.lower() != 'nat')
            status_icon = "✅ รับแล้ว" if is_received else ("✅ ครบ/ปิด" if qty_ord <= 0 else "⏳ รอของ")
            display_text = f"[{status_icon}] {row.get('PO_Number','-')} : {row.get('Product_ID','-')} (สั่ง: {qty_ord})"
            
            po_map[display_text] = row
            key_id = (str(row.get('PO_Number', '')), str(row.get('Product_ID', '')))
            po_map_key[key_id] = row

    # --- 2. Logic การเลือกรายการ ---
    if pre_selected_po and pre_selected_pid:
        target_key = (str(pre_selected_po), str(pre_selected_pid))
        if target_key in po_map_key:
            selected_row = po_map_key[target_key]
            if 'Sheet_Row_Index' in selected_row: row_index = selected_row['Sheet_Row_Index']
        else:
            st.error("❌ ไม่พบรายการที่เลือก (อาจมีการเปลี่ยนแปลงข้อมูล)")

    if selected_row is None:
        st.caption("🔍 ค้นหารายการที่ต้องการแก้ไข หรือ รับของ")
        sorted_keys = sorted([k for k in po_map.keys() if isinstance(k, str)], key=lambda x: "⏳" not in x)
        search_key = st.selectbox("เลือกรายการ", options=sorted_keys, index=None, placeholder="พิมพ์เลข PO หรือ รหัสสินค้า...")
        if search_key:
            selected_row = po_map[search_key]
            if 'Sheet_Row_Index' in selected_row: row_index = selected_row['Sheet_Row_Index']
            
    st.divider()

    # --- 3. แสดง Form แก้ไขข้อมูล ---
    if selected_row is not None and row_index is not None:
        def get_val(col, default): return selected_row.get(col, default)
        
        # ดึงค่าตั้งต้น
        pid_current = str(get_val('Product_ID', '')).strip()
        po_current_num = str(get_val('PO_Number', '')).strip()
        pname = get_val('Product_Name', '')
        old_qty = int(get_val('Qty_Ordered', 1))
        
        # แสดงหัวข้อสินค้า
        with st.container(border=True):
            c_img, c_detail = st.columns([1, 4])
            img_url = get_val('Image', '')
            if not df_master.empty:
                m_row = df_master[df_master['Product_ID'] == pid_current]
                if not m_row.empty: 
                    img_url = m_row.iloc[0].get('Image', img_url)
                    pname = m_row.iloc[0].get('Product_Name', pname)
            if img_url: c_img.image(img_url, width=80)
            c_detail.markdown(f"### {pid_current}")
            c_detail.write(f"**{pname}**")

        with st.form(key="full_edit_po_form"):
            
            # ตรวจสอบประเภทขนส่งก่อน
            curr_trans = get_val('Transport_Type', 'ทางรถ')
            is_internal_check = (curr_trans == "สินค้าภายใน")

            # =========================================================
            # SECTION 1: ข้อมูลรับของ (Receiving)
            # =========================================================
            st.markdown("##### 📦 1. ข้อมูลรับของ (Receiving)")
            r1_c1, r1_c2 = st.columns(2)
            new_qty_recv = r1_c1.number_input("จำนวนที่ได้รับรอบนี้ (ชิ้น)", min_value=1, value=old_qty, key="e_qty_recv")
            
            try: d_recv_def = datetime.strptime(str(get_val('Received_Date', date.today())), "%Y-%m-%d").date()
            except: d_recv_def = date.today()
            new_recv_date = r1_c2.date_input("วันที่ได้รับของ", value=d_recv_def, key="e_recv_date")

            st.markdown("---")
            
            # =========================================================
            # SECTION 2: แก้ไขรายละเอียด PO (เพิ่ม CBM/Weight Total)
            # =========================================================
            with st.expander("📝 แก้ไขรายละเอียด PO (Header & Cost)", expanded=True):
                
                # --- Row 1: Header ---
                h1, h2, h3 = st.columns(3)
                new_po = h1.text_input("เลข PO", value=po_current_num, key="e_po")
                
                trans_opts = ["ทางรถ", "ทางเรือ", "สินค้าภายใน"]
                try: trans_idx = trans_opts.index(curr_trans)
                except: trans_idx = 0
                new_trans = h2.selectbox("ขนส่ง", trans_opts, index=trans_idx, key="e_trans")
                is_internal = (new_trans == "สินค้าภายใน") 

                try: d_ord_def = datetime.strptime(str(get_val('Order_Date', date.today())), "%Y-%m-%d").date()
                except: d_ord_def = date.today()
                new_ord_date = h3.date_input("วันที่สั่งซื้อ", value=d_ord_def, key="e_ord_date")
                
                # --- Row 2: Total Qty & Price ---
                st.markdown("**ข้อมูลยอดรวม (Total)**")
                q1, q2, q3, q4 = st.columns(4)
                new_qty_ordered = q1.number_input("จำนวนสั่งทั้งหมดใน PO", min_value=1, value=old_qty, key="e_qty_ord")
                
                new_total_yuan_full = 0.0
                new_rate = 0.0
                new_ship_rate = 0.0
                new_total_thb_full = 0.0
                
                # ตัวแปรรับค่า CBM/Weight รวม (ใส่ไว้ก่อน กัน Error)
                total_cbm_input = 0.0
                total_weight_input = 0.0

                if is_internal:
                    curr_thb_total = float(get_val('Total_THB', 0))
                    new_total_thb_full = q2.number_input("ราคาสินค้าบาท (รวม)", min_value=0.0, value=curr_thb_total, step=1.0, format="%.2f", key="e_thb_full")
                else:
                    curr_yuan_total = float(get_val('Total_Yuan', 0))
                    new_total_yuan_full = q2.number_input("ราคาหยวน (รวม)", min_value=0.0, value=curr_yuan_total, step=1.0, format="%.2f", key="e_yuan_full")
                    
                    new_rate = q3.number_input("เรทเงิน", min_value=0.0, value=float(get_val('Yuan_Rate', 5.0)), step=0.01, format="%.2f", key="e_rate")
                    new_ship_rate = q4.number_input("เรทค่าขนส่ง", min_value=0.0, value=float(get_val('Ship_Rate', 6000)), step=50.0, format="%.2f", key="e_ship_rate")

                    # --- New: Total CBM & Weight Section ---
                    st.markdown("---")
                    st.markdown('<span style="color:#ff4b4b;"><b>🚚 อัปเดต CBM/Weight รวม (ระบบจะหารเฉลี่ยให้อัตโนมัติ)</b></span>', unsafe_allow_html=True)
                    cw1, cw2 = st.columns(2)
                    
                    # ลองหาค่ารวม CBM เดิมของ PO นี้ (ถ้าหาไม่ได้ให้เป็น 0)
                    current_po_rows = df_po[df_po['PO_Number'] == po_current_num]
                    sum_cbm_existing = current_po_rows['CBM'].sum() if not current_po_rows.empty else 0.0
                    sum_weight_existing = current_po_rows['Transport_Weight'].sum() if not current_po_rows.empty else 0.0
                    
                    # ช่องกรอกยอดรวมใหม่
                    total_cbm_input = cw1.number_input("จำนวนคิวทั้งหมด (Total CBM)", min_value=0.0, value=float(sum_cbm_existing), step=0.001, format="%.4f", key="e_tot_cbm")
                    total_weight_input = cw2.number_input("จำนวนน้ำหนักทั้งหมด (Total KG)", min_value=0.0, value=float(sum_weight_existing), step=0.1, format="%.2f", key="e_tot_weight")
                    
                    apply_avg_to_all = st.checkbox(f"✅ ต้องการนำ CBM/Weight นี้ไปหารเฉลี่ยให้สินค้าทุกรายการใน PO: {po_current_num}", value=True)

                # --- Row 3: Sales & Note ---
                st.markdown("---")
                st.markdown("**ราคาขาย & อื่นๆ**")
                m1, m2, m3 = st.columns(3)
                new_shopee = m1.number_input("Shopee", value=float(get_val('Shopee_Price', 0)), key="e_shop")
                new_lazada = m2.number_input("Lazada", value=float(get_val('Lazada_Price', 0)), key="e_laz")
                new_tiktok = m3.number_input("TikTok", value=float(get_val('TikTok_Price', 0)), key="e_tik")
                
                new_note = st.text_input("หมายเหตุ", value=get_val('Note', ''), key="e_note")
                
                l1, l2 = st.columns(2)
                new_link = l1.text_input("Link", value=get_val('Link', ''), key="e_link")
                new_wechat = l2.text_input("WeChat", value=get_val('WeChat', ''), key="e_wechat")

            # --- Calculation Logic (เฉพาะแถวที่กำลังแก้) ---
            # 1. คำนวณ CBM/Weight ของ "รายการนี้" ก่อน (เพื่อใช้บันทึกเฉพาะแถวนี้)
            calc_qty_base = new_qty_ordered if new_qty_ordered > 0 else 1
            row_cbm_val = float(get_val('CBM', 0)) # ค่าเดิม
            row_weight_val = float(get_val('Transport_Weight', 0)) # ค่าเดิม
            
            if not is_internal and apply_avg_to_all:
                # Logic: (Qty ของแถวนี้ / Total Qty ทั้ง PO) * ยอดรวม CBM
                total_qty_po = current_po_rows['Qty_Ordered'].sum() if not current_po_rows.empty else calc_qty_base
                if total_qty_po == 0: total_qty_po = 1
                
                ratio = new_qty_ordered / total_qty_po 
                row_cbm_val = total_cbm_input * ratio
                row_weight_val = total_weight_input * ratio
            
            # 2. คำนวณต้นทุน (Cost) ของแถวนี้
            if is_internal:
                unit_yuan = 0
                unit_thb_cost = new_total_thb_full / calc_qty_base
                final_ship_cost_row = 0
            else:
                unit_yuan = new_total_yuan_full / calc_qty_base
                final_ship_cost_row = row_cbm_val * new_ship_rate
                
                unit_ship_cost = final_ship_cost_row / new_qty_recv if new_qty_recv > 0 else 0
                unit_thb_cost = (unit_yuan * new_rate) + unit_ship_cost

            # ปุ่มบันทึก
            if st.form_submit_button("💾 บันทึกการแก้ไข / รับของ", type="primary"):
                
                rows_to_update_batch = [] # เก็บรายการที่จะ update (index, data)
                
                # === กรณีต้องการกระจายยอด CBM/Weight (Recalculate All Items in PO) ===
                if not is_internal and apply_avg_to_all and not current_po_rows.empty:
                    # 1. หา Total Qty ใหม่ของทั้ง PO
                    temp_df = current_po_rows.copy()
                    temp_df.loc[temp_df['Product_ID'] == pid_current, 'Qty_Ordered'] = new_qty_ordered
                    
                    final_total_qty_po = temp_df['Qty_Ordered'].sum()
                    if final_total_qty_po <= 0: final_total_qty_po = 1
                    
                    # คำนวณค่าเฉลี่ยต่อ 1 ชิ้น
                    avg_cbm_per_unit = total_cbm_input / final_total_qty_po
                    avg_weight_per_unit = total_weight_input / final_total_qty_po
                    
                    # Loop ทุกแถวใน PO เพื่อเตรียมข้อมูลอัปเดต
                    for i, r in temp_df.iterrows():
                        r_idx = r['Sheet_Row_Index'] # Index จริงใน Google Sheet
                        r_pid = str(r['Product_ID'])
                        
                        # แยก Case: แถวปัจจุบัน vs แถวอื่น
                        if r_pid == pid_current:
                            curr_qty = new_qty_ordered
                            curr_recv_qty = new_qty_recv
                            curr_tot_yuan = new_total_yuan_full
                        else:
                            curr_qty = r['Qty_Ordered']
                            curr_recv_qty = r['Qty_Received']
                            curr_tot_yuan = r['Total_Yuan']

                        # คำนวณ CBM/Weight ใหม่ของแถวนั้น
                        new_row_cbm = curr_qty * avg_cbm_per_unit
                        new_row_weight = curr_qty * avg_weight_per_unit
                        new_row_ship_cost = new_row_cbm * new_ship_rate
                        
                        # คำนวณ THB Total & Unit Cost ใหม่
                        if is_internal:
                            pass 
                        else:
                            # External
                            curr_thb_prod = curr_tot_yuan * new_rate
                            new_row_total_thb = curr_thb_prod + new_row_ship_cost
                            
                            new_row_unit_thb = new_row_total_thb / curr_qty if curr_qty > 0 else 0
                            new_row_unit_yuan = curr_tot_yuan / curr_qty if curr_qty > 0 else 0
                            
                            # --- FIX: จัดการเรื่องวันที่ (ป้องกัน String/Date Error) ---
                            raw_recv = r.get('Received_Date')
                            this_recv_date_str = ""
                            if pd.notna(raw_recv) and str(raw_recv).strip() != "":
                                if isinstance(raw_recv, str):
                                    this_recv_date_str = raw_recv
                                elif hasattr(raw_recv, "strftime"):
                                    this_recv_date_str = raw_recv.strftime("%Y-%m-%d")
                                else:
                                    this_recv_date_str = str(raw_recv)

                            this_wait_days = r.get('Wait_Days', 0)
                            
                            # คำนวณ Wait Days ใหม่เฉพาะถ้ามีข้อมูลครบ
                            try:
                                if r_pid == pid_current:
                                    this_recv_date_str = new_recv_date.strftime("%Y-%m-%d")
                                    if new_ord_date: this_wait_days = (new_recv_date - new_ord_date).days
                                elif pd.notna(raw_recv) and pd.notna(r.get('Order_Date')):
                                    # พยายามแปลงเป็น Datetime เพื่อคำนวณวัน
                                    d_recv = pd.to_datetime(raw_recv, errors='coerce')
                                    d_ord = pd.to_datetime(r['Order_Date'], errors='coerce')
                                    if pd.notna(d_recv) and pd.notna(d_ord):
                                        this_wait_days = (d_recv - d_ord).days
                            except:
                                pass # ถ้าคำนวณไม่ได้ ให้ใช้ค่าเดิม

                            # จัดการ Expected Date (ป้องกัน Error เหมือนกัน)
                            raw_exp = r.get('Expected_Date')
                            exp_date_str = ""
                            if pd.notna(raw_exp) and str(raw_exp).strip() != "":
                                if hasattr(raw_exp, "strftime"): exp_date_str = raw_exp.strftime("%Y-%m-%d")
                                else: exp_date_str = str(raw_exp)

                            # Construct Data List
                            data_row = [
                                r_pid, new_po, new_trans, new_ord_date.strftime("%Y-%m-%d"),
                                this_recv_date_str, this_wait_days, curr_qty, curr_recv_qty,
                                round(new_row_unit_thb, 2), round(curr_tot_yuan, 2), round(new_row_total_thb, 2),
                                new_rate, new_ship_rate, round(new_row_cbm, 4), round(new_row_ship_cost, 2), round(new_row_weight, 2), round(new_row_unit_yuan, 4),
                                new_shopee if r_pid == pid_current else r.get('Shopee_Price',0), 
                                new_lazada if r_pid == pid_current else r.get('Lazada_Price',0), 
                                new_tiktok if r_pid == pid_current else r.get('TikTok_Price',0), 
                                new_note if r_pid == pid_current else r.get('Note',''), 
                                new_link if r_pid == pid_current else r.get('Link',''), 
                                new_wechat if r_pid == pid_current else r.get('WeChat',''), 
                                exp_date_str
                            ]
                            rows_to_update_batch.append({"idx": r_idx, "data": data_row})

                # === กรณีปกติ (แก้แค่แถวเดียว หรือ Internal) ===
                else:
                    recv_ratio = new_qty_recv / calc_qty_base
                    rem_qty = new_qty_ordered - new_qty_recv
                    
                    # Data สำหรับแถวที่รับ (Received Row)
                    recv_yuan = new_total_yuan_full * recv_ratio 
                    
                    # Safely Format Expected Date
                    exp_date_val = get_val('Expected_Date', '')
                    exp_date_str = ""
                    if pd.notna(exp_date_val) and str(exp_date_val).strip() != "":
                        if hasattr(exp_date_val, "strftime"): exp_date_str = exp_date_val.strftime("%Y-%m-%d")
                        else: exp_date_str = str(exp_date_val)

                    if is_internal:
                        recv_total_thb = new_total_thb_full * recv_ratio
                        data_recv = [
                            pid_current, new_po, new_trans, new_ord_date.strftime("%Y-%m-%d"),
                            new_recv_date.strftime("%Y-%m-%d"), (new_recv_date - new_ord_date).days, new_qty_recv, new_qty_recv,
                            round(unit_thb_cost, 2), 0, round(recv_total_thb, 2),
                            0, 0, 0, 0, 0, 0,
                            new_shopee, new_lazada, new_tiktok, new_note, new_link, new_wechat, 
                            exp_date_str
                        ]
                    else:
                        recv_total_thb = (recv_yuan * new_rate) + final_ship_cost_row
                        data_recv = [
                            pid_current, new_po, new_trans, new_ord_date.strftime("%Y-%m-%d"),
                            new_recv_date.strftime("%Y-%m-%d"), (new_recv_date - new_ord_date).days, new_qty_recv, new_qty_recv,
                            round(unit_thb_cost, 2), round(recv_yuan, 2), round(recv_total_thb, 2),
                            new_rate, new_ship_rate, round(row_cbm_val, 4), round(final_ship_cost_row, 2), round(row_weight_val, 2), round(unit_yuan, 4),
                            new_shopee, new_lazada, new_tiktok, new_note, new_link, new_wechat,
                            exp_date_str
                        ]
                    
                    # กรณี Split (ถ้าของมาไม่ครบ และไม่ได้กดกระจายยอด)
                    if rem_qty > 0:
                        rem_ratio = rem_qty / calc_qty_base
                        rem_yuan = new_total_yuan_full * rem_ratio
                        rem_cbm = 0 
                        rem_total_thb = 0
                        if is_internal: rem_total_thb = new_total_thb_full * rem_ratio
                        
                        data_rem = [
                            pid_current, new_po, new_trans, new_ord_date.strftime("%Y-%m-%d"),
                            None, 0, rem_qty, 0, 
                            0, round(rem_yuan, 2), round(rem_total_thb, 2),
                            new_rate, new_ship_rate, round(rem_cbm, 4), 0, 0, 0,
                            new_shopee, new_lazada, new_tiktok, f"รอรับส่วนที่เหลือ ({rem_qty})", new_link, new_wechat,
                            exp_date_str
                        ]
                        # บันทึกแบบ Split
                        save_po_edit_split(row_index, data_rem, data_recv)
                        rows_to_update_batch = [] # Clear เพื่อไม่ให้ไปทำซ้ำข้างล่าง
                        st.success("✅ บันทึกและแยกรายการส่วนที่เหลือเรียบร้อย!")
                        st.session_state.active_dialog = None
                        st.session_state.target_edit_data = {}
                        time.sleep(1)
                        st.rerun()
                        return

                    # ถ้าไม่ Split ก็เพิ่มเข้า List เพื่อ Update ตามปกติ
                    rows_to_update_batch.append({"idx": row_index, "data": data_recv})

                # === ทำการบันทึกจริง (Loop Update) ===
                success_count = 0
                for item in rows_to_update_batch:
                    if save_po_edit_update(item["idx"], item["data"]):
                        success_count += 1
                
                if success_count > 0:
                    st.success(f"✅ บันทึกข้อมูลเรียบร้อย! (อัปเดต {success_count} รายการ)")
                    st.session_state.active_dialog = None
                    st.session_state.target_edit_data = {}
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ เกิดข้อผิดพลาดในการบันทึก")

@st.dialog("⚠️ ยืนยันการลบ", width="small")
def delete_confirm_dialog():
    st.warning(f"คุณต้องการลบรายการ PO: {st.session_state.get('target_delete_po')} ใช่หรือไม่?")
    st.caption("การลบนี้จะหายไปจาก Google Sheet ทันทีและกู้คืนไม่ได้")
    
    col1, col2 = st.columns(2)
    if col1.button("ยืนยันลบ", type="primary", use_container_width=True):
        idx_to_del = st.session_state.get("target_delete_idx")
        if idx_to_del:
            if delete_po_row_from_sheet(idx_to_del):
                st.success("ลบข้อมูลเรียบร้อย")
                st.session_state.active_dialog = None
                time.sleep(1)
                st.rerun()
    
    if col2.button("ยกเลิก", use_container_width=True):
        st.session_state.active_dialog = None
        st.rerun()
@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    # --- Function: คำนวณวันที่คาดการณ์อัตโนมัติ ---
    def auto_update_batch_date():
        t = st.session_state.get("bp_trans", "ทางรถ")
        d = st.session_state.get("bp_ord_date", date.today())
        
        days_add = 0
        if t == "ทางรถ": days_add = 14
        elif t == "ทางเรือ": days_add = 25
        
        if d:
            st.session_state.bp_expected_date = d + timedelta(days=days_add)

    # --- Reset Logic ---
    if st.session_state.get("need_reset_inputs", False):
        keys_to_reset = ["bp_sel_prod", "bp_qty", "bp_cost_yuan", "bp_cbm", "bp_weight", 
                         "bp_note", "bp_shop_s", "bp_shop_l", "bp_shop_t", "bp_expected_date", 
                         "bp_recv_date", "bp_ship_rate"]
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]
        st.session_state["need_reset_inputs"] = False
        
        # หลัง Reset ให้คำนวณวันที่ใหม่ตาม Header ปัจจุบัน
        auto_update_batch_date()

    # --- 1. Header Section ---
    with st.container(border=True):
        st.subheader("1. ข้อมูลเอกสาร (Header)")
        c1, c2, c3 = st.columns(3)
        po_number = c1.text_input("เลข PO", placeholder="XXXXX", key="bp_po_num")
        
        # เพิ่ม on_change
        transport_type = c2.selectbox(
            "การขนส่ง", 
            ["ทางรถ", "ทางเรือ", "สินค้าภายใน"], 
            key="bp_trans",
            on_change=auto_update_batch_date
        )
        
        # เพิ่ม on_change
        order_date = c3.date_input(
            "วันที่สั่งซื้อ", 
            date.today(), 
            key="bp_ord_date",
            on_change=auto_update_batch_date
        )
        
        # Set Default ถ้ายังไม่มีค่าใน Session State
        if "bp_expected_date" not in st.session_state:
            auto_update_batch_date()

    # --- 2. Item Form Section ---
    with st.container(border=True):
        st.subheader("2. รายละเอียดสินค้า")
        prod_list = []
        if not df_master.empty:
            prod_list = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        
        sel_prod = st.selectbox("เลือกสินค้า", prod_list, index=None, key="bp_sel_prod")
        
        img_url = ""
        pid = ""
        if sel_prod:
            pid = sel_prod.split(" : ")[0]
            item_data = df_master[df_master['Product_ID'] == pid]
            if not item_data.empty: img_url = item_data.iloc[0].get('Image', '')

        with st.form(key="add_item_form", clear_on_submit=False):
            col_img, col_data = st.columns([1, 4])
            with col_img:
                if img_url: st.image(img_url, width=120)
                else: st.info("No Image")

            with col_data:
                st.markdown('<span style="color:#2ecc71; font-weight:bold;">(กรอกตอนสั่งซื้อ)</span>', unsafe_allow_html=True)
                r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
                
                # ช่องวันที่คาดการณ์ (ค่าจะเปลี่ยนตาม Session State)
                expected_date = r1_c1.date_input("วันที่คาดว่าจะได้รับ", key="bp_expected_date")
                
                qty = r1_c2.number_input("จำนวนสั่งซื้อ (ชิ้น)", min_value=1, value=None, placeholder="XXXXX", key="bp_qty")
                rate_money = r1_c3.number_input("เรทเงิน", min_value=0.0, step=0.01, value=5.0, format="%.2f", key="bp_rate")
                ship_rate = r1_c4.number_input("เรทขนส่ง", min_value=0.0, step=10.0, value=None, format="%.2f", placeholder="XXXXX", key="bp_ship_rate")

                r2_c1, r2_c2 = st.columns([1, 3])
                total_yuan = r2_c1.number_input("ราคาหยวนทั้งหมด*", min_value=0.0, step=0.01, value=None, format="%.2f", placeholder="XXXXX", key="bp_cost_yuan")
                note = r2_c2.text_input("หมายเหตุ (ถ้ามี)", placeholder="XXXXX", key="bp_note")

                with st.expander("ข้อมูลเพิ่มเติม (Link / ราคาขาย)"):
                    l1, l2 = st.columns(2)
                    link_shop = l1.text_input("Link", key="bp_link")
                    wechat = l2.text_input("WeChat", key="bp_wechat")
                    p1, p2, p3 = st.columns(3)
                    p_shopee = p1.number_input("Shopee", value=None, placeholder="0.00", key="bp_shop_s")
                    p_lazada = p2.number_input("Lazada", value=None, placeholder="0.00", key="bp_shop_l")
                    p_tiktok = p3.number_input("TikTok", value=None, placeholder="0.00", key="bp_shop_t")

                st.markdown("---")
                st.markdown('<span style="color:#ff0000; font-weight:bold;">(กรอกตอนสินค้าเข้า)</span>', unsafe_allow_html=True)
                r3_c1, r3_c2, r3_c3 = st.columns(3)
                recv_date = r3_c1.date_input("วันที่ได้รับสินค้า", value=None, key="bp_recv_date")
                cbm_val = r3_c2.number_input("ขนาดคิว (คิว)", min_value=0.0, step=0.001, value=None, format="%.4f", key="bp_cbm")
                weight_val = r3_c3.number_input("น้ำหนัก", min_value=0.0, step=0.1, value=None, format="%.2f", key="bp_weight")

            if st.form_submit_button("➕ เพิ่มรายการลงตระกร้า", type="primary"):
                if not po_number or not sel_prod:
                    st.error("กรุณากรอก เลข PO และ เลือกสินค้า")
                else:
                    c_qty = qty if qty is not None else 0
                    c_total_yuan = total_yuan if total_yuan is not None else 0.0
                    c_rate = rate_money if rate_money is not None else 0.0
                    c_cbm = cbm_val if cbm_val is not None else 0.0
                    c_ship_rate = ship_rate if ship_rate is not None else 0.0
                    
                    unit_yuan = c_total_yuan / c_qty if c_qty > 0 else 0
                    total_ship_cost = c_cbm * c_ship_rate
                    total_thb = (c_total_yuan * c_rate) 
                    unit_thb_final = (total_thb + total_ship_cost) / c_qty if c_qty > 0 else 0
                    
                    wait_days = 0
                    if recv_date and order_date: wait_days = (recv_date - order_date).days

                    item = {
                        "SKU": pid, "PO": po_number, "Trans": transport_type, "Ord": str(order_date), 
                        "Exp": str(expected_date) if expected_date else "",   
                        "Recv": str(recv_date) if recv_date else "", "Wait": wait_days,
                        "Qty": int(c_qty), "UnitTHB": round(unit_thb_final, 2),
                        "TotYuan": round(c_total_yuan, 2), "TotTHB": round(total_thb, 2), 
                        "Rate": c_rate, "ShipRate": c_ship_rate, "CBM": round(c_cbm, 4), 
                        "ShipCost": round(total_ship_cost, 2), "W": weight_val if weight_val else 0, 
                        "UnitYuan": round(unit_yuan, 4), "Shopee": p_shopee if p_shopee else 0, 
                        "Laz": p_lazada if p_lazada else 0, "Tik": p_tiktok if p_tiktok else 0, 
                        "Note": note, "Link": link_shop, "WeChat": wechat
                    }
                    st.session_state.po_temp_cart.append(item)
                    st.toast(f"✅ เพิ่ม {pid} แล้ว", icon="🛒")
                    st.session_state["need_reset_inputs"] = True
                    st.rerun()

    if st.session_state.po_temp_cart:
        st.divider()
        st.write(f"🛒 ตระกร้า ({len(st.session_state.po_temp_cart)} รายการ)")
        cart_df = pd.DataFrame(st.session_state.po_temp_cart)
        st.dataframe(
            cart_df[["SKU", "Qty", "TotYuan", "Exp", "Recv"]], 
            use_container_width=True, hide_index=True,
            column_config={
                "SKU": st.column_config.TextColumn("ชื่อสินค้า"),
                "Qty": st.column_config.NumberColumn("จำนวน", format="%d"),
                "TotYuan": st.column_config.NumberColumn("ราคาหยวนทั้งหมด", format="%.2f"),
                "Exp": st.column_config.TextColumn("วันที่คาดว่าจะได้รับ"),
                "Recv": st.column_config.TextColumn("วันที่ได้รับสินค้า"),
            }
        )
        c1, c2 = st.columns([1, 4])
        if c1.button("🗑️ ล้างตระกร้า"):
            st.session_state.po_temp_cart = []
            st.rerun()
            
        if c2.button("💾 บันทึก PO ทั้งหมด", type="primary"):
            rows = []
            for i in st.session_state.po_temp_cart:
                 rows.append([
                     i["SKU"], i["PO"], i["Trans"], i["Ord"], 
                     i["Recv"], i["Wait"], i["Qty"],  
                     i["Qty"] if i["Recv"] else 0, 
                     i["UnitTHB"], i["TotYuan"], i["TotTHB"],         
                     i["Rate"], i["ShipRate"], i["CBM"], i["ShipCost"], i["W"], i["UnitYuan"], 
                     i["Shopee"], i["Laz"], i["Tik"], i["Note"], i["Link"], i["WeChat"],
                     i["Exp"] 
                 ])
            if save_po_batch_to_sheet(rows):
                st.success("✅ บันทึกสำเร็จ!")
                st.session_state.po_temp_cart = []
                if "bp_po_num" in st.session_state: del st.session_state["bp_po_num"]
                st.session_state.active_dialog = None 
                time.sleep(1)
                st.rerun()

@st.dialog("📝 บันทึก PO สินค้าภายใน (Internal)", width="large")
def po_internal_batch_dialog():
    # --- Function: คำนวณวันที่คาดการณ์อัตโนมัติ (Internal +3 วัน) ---
    def auto_update_internal_date():
        d = st.session_state.get("int_ord_date", date.today())
        if d:
            st.session_state.int_expected_date = d + timedelta(days=3) # Default 3 วันสำหรับในประเทศ

    # --- Reset Logic ---
    if st.session_state.get("need_reset_inputs_int", False):
        keys_to_reset = ["int_sel_prod", "int_qty", "int_total_thb", "int_note", 
                         "int_link", "int_contact", "int_shop_s", "int_shop_l", "int_shop_t", 
                         "int_expected_date", "int_recv_date"]
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]
        st.session_state["need_reset_inputs_int"] = False
        
        # หลัง Reset ให้คำนวณวันที่ใหม่
        auto_update_internal_date()

    # --- 1. Header Section ---
    with st.container(border=True):
        st.subheader("1. ข้อมูลเอกสาร (Header)")
        c1, c2 = st.columns(2)
        po_number = c1.text_input("เลข PO", placeholder="XXXXX", key="int_po_num")
        
        # เพิ่ม on_change
        order_date = c2.date_input(
            "วันที่สั่งซื้อ", 
            date.today(), 
            key="int_ord_date",
            on_change=auto_update_internal_date
        )
        
        # Set Default ครั้งแรก
        if "int_expected_date" not in st.session_state:
            auto_update_internal_date()

    # --- 2. Item Form Section ---
    with st.container(border=True):
        st.subheader("2. รายละเอียดสินค้า")
        prod_list = []
        if not df_master.empty:
            prod_list = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        
        sel_prod = st.selectbox("เลือกสินค้า", prod_list, index=None, key="int_sel_prod")
        
        img_url = ""
        pid = ""
        if sel_prod:
            pid = sel_prod.split(" : ")[0]
            item_data = df_master[df_master['Product_ID'] == pid]
            if not item_data.empty: img_url = item_data.iloc[0].get('Image', '')

        with st.form(key="add_item_form_internal", clear_on_submit=False):
            col_img, col_data = st.columns([1, 4])
            with col_img:
                if img_url: st.image(img_url, width=100)
                else: st.info("No Image")
            
            with col_data:
                st.markdown('<span style="color:#2ecc71; font-weight:bold;">(กรอกตอนสั่งซื้อ)</span>', unsafe_allow_html=True)
                r1_c1, r1_c2, r1_c3 = st.columns(3)
                
                # ช่องวันที่คาดการณ์ (อัปเดตตาม Session State)
                expected_date = r1_c1.date_input("วันที่คาดว่าจะได้รับ", key="int_expected_date")
                
                qty = r1_c2.number_input("จำนวนสั่งซื้อ (ชิ้น)", min_value=1, value=None, placeholder="XXXXX", key="int_qty")
                recv_date = r1_c3.date_input("วันที่ได้รับ (ถ้าได้เลย)", value=None, key="int_recv_date")
                r2_c1, r2_c2 = st.columns(2)
                total_thb = r2_c1.number_input("ราคาสินค้าที่สั่ง (บาท)", min_value=0.0, step=1.0, value=None, format="%.2f", placeholder="XXXXX", key="int_total_thb")
                note = r2_c2.text_input("หมายเหตุ (ถ้ามี)", placeholder="XXXXX", key="int_note")
                st.markdown("**ข้อมูลเพิ่มเติม (Link / ราคาขาย)**")
                r3_c1, r3_c2 = st.columns(2)
                link_shop = r3_c1.text_input("Link", key="int_link")
                contact_other = r3_c2.text_input("ช่องทางติดต่ออื่นๆ (WeChat)", key="int_contact")
                r4_c1, r4_c2, r4_c3 = st.columns(3)
                p_shopee = r4_c1.number_input("Shopee", value=None, placeholder="0.00", key="int_shop_s")
                p_lazada = r4_c2.number_input("Lazada", value=None, placeholder="0.00", key="int_shop_l")
                p_tiktok = r4_c3.number_input("TikTok", value=None, placeholder="0.00", key="int_shop_t")

            if st.form_submit_button("➕ เพิ่มรายการลงตระกร้า", type="primary"):
                if not po_number or not sel_prod:
                    st.error("กรุณากรอก เลข PO และ เลือกสินค้า")
                else:
                    c_qty = qty if qty is not None else 0
                    c_total_thb = total_thb if total_thb is not None else 0.0
                    unit_thb = c_total_thb / c_qty if c_qty > 0 else 0
                    wait_days = 0
                    if recv_date and order_date: wait_days = (recv_date - order_date).days

                    item = {
                        "SKU": pid, "PO": po_number, 
                        "Trans": "สินค้าภายใน", "Ord": str(order_date), 
                        "Exp": str(expected_date) if expected_date else "",   
                        "Recv": str(recv_date) if recv_date else "", "Wait": wait_days,
                        "Qty": int(c_qty), "UnitTHB": round(unit_thb, 2), "TotYuan": 0, "TotTHB": round(c_total_thb, 2), 
                        "Rate": 0, "ShipRate": 0, "CBM": 0, "ShipCost": 0, "W": 0, "UnitYuan": 0, 
                        "Shopee": p_shopee if p_shopee else 0, "Laz": p_lazada if p_lazada else 0, "Tik": p_tiktok if p_tiktok else 0, 
                        "Note": note, "Link": link_shop, "WeChat": contact_other
                    }
                    st.session_state.po_temp_cart.append(item)
                    st.toast(f"✅ เพิ่ม {pid} (Internal) แล้ว", icon="🛒")
                    st.session_state["need_reset_inputs_int"] = True
                    st.rerun()

    if st.session_state.po_temp_cart:
        st.divider()
        st.write(f"🛒 ตระกร้า ({len(st.session_state.po_temp_cart)} รายการ)")
        cart_df = pd.DataFrame(st.session_state.po_temp_cart)
        st.dataframe(
            cart_df[["SKU", "Qty", "TotTHB", "Trans"]], 
            use_container_width=True, hide_index=True,
            column_config={
                "SKU": st.column_config.TextColumn("ชื่อสินค้า"),
                "Qty": st.column_config.NumberColumn("จำนวน", format="%d"),
                "TotTHB": st.column_config.NumberColumn("ยอดเงินบาท", format="%.2f"),
                "Trans": st.column_config.TextColumn("ประเภท"),
            }
        )
        c1, c2 = st.columns([1, 4])
        if c1.button("🗑️ ล้างตระกร้า", key="clear_cart_int"):
            st.session_state.po_temp_cart = []
            st.rerun()
            
        if c2.button("💾 บันทึก PO ทั้งหมด", type="primary", key="save_cart_int"):
            rows = []
            for i in st.session_state.po_temp_cart:
                 rows.append([
                     i["SKU"], i["PO"], i["Trans"], i["Ord"], 
                     i["Recv"], i["Wait"], i["Qty"],  
                     i["Qty"] if i["Recv"] else 0, 
                     i["UnitTHB"], i["TotYuan"], i["TotTHB"],         
                     i["Rate"], i["ShipRate"], i["CBM"], i["ShipCost"], i["W"], i["UnitYuan"], 
                     i["Shopee"], i["Laz"], i["Tik"], i["Note"], i["Link"], i["WeChat"],
                     i["Exp"] 
                 ])
            if save_po_batch_to_sheet(rows):
                st.success("✅ บันทึกสำเร็จ!")
                st.session_state.po_temp_cart = []
                if "int_po_num" in st.session_state: del st.session_state["int_po_num"]
                st.session_state.active_dialog = None 
                time.sleep(1)
                st.rerun()

@st.dialog("📝 บันทึก PO หลายรายการ", width="large")
def po_multi_item_dialog():
    # --- Function: Auto-Calculate Expected Date ---
    def auto_update_exp_date():
        # ดึงค่าปัจจุบันจาก State
        t_type = st.session_state.mi_trans
        o_date = st.session_state.mi_ord_date
        
        days_add = 0
        if t_type == "ทางรถ": days_add = 14
        elif t_type == "ทางเรือ": days_add = 25
        
        # ถ้าเข้าเงื่อนไข ให้คำนวณและอัปเดตวันที่คาดการณ์
        if days_add > 0 and o_date:
            st.session_state.mi_exp_date = o_date + timedelta(days=days_add)

    # --- 1. Header Section ---
    with st.container(border=True):
        st.subheader("1. ข้อมูลเอกสาร (Header)")
        h1, h2, h3, h4 = st.columns(4)
        po_number = h1.text_input("เลข PO", placeholder="XXXXX", key="mi_po")
        
        # เพิ่ม on_change เพื่อเรียกฟังก์ชันคำนวณวันที่อัตโนมัติ
        transport = h2.selectbox(
            "การขนส่ง", 
            ["ทางรถ", "ทางเรือ", "สินค้าภายใน"], 
            key="mi_trans",
            on_change=auto_update_exp_date 
        )
        
        # เพิ่ม on_change กรณีเปลี่ยนวันที่สั่งซื้อ ก็ให้คำนวณใหม่ด้วย
        ord_date = h3.date_input(
            "วันที่สั่งซื้อ", 
            date.today(), 
            key="mi_ord_date",
            on_change=auto_update_exp_date
        )
        
        # Logic เริ่มต้น: ถ้าเปิดมาครั้งแรกยังไม่มีค่า ให้คำนวณ Default (ทางรถ +14) ไว้รอเลย
        if "mi_exp_date" not in st.session_state:
            st.session_state.mi_exp_date = date.today() + timedelta(days=14)

        exp_date = h4.date_input("วันที่คาดว่าจะได้รับ", key="mi_exp_date")

    # --- 2. Items Table Section ---
    with st.container(border=True):
        st.subheader("2. รายการสินค้า")
        
        # Prepare Master Data for Dropdown
        product_options = []
        if not df_master.empty:
            product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()

        # Data Editor Setup
        if "mi_items_df" not in st.session_state:
            st.session_state.mi_items_df = pd.DataFrame([{"สินค้า": None, "จำนวน": 0}])

        edited_df = st.data_editor(
            st.session_state.mi_items_df,
            column_config={
                "สินค้า": st.column_config.SelectboxColumn("เลือกสินค้า (SKU)", options=product_options, width="large", required=True),
                "จำนวน": st.column_config.NumberColumn("จำนวนสั่งซื้อ (ชิ้น)", min_value=1, step=1, required=True, width="small"),
            },
            num_rows="dynamic",
            use_container_width=True,
            key="mi_editor"
        )
        
        # Calculate Total Qty immediately for use in Section 3
        total_qty_calculated = edited_df["จำนวน"].sum()

    # --- 3. Grand Totals & Receiving Section ---
    with st.container(border=True):
        st.subheader("3. ยอดรวมทั้งหมด (Grand Totals)")
        
        # --- 3.1 Ordering Info ---
        st.markdown('<span style="color:#2ecc71; font-weight:bold;">(กรอกตอนสั่งซื้อ)</span>', unsafe_allow_html=True)
        t1, t2, t3, t4 = st.columns(4)
        
        rate_money = t1.number_input("เรทเงิน", min_value=0.0, step=0.01, value=None, placeholder="5.00", format="%.2f", key="mi_rate")
        ship_rate = t2.number_input("เรทขนส่ง", min_value=0.0, step=10.0, value=None, placeholder="6000.00", format="%.2f", key="mi_ship_rate")
        
        grand_total_yuan = t3.number_input("ราคาหยวนทั้งหมด (¥)", min_value=0.0, step=1.0, format="%.2f", key="mi_tot_yuan")
        note = t4.text_input("หมายเหตุ (Note)", key="mi_note")
        
        st.divider()

        # --- 3.2 Receiving Info ---
        st.markdown('<span style="color:#ff4b4b; font-weight:bold;">(กรอกตอนสินค้าเข้า)</span> 💡 หากกรอกจะถือว่าได้รับของแล้ว', unsafe_allow_html=True)
        r1, r2, r3 = st.columns(3)
        recv_date = r1.date_input("วันที่ได้รับสินค้า", value=None, key="mi_recv_date")
        grand_total_cbm = r2.number_input("คิวทั้งหมด (Total CBM)", min_value=0.0, step=0.001, format="%.4f", key="mi_tot_cbm")
        grand_total_weight = r3.number_input("น้ำหนักทั้งหมด (Total KG)", min_value=0.0, step=0.1, format="%.2f", key="mi_tot_weight")

        # --- Real-time Calculation Logic ---
        unit_yuan = grand_total_yuan / total_qty_calculated if total_qty_calculated > 0 else 0
        unit_cbm = grand_total_cbm / total_qty_calculated if total_qty_calculated > 0 and grand_total_cbm > 0 else 0
        unit_weight = grand_total_weight / total_qty_calculated if total_qty_calculated > 0 and grand_total_weight > 0 else 0

        # 2. Create Preview Table
        preview_data = []
        if total_qty_calculated > 0 and not edited_df.empty:
            for idx, row in edited_df.iterrows():
                if row["สินค้า"] and row["จำนวน"] > 0:
                    sku = row["สินค้า"].split(" : ")[0]
                    qty = row["จำนวน"]
                    
                    # Calculate Row Values
                    row_yuan = qty * unit_yuan
                    row_cbm = qty * unit_cbm
                    row_weight = qty * unit_weight
                    
                    preview_data.append({
                        "No.": idx + 1,
                        "SKU": sku,
                        "จำนวน": qty,
                        "รวมหยวน (¥)": round(row_yuan, 2),
                        "รวมคิว (CBM)": round(row_cbm, 4),
                        "รวมน้ำหนัก (KG)": round(row_weight, 2)
                    })
        
        # Show Summary Box
        if total_qty_calculated > 0:
            st.markdown(f"""
            <div style="background-color:#1e3c72; padding:10px; border-radius:5px; color:white; margin-top:10px;">
                <b>📊 สรุปการคำนวณเฉลี่ย:</b> จำนวนสินค้าทั้งหมด <b>{total_qty_calculated:,}</b> ชิ้น<br>
                • เฉลี่ย 1 ชิ้น = <b>{unit_yuan:,.2f}</b> หยวน<br>
                • เฉลี่ย 1 ชิ้น = <b>{unit_cbm:,.4f}</b> CBM {'(รอใส่ยอดรวม)' if unit_cbm == 0 else ''}<br>
                • เฉลี่ย 1 ชิ้น = <b>{unit_weight:,.2f}</b> KG {'(รอใส่ยอดรวม)' if unit_weight == 0 else ''}
            </div>
            """, unsafe_allow_html=True)

    # --- 4. Footer & Save ---
    with st.container(border=True):
        st.subheader("4. ข้อมูลเพิ่มเติม (ใช้ร่วมกันทุกรายการ)")
        f1, f2 = st.columns(2)
        link_shop = f1.text_input("Link Shop", key="mi_link")
        wechat = f2.text_input("WeChat / Contact", key="mi_wechat")
        
        p1, p2, p3 = st.columns(3)
        p_s = p1.number_input("Shopee Price", min_value=0.0, key="mi_p_s")
        p_l = p2.number_input("Lazada Price", min_value=0.0, key="mi_p_l")
        p_t = p3.number_input("TikTok Price", min_value=0.0, key="mi_p_t")

    st.divider()
    
    # Save Button Logic
    if st.button("💾 บันทึก PO รายการทั้งหมด", type="primary", use_container_width=True):
        if not po_number:
            st.error("❌ กรุณากรอกเลข PO")
        elif total_qty_calculated <= 0:
            st.error("❌ กรุณาเพิ่มรายการสินค้าอย่างน้อย 1 รายการ")
        else:
            c_rate_money = rate_money if rate_money is not None else 0.0
            c_ship_rate = ship_rate if ship_rate is not None else 0.0

            rows_to_save = []
            
            for item in preview_data:
                c_sku = item["SKU"]
                c_qty = item["จำนวน"]
                c_yuan_total = item["รวมหยวน (¥)"]
                c_cbm_total = item["รวมคิว (CBM)"]
                c_weight_total = item["รวมน้ำหนัก (KG)"]
                
                c_ship_cost_total = c_cbm_total * c_ship_rate
                c_thb_product_total = c_yuan_total * c_rate_money
                c_thb_final_total = c_thb_product_total + c_ship_cost_total
                
                c_unit_thb = c_thb_final_total / c_qty if c_qty > 0 else 0
                c_unit_yuan = c_yuan_total / c_qty if c_qty > 0 else 0

                final_recv_date_str = ""
                final_wait_days = 0
                final_qty_recv = 0
                
                if recv_date:
                    final_recv_date_str = recv_date.strftime("%Y-%m-%d")
                    final_qty_recv = c_qty
                    if ord_date:
                        final_wait_days = (recv_date - ord_date).days

                row_data = [
                    c_sku, po_number, transport, ord_date.strftime("%Y-%m-%d"),
                    final_recv_date_str, final_wait_days, c_qty, final_qty_recv,
                    round(c_unit_thb, 2), round(c_yuan_total, 2), round(c_thb_final_total, 2),
                    c_rate_money, c_ship_rate, round(c_cbm_total, 4), round(c_ship_cost_total, 2), round(c_weight_total, 2), round(c_unit_yuan, 4),
                    p_s, p_l, p_t, note, link_shop, wechat,
                    exp_date.strftime("%Y-%m-%d") if exp_date else ""
                ]
                rows_to_save.append(row_data)

            if save_po_batch_to_sheet(rows_to_save):
                st.success(f"✅ บันทึก {len(rows_to_save)} รายการเรียบร้อยแล้ว!")
                if "mi_items_df" in st.session_state: del st.session_state.mi_items_df
                if "mi_exp_date" in st.session_state: del st.session_state.mi_exp_date # Clear date state
                time.sleep(1.5)
                st.session_state.active_dialog = None
                st.rerun()


# ==========================================
# 6. NAVIGATION & LOGIC
# ==========================================

# --- FIX: ย้ายการเช็ค Edit Params มาไว้ตรงนี้ (ก่อนสร้าง Menu Navigation) ---
# เพื่อให้เมื่อ Reload หน้าแล้ว ระบบจะดักจับได้ทันทีและบังคับเข้าหน้า PO
if "edit_po" in st.query_params and "edit_pid" in st.query_params:
    p_po = st.query_params["edit_po"]
    p_pid = st.query_params["edit_pid"]
    
    # ลบ params ออกเพื่อไม่ให้วนลูป
    if "edit_po" in st.query_params: del st.query_params["edit_po"]
    if "edit_pid" in st.query_params: del st.query_params["edit_pid"]
    
    # บันทึกข้อมูลเป้าหมาย และบังคับเปลี่ยนหน้า
    st.session_state.target_edit_data = {"po": p_po, "pid": p_pid}
    st.session_state.active_dialog = "po_edit_direct"
    st.session_state.current_page = "📝 รายการสั่งซื้อ" # <--- บรรทัดสำคัญ: บังคับให้เป็นหน้านี้
    st.rerun()
if "delete_idx" in st.query_params:
    d_idx = st.query_params["delete_idx"]
    d_po = st.query_params.get("del_po", "Unknown")
    
    # เก็บค่าเข้า Session เพื่อส่งให้ Dialog
    st.session_state.target_delete_idx = d_idx
    st.session_state.target_delete_po = d_po
    
    # ล้าง Query Params เพื่อไม่ให้รีเฟรชแล้วลบซ้ำ
    del st.query_params["delete_idx"]
    if "del_po" in st.query_params: del st.query_params["del_po"]
    
    # เปิด Dialog ยืนยัน
    st.session_state.active_dialog = "delete_confirm"
    st.session_state.current_page = "📝 รายการสั่งซื้อ"
    st.rerun()
# -------------------------------------------

selected_page = st.radio(
    "", 
    options=["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"],
    index=["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"].index(st.session_state.current_page),
    horizontal=True,
    label_visibility="collapsed",
    key="nav_radio",
    on_change=lambda: st.session_state.update(current_page=st.session_state.nav_radio)
)

st.divider()

# --- Global Variables for All Pages ---
thai_months = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน", 
               "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
today = date.today()
all_years = [today.year - i for i in range(3)]

# --- Page 1 (Daily Sales) ---
if st.session_state.current_page == "📅 สรุปยอดขายรายวัน":
    st.subheader("📅 สรุปยอดขายรายวัน")
    
    if "history_pid" in st.query_params:
        hist_pid = st.query_params["history_pid"]
        del st.query_params["history_pid"] 
        show_history_dialog(fixed_product_id=hist_pid)

    def update_m_dates():
        y = st.session_state.m_y
        m_index = thai_months.index(st.session_state.m_m) + 1
        _, last_day = calendar.monthrange(y, m_index)
        st.session_state.m_d_start = date(y, m_index, 1)
        st.session_state.m_d_end = date(y, m_index, last_day)

    if "m_d_start" not in st.session_state: st.session_state.m_d_start = date(today.year, today.month, 1)
    if "m_d_end" not in st.session_state:
        _, last_day = calendar.monthrange(today.year, today.month)
        st.session_state.m_d_end = date(today.year, today.month, last_day)

    with st.container(border=True):
        st.markdown("##### 🔍 ตัวกรองข้อมูล")
        c_y, c_m, c_s, c_e = st.columns([1, 1.5, 1.5, 1.5])
        with c_y: st.selectbox("ปี", all_years, key="m_y", on_change=update_m_dates)
        with c_m: st.selectbox("เดือน", thai_months, index=today.month-1, key="m_m", on_change=update_m_dates)
        with c_s: st.date_input("วันที่เริ่มต้น", key="m_d_start")
        with c_e: st.date_input("วันที่สิ้นสุด", key="m_d_end")
        st.divider()
        col_sec_check, col_sec_date = st.columns([2, 2])
        with col_sec_check:
            st.write("") 
            use_focus_date = st.checkbox("🔎 กรองเฉพาะสินค้าที่มียอดขายในวันที่...โปรดติก ✅ และเลือกวันที่", key="use_focus_date")
        focus_date = None
        if use_focus_date:
            with col_sec_date: focus_date = st.date_input("ระบุวันที่ขาย (Focus Date):", value=today, key="filter_focus_date")
        st.divider()
        col_cat, col_sku = st.columns([1.5, 3])
        category_options = ["แสดงทั้งหมด"]
        if not df_master.empty and 'Product_Type' in df_master.columns:
            unique_types = sorted(df_master['Product_Type'].astype(str).unique().tolist())
            category_options += unique_types
        sku_options = []
        if not df_master.empty:
            sku_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        with col_cat: selected_category = st.selectbox("หมวดหมู่สินค้า", category_options, key="filter_category")
        with col_sku: selected_skus = st.multiselect("รายการที่เลือก (Choose options):", sku_options, key="filter_skus")

    start_date = st.session_state.m_d_start
    end_date = st.session_state.m_d_end
    
    if start_date and end_date:
        if start_date > end_date: st.error("⚠️ วันที่เริ่มต้นต้องมาก่อนวันที่สิ้นสุด")
        else:
            if not df_sale.empty and 'Date_Only' in df_sale.columns:
                mask_range = (df_sale['Date_Only'] >= start_date) & (df_sale['Date_Only'] <= end_date)
                df_sale_range = df_sale.loc[mask_range].copy()
                df_pivot = pd.DataFrame()
                if not df_sale_range.empty:
                    thai_abbr = ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
                    df_sale_range['Day_Col'] = df_sale_range['Order_Time'].apply(lambda x: f"{x.day} {thai_abbr[x.month]}")
                    df_sale_range['Day_Sort'] = df_sale_range['Order_Time'].dt.strftime('%Y%m%d')
                    pivot_data = df_sale_range.groupby(['Product_ID', 'Day_Col', 'Day_Sort'])['Qty_Sold'].sum().reset_index()
                    df_pivot = pivot_data.pivot(index='Product_ID', columns='Day_Col', values='Qty_Sold').fillna(0).astype(int)
                    if use_focus_date and focus_date:
                        products_sold_on_focus = df_sale[(df_sale['Date_Only'] == focus_date) & (df_sale['Qty_Sold'] > 0)]['Product_ID'].unique()
                        df_pivot = df_pivot[df_pivot.index.isin(products_sold_on_focus)]

                if not df_pivot.empty:
                    df_pivot = df_pivot.reset_index()
                    final_report = pd.merge(df_master, df_pivot, on='Product_ID', how='left')
                else: final_report = df_master.copy()
                
                day_cols = [c for c in final_report.columns if c not in df_master.columns]
                day_cols = [c for c in day_cols if isinstance(c, str) and "🔴" not in c and "หมด" not in c]

                final_report[day_cols] = final_report[day_cols].fillna(0).astype(int)
                
                if selected_category != "แสดงทั้งหมด": final_report = final_report[final_report['Product_Type'] == selected_category]
                if selected_skus:
                    selected_ids = [item.split(" : ")[0] for item in selected_skus]
                    final_report = final_report[final_report['Product_ID'].isin(selected_ids)]
                if use_focus_date and focus_date and not df_pivot.empty:
                     final_report = final_report[final_report['Product_ID'].isin(df_pivot['Product_ID'])]
                elif use_focus_date and focus_date and df_pivot.empty:
                     final_report = pd.DataFrame()

                if final_report.empty: st.warning(f"⚠️ ไม่พบข้อมูลสินค้า")
                else:
                    final_report['Total_Sales_Range'] = final_report[day_cols].sum(axis=1).astype(int)
                    stock_map = df_master.set_index('Product_ID')['Initial_Stock'].to_dict()
                    final_report['Current_Stock'] = final_report['Product_ID'].apply(lambda x: stock_map.get(x, 0) - recent_sales_map.get(x, 0)).astype(int)
                    final_report['Status'] = final_report['Current_Stock'].apply(lambda x: "🔴 หมด" if x<=0 else ("⚠️ ต่ำ" if x<10 else "🟢 ปกติ"))
                    
                    if not df_sale_range.empty:
                         pivot_data_temp = df_sale_range.groupby(['Product_ID', 'Day_Col', 'Day_Sort'])['Qty_Sold'].sum().reset_index()
                         sorted_day_cols = sorted(day_cols, key=lambda x: pivot_data_temp[pivot_data_temp['Day_Col'] == x]['Day_Sort'].values[0] if x in pivot_data_temp['Day_Col'].values else 0)
                    else: sorted_day_cols = sorted(day_cols)

                    fixed_cols = ['Product_ID', 'Image', 'Product_Name', 'Product_Type', 'Current_Stock', 'Total_Sales_Range', 'Status']
                    available_fixed = [c for c in fixed_cols if c in final_report.columns]
                    final_df = final_report[available_fixed + sorted_day_cols]
                    
                    st.divider()
                    st.markdown(f"**📊 แสดงผล:** ({len(final_df)} รายการ)")
                    
                    st.markdown("""
                    <style>
                        .daily-sales-table-wrapper { overflow: auto; width: 100%; max-height: 800px; margin-top: 10px; background: #1c1c1c; border-radius: 8px; border: 1px solid #444; }
                        .daily-sales-table { width: 100%; min-width: 1000px; border-collapse: separate; border-spacing: 0; font-family: 'Sarabun', sans-serif; font-size: 11px; color: #ddd; }
                        .daily-sales-table th, .daily-sales-table td { padding: 4px 6px; line-height: 1.2; text-align: center; border-bottom: 1px solid #333; border-right: 1px solid #333; white-space: nowrap; vertical-align: middle; }
                        .daily-sales-table thead th { position: sticky; top: 0; z-index: 100; background-color: #1e3c72 !important; color: white !important; font-weight: 700; border-bottom: 2px solid #ffffff !important; min-height: 40px; }
                        .daily-sales-table tbody tr:nth-child(even) td { background-color: #262626 !important; }
                        .daily-sales-table tbody tr:nth-child(odd) td { background-color: #1c1c1c !important; }
                        .daily-sales-table tbody tr:hover td { background-color: #333 !important; }
                        .negative-value { color: #FF0000 !important; font-weight: bold !important; }
                        .col-history { width: 50px !important; min-width: 50px !important; }
                        .col-small { width: 90px !important; min-width: 90px !important; }
                        .col-medium { width: 90px !important; min-width: 90px !important; }
                        .col-image { width: 55px !important; min-width: 55px !important; }
                        .col-name { width: 150px !important; min-width: 150px !important; text-align: left !important; }
                        a.history-link { text-decoration: none; color: white; font-size: 16px; cursor: pointer; }
                        a.history-link:hover { transform: scale(1.2); }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    curr_token = st.query_params.get("token", "")
                    
                    html_table = """
                    <div class="daily-sales-table-wrapper"><table class="daily-sales-table"><thead><tr>
                        <th class="col-history">ประวัติ</th><th class="col-small">รหัส</th><th class="col-image">รูป</th><th class="col-name">ชื่อสินค้า</th><th class="col-small">คงเหลือ</th><th class="col-medium">ยอดรวม</th><th class="col-medium">สถานะ</th>
                    """
                    for day_col in sorted_day_cols: 
                        html_table += f'<th class="col-small">{day_col}</th>'
                    html_table += "</tr></thead><tbody>"
                    
                    for idx, row in final_df.iterrows():
                        current_stock_class = "negative-value" if row['Current_Stock'] < 0 else ""
                        h_link = f"?history_pid={row['Product_ID']}&token={curr_token}"
                        
                        html_table += f'<tr><td class="col-history"><a class="history-link" href="{h_link}" target="_self">📜</a></td>'
                        html_table += f'<td class="col-small">{row["Product_ID"]}</td>'
                        if pd.notna(row.get('Image')) and str(row['Image']).startswith('http'):
                            html_table += f'<td class="col-image"><img src="{row["Image"]}" style="width: 40px; height: 40px; object-fit: cover; border-radius: 4px;"></td>'
                        else: html_table += f'<td class="col-image"></td>'
                        html_table += f'<td class="col-name">{row.get("Product_Name","")}</td><td class="col-small {current_stock_class}">{row["Current_Stock"]}</td>'
                        html_table += f'<td class="col-medium">{row["Total_Sales_Range"]}</td><td class="col-medium">{row["Status"]}</td>'
                        for day_col in sorted_day_cols:
                            day_value = row.get(day_col, 0)
                            day_class = "negative-value" if isinstance(day_value, (int, float)) and day_value < 0 else ""
                            html_table += f'<td class="col-small {day_class}">{int(day_value) if isinstance(day_value, (int, float)) else day_value}</td>'
                        html_table += '</tr>'
                    html_table += "</tbody></table></div>"
                    st.markdown(html_table, unsafe_allow_html=True)
            else: st.error("⚠️ ไม่พบข้อมูลการขาย")

# --- Page 2: Purchase Orders ---
elif st.session_state.current_page == "📝 รายการสั่งซื้อ":
    
    # [REMOVED] ตรงนี้คือโค้ดเดิมที่ผิดที่ (เอา edit_po check ออกจากตรงนี้แล้ว)
    
    if "view_info" in st.query_params:
        val_to_show = st.query_params["view_info"]
        show_info_dialog(val_to_show)

    col_head, col_action = st.columns([4, 3])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
    with col_action:
        # ปรับ columns เป็น 4 ช่อง
        b1, b2, b3, b4 = st.columns(4) 
        
        if b1.button("➕ PO สินค้านำเข้า", type="primary", use_container_width=True): 
            st.session_state.active_dialog = "po_batch"
            st.rerun()
            
        if b2.button("➕ PO หลายรายการ", type="primary", use_container_width=True):
            st.session_state.active_dialog = "po_multi_item"
            st.rerun()

        if b3.button("➕ PO ภายใน", type="secondary", use_container_width=True): 
            st.session_state.active_dialog = "po_internal"
            st.rerun()
            
        if b4.button("🔍 ค้นหา/แก้ไข", type="secondary", use_container_width=True): 
            st.session_state.active_dialog = "po_search"
            st.rerun()

    if not df_po.empty and not df_master.empty:
        # ==================================================================================
        # ✅ [STEP 1] เตรียมข้อมูลก่อน (Merge Data First)
        # ต้องรวมข้อมูลก่อน เพื่อเอาชื่อสินค้าและ SKU มาสร้างเป็นตัวเลือกในกล่องค้นหา
        # ==================================================================================
        df_po_filter = df_po.copy()
        
        # แปลงวันที่ให้ถูกต้อง
        if 'Order_Date' in df_po_filter.columns: df_po_filter['Order_Date'] = pd.to_datetime(df_po_filter['Order_Date'], errors='coerce')
        if 'Received_Date' in df_po_filter.columns: df_po_filter['Received_Date'] = pd.to_datetime(df_po_filter['Received_Date'], errors='coerce')
        if 'Expected_Date' in df_po_filter.columns: df_po_filter['Expected_Date'] = pd.to_datetime(df_po_filter['Expected_Date'], errors='coerce')
        df_po_filter['Product_ID'] = df_po_filter['Product_ID'].astype(str)

        # Merge กับ Master Data
        df_display = pd.merge(df_po_filter, df_master[['Product_ID','Product_Name','Image','Product_Type']], on='Product_ID', how='left')

        # ✅ สร้างคอลัมน์ "ตัวเลือกค้นหา" (Search Label) : PO + SKU + ชื่อสินค้า
        df_display['Search_Label'] = df_display.apply(
            lambda x: f"{x['PO_Number']} : {x['Product_ID']} {str(x['Product_Name'])}", axis=1
        )
        
        # ดึงรายชื่อทั้งหมดมาทำเป็นตัวเลือก (เรียงลำดับล่าสุดก่อน)
        search_options = sorted(df_display['Search_Label'].unique().tolist(), reverse=True)

        # ==================================================================================
        # ✅ [STEP 2] แสดงตัวกรอง (UI Filters)
        # ==================================================================================
        with st.container(border=True):
            st.markdown("##### 🔍 ตัวกรองและค้นหา")
            c_search, c_status, c_cat = st.columns([2, 1.5, 1.5])
            
            with c_search:
                # 👉 เปลี่ยนจาก text_input เป็น multiselect ตามที่คุณต้องการ
                sel_search_items = st.multiselect(
                    "🔍 ค้นหา (เลือกจากรายการ)", 
                    options=search_options,
                    placeholder="พิมพ์เลข PO, SKU หรือชื่อสินค้า..."
                )
                
            with c_status:
                sel_status = st.selectbox("สถานะ:", ["ทั้งหมด", "สินค้าใกล้ถึง", "รอจัดส่ง", "สินค้าไม่ครบ", "เรียบร้อย"])
            with c_cat:
                all_types = ["แสดงทั้งหมด"]
                if not df_master.empty and 'Product_Type' in df_master.columns:
                    all_types += sorted(df_master['Product_Type'].astype(str).unique().tolist())
                sel_cat_po = st.selectbox("หมวดหมู่สินค้า", all_types, key="po_cat_filter")
            
            c_check, c_d1, c_d2 = st.columns([1, 1.5, 1.5])
            with c_check:
                use_date_filter = st.checkbox("📅 กรองตามวันที่", value=False)
            with c_d1:
                d_start = st.date_input("ตั้งแต่", value=date.today().replace(day=1), disabled=not use_date_filter)
            with c_d2:
                d_end = st.date_input("ถึง", value=date.today(), disabled=not use_date_filter)

        # ==================================================================================
        # ✅ [STEP 3] กรองข้อมูลตามที่เลือก (Filtering Logic)
        # ==================================================================================
        
        # 1. กรองตาม Search Box (Dropdown)
        if sel_search_items:
            df_display = df_display[df_display['Search_Label'].isin(sel_search_items)]

        # 2. กรองตามวันที่ (ถ้าติ๊ก)
        if use_date_filter:
            mask_date = (df_display['Order_Date'].dt.date >= d_start) & (df_display['Order_Date'].dt.date <= d_end)
            df_display = df_display[mask_date]
            
        # 3. กรองตามหมวดหมู่
        if sel_cat_po != "แสดงทั้งหมด":
            df_display = df_display[df_display['Product_Type'] == sel_cat_po]

        def get_status(row):
            qty_ord = float(row.get('Qty_Ordered', 0))
            qty_recv = float(row.get('Qty_Received', 0))
            if qty_recv >= qty_ord and qty_ord > 0:
                return "เรียบร้อย", "#d4edda", "#155724" 
            if qty_recv > 0 and qty_recv < qty_ord:
                return "สินค้าไม่ครบ", "#fff3cd", "#856404" 
            exp_date = row.get('Expected_Date')
            if pd.notna(exp_date):
                today_date = pd.Timestamp.today().normalize()
                diff_days = (exp_date - today_date).days
                if 0 <= diff_days <= 4:
                    return "สินค้าใกล้ถึง", "#cce5ff", "#004085" 
            return "รอจัดส่ง", "#f8f9fa", "#333333" 

        status_results = df_display.apply(get_status, axis=1)
        df_display['Status_Text'] = status_results.apply(lambda x: x[0])
        df_display['Status_BG'] = status_results.apply(lambda x: x[1])
        df_display['Status_Color'] = status_results.apply(lambda x: x[2])

        if sel_status != "ทั้งหมด":
            df_display = df_display[df_display['Status_Text'] == sel_status]

        df_display = df_display.sort_values(by=['Order_Date', 'PO_Number', 'Product_ID'], ascending=[False, False, False])
        
        st.markdown("""
        <style>
            .po-table-container { overflow-x: auto; box-shadow: 0 4px 6px rgba(0,0,0,0.3); margin-top: 10px; }
            .custom-po-table { width: 100%; border-collapse: separate; font-size: 13px; color: #e0e0e0; min-width: 2200px; }
            .custom-po-table th { background-color: #1e3c72; color: white; padding: 10px; text-align: center; border-bottom: 2px solid #fff; border-right: 1px solid #4a4a4a; position: sticky; top: 0; white-space: nowrap; vertical-align: middle;}
            .custom-po-table td { padding: 8px 5px; border-bottom: 1px solid #111; border-right: 1px solid #444; vertical-align: middle; text-align: center; }
            .td-merged { border-right: 2px solid #666 !important; background-color: inherit; }
            .status-badge { padding: 4px 8px; border-radius: 12px; font-weight: bold; font-size: 12px; display: inline-block; width: 120px;}
        </style>
        """, unsafe_allow_html=True)

        table_html = """
        <div class="po-table-container"><table class="custom-po-table"><thead><tr>
            <th style="width:50px;">แก้ไข</th>
            <th>รหัสสินค้า</th>
            <th>รูปสินค้า</th>
            <th>สถานะ</th>
            <th>เลข PO</th>
            <th>ประเภทการนำเข้า</th>
            <th style="background-color: #5f00bf;">วันที่สั่งซื้อ</th>
            <th style="background-color: #5f00bf;">วันคาดการณ์</th>
            <th style="background-color: #5f00bf;">วันที่ได้รับ</th>
            <th style="background-color: #5f00bf;">ระยะเวลา</th>
            <th style="background-color: #5f00bf;">จำนวนที่ได้รับ</th>
            <th style="background-color: #00bf00;">จำนวนสั่งซื้อ</th>
            <th style="background-color: #00bf00;">ต้นทุน/ชิ้น (฿)</th>
            <th>ยอดเงินหยวน (¥)</th>
            <th>ยอดเงินบาทที่ใช้ (฿)</th>
            <th>เรทเงิน</th>
            <th>เรทค่าขนส่ง</th>
            <th>ขนาด (คิว)</th>
            <th>ค่าส่ง</th>
            <th>น้ำหนัก / KG</th>
            <th>ราคา / ชิ้น (หยวน)</th>
            <th style="background-color: #ff6600;">SHOPEE</th>
            <th>LAZADA</th>
            <th style="background-color: #000000;">TIKTOK</th>
            <th>หมายเหตุ</th>
            <th>ร้านค้า</th>
        </tr></thead><tbody>"""

        def fmt_date(d): return d.strftime("%d/%m/%Y") if pd.notna(d) and str(d) != 'NaT' else "-"
        def fmt_num(val, dec=2): 
            try: return f"{float(val):,.{dec}f}"
            except: return "0.00"

        grouped = df_display.groupby(['PO_Number', 'Product_ID'], sort=False)
        
        for group_idx, ((po, pid), group) in enumerate(grouped):
            row_count = len(group)
            first_row = group.iloc[0] 
            is_internal = (str(first_row.get('Transport_Type', '')).strip() == "สินค้าภายใน")

            total_order_qty = group['Qty_Ordered'].sum()
            if total_order_qty == 0: total_order_qty = 1 
            total_yuan = group['Total_Yuan'].sum()
            total_ship_cost = group['Ship_Cost'].sum()
            calc_total_thb_used = 0
            if is_internal:
                calc_total_thb_used = group['Total_THB'].sum()
            else:
                for _, r in group.iterrows():
                    calc_total_thb_used += (float(r.get('Total_Yuan',0)) * float(r.get('Yuan_Rate',0)))

            cost_per_unit_thb = (calc_total_thb_used + total_ship_cost) / total_order_qty if total_order_qty > 0 else 0
            price_per_unit_yuan = total_yuan / total_order_qty if total_order_qty > 0 else 0
            rate = float(first_row.get('Yuan_Rate', 0))

            bg_color = "#222222" if group_idx % 2 == 0 else "#2e2e2e"
            s_text = first_row['Status_Text']
            s_bg = first_row['Status_BG']
            s_col = first_row['Status_Color']

            for idx, (i, row) in enumerate(group.iterrows()):
                table_html += f'<tr style="background-color: {bg_color};">'
                
                if idx == 0:
                    curr_token = st.query_params.get("token", "")
                    ts = int(time.time() * 1000)
                    edit_link = f"?edit_po={row['PO_Number']}&edit_pid={row['Product_ID']}&t={ts}&token={curr_token}"
                    # --- แก้ไขปุ่ม Action (Edit & Delete) ---
                    edit_btn_html = f"""<a href="{edit_link}" target="_self" style="text-decoration:none; font-size:18px; color:#ffc107; cursor:pointer; margin-right: 8px;" title="แก้ไข">✏️</a>"""
                    
                    # สร้าง Link สำหรับลบ (ส่ง Index ของแถวไป)
                    row_idx_to_delete = row.get("Sheet_Row_Index", 0)
                    delete_link = f"?delete_idx={row_idx_to_delete}&del_po={row['PO_Number']}&token={curr_token}"
                    delete_btn_html = f"""<a href="{delete_link}" target="_self" style="text-decoration:none; font-size:18px; color:#ff4b4b; cursor:pointer;" title="ลบรายการ">🗑️</a>"""
                    
                    # รวมปุ่มไว้ในช่องเดียวกัน
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{edit_btn_html}{delete_btn_html}</td>'

                    table_html += f'<td rowspan="{row_count}" class="td-merged"><b>{row["Product_ID"]}</b><br><small>{row.get("Product_Name","")[:15]}..</small></td>'
                    img_src = row.get('Image', '')
                    img_html = f'<img src="{img_src}" width="50" height="50">' if str(img_src).startswith('http') else ''
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{img_html}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged"><span class="status-badge" style="background-color:{s_bg}; color:{s_col};">{s_text}</span></td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{row["PO_Number"]}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Transport_Type", "-")}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_date(row["Order_Date"])}</td>'
                    exp_d = row.get('Expected_Date')
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_date(exp_d)}</td>'

                recv_d = fmt_date(row['Received_Date'])
                table_html += f'<td>{recv_d}</td>'
                
                wait_val = "-"
                if pd.notna(row['Received_Date']) and pd.notna(row['Order_Date']):
                    wait_val = f"{(row['Received_Date'] - row['Order_Date']).days} วัน"
                table_html += f'<td>{wait_val}</td>'

                qty_recv = int(row.get('Qty_Received', 0))
                q_style = "color: #ff4b4b; font-weight:bold;" if (qty_recv > 0 and qty_recv != int(row.get('Qty_Ordered', 0))) else "font-weight:bold;"
                table_html += f'<td style="{q_style}">{qty_recv:,}</td>'

                if idx == 0:
                    table_html += f'<td rowspan="{row_count}" class="td-merged" style="color:#AED6F1; font-weight:bold;">{int(total_order_qty):,}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(cost_per_unit_thb)}</td>'
                    val_yuan = "-" if is_internal else fmt_num(total_yuan)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_yuan}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(calc_total_thb_used)}</td>'
                    val_rate = "-" if is_internal else fmt_num(rate)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_rate}</td>'
                    val_ship_rate = "-" if is_internal else fmt_num(row.get("Ship_Rate",0))
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_ship_rate}</td>'
                    val_cbm = "-" if is_internal else fmt_num(row.get("CBM",0), 4)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_cbm}</td>'
                    val_ship_cost = "-" if is_internal else fmt_num(total_ship_cost)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_ship_cost}</td>'
                    val_weight = "-" if is_internal else fmt_num(row.get("Transport_Weight",0))
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_weight}</td>'
                    val_unit_yuan = "-" if is_internal else fmt_num(price_per_unit_yuan)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_unit_yuan}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("Shopee_Price",0))}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("Lazada_Price",0))}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("TikTok_Price",0))}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Note","")}</td>'
                    
                    link_val = str(row.get("Link", "")).strip()
                    wechat_val = str(row.get("WeChat", "")).strip()
                    
                    icons_html = []
                    import time, urllib.parse
                    ts = int(time.time() * 1000) 
                    
                    curr_token = st.query_params.get("token", "")
                    
                    if link_val and link_val.lower() not in ['nan', 'none', '']:
                        safe_link = urllib.parse.quote(link_val)
                        icons_html.append(f"""<a href="?view_info={safe_link}&t={ts}_{idx}&token={curr_token}" target="_self" style="text-decoration:none; font-size:16px; margin-right:5px; color:#007bff;">🔗</a>""")

                    if wechat_val and wechat_val.lower() not in ['nan', 'none', '']:
                        safe_wechat = urllib.parse.quote(wechat_val)
                        icons_html.append(f"""<a href="?view_info={safe_wechat}&t={ts}_{idx}&token={curr_token}" target="_self" style="text-decoration:none; font-size:16px; color:#25D366;">💬</a>""")
                    
                    final_store_html = "".join(icons_html) if icons_html else "-"
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{final_store_html}</td>'

        st.markdown(table_html, unsafe_allow_html=True)
    else: st.info("ยังไม่มีข้อมูล PO")

# --- Page 3: Stock ---
elif st.session_state.current_page == "📈 รายงาน Stock":
    st.subheader("📈 รายงาน Stock & ตั้งค่าการเตือน")
    if not df_master.empty and 'Product_ID' in df_master.columns:
        if not df_po.empty and 'Product_ID' in df_po.columns:
            df_po_latest = df_po.drop_duplicates(subset=['Product_ID'], keep='last')
            df_stock_report = pd.merge(df_master, df_po_latest, on='Product_ID', how='left')
        else:
            df_stock_report = df_master.copy()
            df_stock_report['PO_Number'] = ""
        
        total_sales_map = {}
        if not df_sale.empty and 'Product_ID' in df_sale.columns:
            total_sales_map = df_sale.groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()
        
        df_stock_report['Recent_Sold'] = df_stock_report['Product_ID'].map(recent_sales_map).fillna(0).astype(int)
        df_stock_report['Total_Sold_All'] = df_stock_report['Product_ID'].map(total_sales_map).fillna(0).astype(int)
        if 'Initial_Stock' not in df_stock_report.columns: df_stock_report['Initial_Stock'] = 0
        df_stock_report['Current_Stock'] = df_stock_report['Initial_Stock'] - df_stock_report['Recent_Sold']
        if 'Min_Limit' not in df_stock_report.columns: df_stock_report['Min_Limit'] = 10
        
        def calc_status(row):
            if row['Current_Stock'] <= 0: return "🔴 หมดเกลี้ยง"
            elif row['Current_Stock'] < row['Min_Limit']: return "⚠️ ใกล้หมด"
            return "🟢 มีของ"
        df_stock_report['Status'] = df_stock_report.apply(calc_status, axis=1)

        with st.container(border=True):
            col_filter, col_search, col_reset = st.columns([2, 2, 0.5])
            with col_filter: selected_status = st.multiselect("ตัวกรองสถานะ", options=["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"], default=[])
            with col_search: search_text = st.text_input("🔍 ค้นหา (ชื่อสินค้า / รหัส)", value="")
            with col_reset:
                if st.button("❌", use_container_width=True): st.rerun()

        edit_df = df_stock_report.copy()
        if selected_status: edit_df = edit_df[edit_df['Status'].isin(selected_status)]
        if search_text: edit_df = edit_df[edit_df['Product_Name'].str.contains(search_text, case=False) | edit_df['Product_ID'].str.contains(search_text, case=False)]

        col_ctrl1, col_ctrl2 = st.columns([3, 1])
        with col_ctrl1: st.info(f"💡 คงเหลือ = Master Stock - ขายล่าสุด ({latest_date_str})")
        with col_ctrl2: 
             if st.button("💾 บันทึกค่าจุดเตือน", type="primary", use_container_width=True):
                 if "edited_stock_data" in st.session_state:
                     update_master_limits(st.session_state.edited_stock_data)
                     st.rerun()

        final_cols = ["Product_ID", "Image", "Product_Name", "Current_Stock", "Recent_Sold", "Total_Sold_All", "PO_Number", "Status", "Min_Limit"]
        for c in final_cols:
            if c not in edit_df.columns: edit_df[c] = "" 

        st.data_editor(
            edit_df[final_cols],
            column_config={
                "Image": st.column_config.ImageColumn(width=60),
                "Product_ID": st.column_config.TextColumn(disabled=True),
                "Min_Limit": st.column_config.NumberColumn("🔔 จุดเตือน*(แก้ไขได้)", min_value=0),
            },
            height=1500, use_container_width=True, hide_index=True, key="edited_stock_data"
        )
    else: st.warning("ไม่พบข้อมูล Master Product")

# ==========================================
# EXECUTE DIALOGS
# ==========================================
if st.session_state.active_dialog == "po_batch": po_batch_dialog()
elif st.session_state.active_dialog == "po_internal": po_internal_batch_dialog()
elif st.session_state.active_dialog == "po_search": po_edit_dialog_v2()
elif st.session_state.active_dialog == "po_edit_direct":
    data = st.session_state.get("target_edit_data", {})
    po_edit_dialog_v2(pre_selected_po=data.get("po"), pre_selected_pid=data.get("pid"))
elif st.session_state.active_dialog == "history": show_history_dialog(fixed_product_id=st.session_state.get("selected_product_history"))
elif st.session_state.active_dialog == "po_multi_item": po_multi_item_dialog()
elif st.session_state.active_dialog == "delete_confirm": delete_confirm_dialog()