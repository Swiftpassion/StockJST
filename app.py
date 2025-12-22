import streamlit as st
import pandas as pd
import io
import json
import time
import calendar
from datetime import date, datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ==========================================
# 1. ตั้งค่า Page & CSS Styles
# ==========================================
st.set_page_config(page_title="JST Hybrid System", layout="wide", page_icon="📦")

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
# 3. ฟังก์ชันจัดการข้อมูล (Data Functions)
# ==========================================

def highlight_negative(val):
    if isinstance(val, (int, float)):
        if val < 0:
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
        
        # Clean Headers
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
        
        # --- Map ชื่อคอลัมน์ภาษาไทย เป็นภาษาอังกฤษ ---
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'เลข PO': 'PO_Number', 'ขนส่ง': 'Transport_Type',
            'วันที่สั่งซื้อ': 'Order_Date', 'วันที่ได้รับ': 'Received_Date', 
            'จำนวน': 'Qty_Ordered',          # ยอดสั่ง (Order)
            'จำนวนที่ได้รับ': 'Qty_Received', # ยอดรับจริง (Actual) - [NEW]
            'ราคา/ชิ้น': 'Price_Unit_NoVAT', 'ราคา (หยวน)': 'Total_Yuan', 'เรทเงิน': 'Yuan_Rate',
            'เรทค่าขนส่ง': 'Ship_Rate', 'ขนาด (คิว)': 'CBM', 'ค่าส่ง': 'Ship_Cost', 'น้ำหนัก / KG': 'Transport_Weight',
            'SHOPEE': 'Shopee_Price', 'LAZADA': 'Lazada_Price', 'TIKTOK': 'TikTok_Price', 'หมายเหตุ': 'Note',
            'ราคา (บาท)': 'Total_THB', 'Link_Shop': 'Link', 'WeChat': 'WeChat'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})

        if not df.empty:
            df['Sheet_Row_Index'] = range(2, len(df) + 2)
            # แปลงตัวเลขให้ชัวร์
            for col in ['Qty_Ordered', 'Qty_Received', 'Total_Yuan', 'Yuan_Rate']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # ถ้าไม่มีคอลัมน์ Qty_Received ให้สร้างขึ้นมาเป็น 0
            if 'Qty_Received' not in df.columns:
                df['Qty_Received'] = 0
                 
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

# --- ฟังก์ชันบันทึกแบบ Split (Update แถวเดิม + Append แถวใหม่) ---
def save_po_edit_split(row_index, current_row_data, new_row_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        # 1. Update แถวเดิม (A:W) -> แก้ไขเป็น W (23 Columns)
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_curr.append("")
            else: formatted_curr.append(item)
        
        # [FIXED] ขยาย Range เป็น W
        range_name = f"A{row_index}:W{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        # 2. Append แถวใหม่
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
        
        # จัดรูปแบบข้อมูล
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): 
                formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: 
                formatted_curr.append("")
            else: 
                formatted_curr.append(item)
        
        # [FIXED] ขยาย Range เป็น W
        range_name = f"A{row_index}:W{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Update ไม่สำเร็จ: {e}")
        return False

# --- ฟังก์ชันบันทึกแบบ Batch (สำหรับ Add New) ---
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
# 4. Main App & Data Loading (วางหลังประกาศ Function แล้ว)
# ==========================================
st.title("📊 JST Hybrid Management System")
if "active_dialog" not in st.session_state:
    st.session_state.active_dialog = None 

if "selected_product_history" not in st.session_state: st.session_state.selected_product_history = None
if 'po_temp_cart' not in st.session_state: st.session_state.po_temp_cart = []

# --- Load Data Here (To fix NameError, functions must be defined above) ---
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
# [UPDATED] Show History Dialog (Sorted by Order Date: Past -> Present)
# ==========================================
@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    # --- [CSS Hack] บังคับขยายขนาด Dialog ให้กว้างและสูงขึ้น ---
    st.markdown("""
    <style>
        div[data-testid="stDialog"] {
            width: 95vw !important; /* กว้าง 95% ของหน้าจอ */
            max-width: 95vw !important;
            min-width: 90vw !important;
        }
        div[role="dialog"] {
            width: 95vw !important;
        }
    </style>
    """, unsafe_allow_html=True)
    # -------------------------------------------------------

    selected_pid = fixed_product_id
    
    # ถ้าไม่ได้ระบุ PID มา (กดเปิดเอง) ให้เลือก Dropdown
    if not selected_pid:
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty: return
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox("🔍 ค้นหาสินค้า", options=product_options, index=None)
        if selected_product: selected_pid = selected_product.split(" : ")[0]
    
    if selected_pid:
        # 1. กรองข้อมูลเฉพาะสินค้านั้น
        if not df_po.empty:
            df_history = df_po[df_po['Product_ID'] == selected_pid].copy()
            
            if not df_history.empty:
                # 2. เตรียมข้อมูล (Merge Master, Date Convert) เหมือน Tab 2
                if 'Order_Date' in df_history.columns:
                    df_history['Order_Date'] = pd.to_datetime(df_history['Order_Date'], errors='coerce')
                if 'Received_Date' in df_history.columns:
                    df_history['Received_Date'] = pd.to_datetime(df_history['Received_Date'], errors='coerce')
                
                # Merge รูปและชื่อสินค้า
                df_history['Product_ID'] = df_history['Product_ID'].astype(str)
                df_master_t = df_master.copy()
                df_master_t['Product_ID'] = df_master_t['Product_ID'].astype(str)
                
                cols_to_use = ['Product_ID', 'Product_Name', 'Image', 'Product_Type']
                valid_cols = [c for c in cols_to_use if c in df_master_t.columns]
                df_final = pd.merge(df_history, df_master_t[valid_cols], on='Product_ID', how='left')
                
                # [แก้ไข] Sort ข้อมูล: เรียงตามวันที่สั่งซื้อ (เก่า -> ใหม่) เป็นหลัก
                df_final = df_final.sort_values(by=['Order_Date', 'PO_Number', 'Received_Date'], ascending=[True, True, True])

                # คำนวณ Wait Days
                def calc_wait(row):
                    if pd.notna(row['Received_Date']) and pd.notna(row['Order_Date']):
                        return (row['Received_Date'] - row['Order_Date']).days
                    return "-"
                df_final['Calc_Wait'] = df_final.apply(calc_wait, axis=1)

                # 3. CSS Styles (ปรับปรุงให้ Table สูงขึ้นด้วย)
                st.markdown("""
                <style>
                    /* ปรับ Container ของตารางให้สูงและ Scroll ได้ */
                    .po-table-container { 
                        overflow-x: auto; 
                        overflow-y: auto;
                        max-height: 75vh; /* ความสูงตารางสูงสุด 75% ของจอ */
                        border-radius: 8px; 
                        margin-top: 10px; 
                    }
                    .custom-po-table {
                        width: 100%; border-collapse: separate; border-spacing: 0;
                        font-family: 'Sarabun', sans-serif; font-size: 13px; color: #e0e0e0; min-width: 1500px;
                    }
                    .custom-po-table th {
                        background-color: #1e3c72; color: white; padding: 10px; text-align: center;
                        border-bottom: 2px solid #fff; border-right: 1px solid #4a4a4a;
                        position: sticky; top: 0; z-index: 10; font-weight: 600; white-space: nowrap;
                    }
                    .custom-po-table td {
                        padding: 8px 5px; border-bottom: 1px solid #111; border-right: 1px solid #444;
                        vertical-align: middle; text-align: center;
                    }
                    .td-merged { border-right: 2px solid #666 !important; }
                    .td-img img { border-radius: 4px; object-fit: cover; border: 1px solid #555; }
                    .status-waiting { color: #ffa726; font-weight: bold; }
                    .status-done { color: #66bb6a; font-weight: bold; }
                    .num-val { font-family: 'Courier New', monospace; }
                    a.table-link { text-decoration: none; font-size: 16px; }
                    a.table-link:hover { transform: scale(1.2); display:inline-block; }
                </style>
                """, unsafe_allow_html=True)

                # 4. สร้าง HTML Table
                table_html = """
                <div class="po-table-container">
                <table class="custom-po-table">
                    <thead>
                        <tr>
                            <th>รหัสสินค้า</th><th>รูปสินค้า</th><th>เลข PO</th><th>ขนส่ง</th><th>วันที่สั่งซื้อ</th>
                            <th style="background-color: #2c3e50;">วันที่ได้รับ</th>
                            <th style="background-color: #2c3e50;">ระยะเวลา</th>
                            <th style="background-color: #2c3e50;">จำนวนสั่งซื้อ</th>
                            <th style="background-color: #2c3e50;">จำนวนที่ได้รับ</th>
                            <th>ราคา/ชิ้น</th><th>ราคา (หยวน)</th><th>ราคา (บาท)</th>
                            <th>เรทเงิน</th><th>เรทค่าขนส่ง</th><th>ขนาด (คิว)</th><th>ค่าส่ง</th>
                            <th>น้ำหนัก (KG)</th><th>ราคา/ชิ้น (หยวน)</th>
                            <th>Shopee</th><th>Lazada</th><th>TikTok</th>
                            <th>หมายเหตุ</th><th>Link</th><th>WeChat</th>
                        </tr>
                    </thead>
                    <tbody>
                """

                # Helper Functions
                def fmt_num(val, decimals=2):
                    try: return f"{float(val):,.{decimals}f}"
                    except: return "0.00"

                def fmt_date(d):
                    if pd.isna(d) or str(d) == 'NaT': return "-"
                    return d.strftime("%d/%m/%Y")

                # 5. Grouping Logic
                grouped = df_final.groupby(['PO_Number', 'Product_ID'], sort=False)

                for group_idx, ((po, pid), group) in enumerate(grouped):
                    row_count = len(group)
                    total_order_qty = group['Qty_Ordered'].sum() # คำนวณยอดรวมสั่งซื้อ
                    bg_color = "#222222" if group_idx % 2 == 0 else "#2e2e2e" # สลับสี

                    for idx, (i, row) in enumerate(group.iterrows()):
                        table_html += f'<tr style="background-color: {bg_color};">'
                        
                        # --- Merged Columns (แสดงครั้งเดียว) ---
                        if idx == 0:
                            img_src = row.get('Image', '')
                            img_html = f'<img src="{img_src}" width="50" height="50">' if str(img_src).startswith('http') else ''
                            
                            try: price_unit_thb = float(row.get('Total_THB', 0)) / float(row.get('Qty_Ordered', 1)) if float(row.get('Qty_Ordered', 1)) > 0 else 0
                            except: price_unit_thb = 0
                            try: price_unit_yuan = float(row.get('Total_Yuan', 0)) / float(row.get('Qty_Ordered', 1)) if float(row.get('Qty_Ordered', 1)) > 0 else 0
                            except: price_unit_yuan = 0

                            table_html += f'<td rowspan="{row_count}" class="td-merged"><b>{row["Product_ID"]}</b><br><small>{row.get("Product_Name","")[:15]}..</small></td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged td-img">{img_html}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{row["PO_Number"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Transport_Type", "-")}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_date(row["Order_Date"])}</td>'

                        # --- Split Columns (แสดงทุกแถว) ---
                        recv_d = fmt_date(row['Received_Date'])
                        status_cls = "status-done" if recv_d != "-" else "status-waiting"
                        table_html += f'<td class="{status_cls}">{recv_d}</td>'
                        
                        wait_val = row['Calc_Wait']
                        table_html += f'<td>{f"{wait_val} วัน" if wait_val != "-" else "-"}</td>'
                        
                        # Qty Ordered (Merged Total)
                        if idx == 0:
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{int(total_order_qty):,}</td>'

                        # Qty Received
                        qty_recv = int(row.get('Qty_Received', 0))
                        qty_row_ord = int(row.get('Qty_Ordered', 0))
                        q_style = "color: #ff4b4b;" if (qty_recv > 0 and qty_recv != qty_row_ord) else ""
                        table_html += f'<td style="{q_style} font-weight:bold;">{qty_recv:,}</td>'

                        # --- Pricing Info (Merged) ---
                        if idx == 0:
                            # เตรียมค่าตัวเลข
                            vals = {
                                'yuan': fmt_num(row.get('Total_Yuan', 0)),
                                'thb': fmt_num(row.get('Total_THB', 0)),
                                'rate': fmt_num(row.get('Yuan_Rate', 0)),
                                'ship_rate': fmt_num(row.get('Ship_Rate', 0)),
                                'cbm': fmt_num(row.get('CBM', 0), 2),
                                'ship_cost': fmt_num(row.get('Ship_Cost', 0)),
                                'weight': fmt_num(row.get('Transport_Weight', 0)),
                                's': fmt_num(row.get('Shopee_Price', 0)),
                                'l': fmt_num(row.get('Lazada_Price', 0)),
                                't': fmt_num(row.get('TikTok_Price', 0)),
                                'note': row.get('Note', ''),
                                'link': row.get('Link', ''),
                                'wechat': row.get('WeChat', '')
                            }
                            
                            link_html = f'<a href="{vals["link"]}" target="_blank" class="table-link">🔗</a>' if vals['link'] else '-'
                            wechat_html = f'<a href="{vals["wechat"]}" target="_blank" class="table-link">💬</a>' if vals['wechat'] else '-'
                            
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{fmt_num(price_unit_thb)}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["yuan"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["thb"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["rate"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["ship_rate"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["cbm"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["ship_cost"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["weight"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{fmt_num(price_unit_yuan)}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["s"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["l"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{vals["t"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged" style="max-width: 150px; overflow:hidden;">{vals["note"]}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{link_html}</td>'
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{wechat_html}</td>'

                        table_html += "</tr>"

                table_html += "</tbody></table></div>"
                st.markdown(table_html, unsafe_allow_html=True)
            else:
                st.warning("❌ ไม่พบประวัติการสั่งซื้อสำหรับสินค้านี้")
        else:
            st.warning("❌ ยังไม่มีข้อมูล PO ในระบบ")

@st.dialog("📝 บันทึกรับของ / แก้ไข PO", width="large")
def po_edit_dialog_v2():
    st.caption("📦 เลือกรายการ -> ระบุจำนวนที่ได้รับจริง (ระบบจะเก็บยอดสั่ง vs ยอดรับแยกกัน)")

    selected_row = None
    row_index = None
    
    if not df_po.empty:
        po_map = {}
        for idx, row in df_po.iterrows():
            qty_ord = int(row.get('Qty_Ordered', 0))
            recv_date = str(row.get('Received_Date', '')).strip()
            is_received = (recv_date != '' and recv_date.lower() != 'nat')
            
            if is_received: status_icon = "✅ รับแล้ว" 
            elif qty_ord <= 0: status_icon = "✅ ครบ/ปิด"     
            else: status_icon = "⏳ รอของ"   

            display_text = f"[{status_icon}] {row.get('PO_Number','-')} : {row.get('Product_ID','-')} (สั่ง: {qty_ord})"
            po_map[display_text] = row
        
        sorted_keys = sorted(po_map.keys(), key=lambda x: "⏳" not in x)
        search_key = st.selectbox("🔍 ค้นหารายการ", options=sorted_keys, index=None, placeholder="พิมพ์เลข PO หรือ รหัสสินค้า...")
        
        if search_key:
            selected_row = po_map[search_key]
            if 'Sheet_Row_Index' in selected_row:
                row_index = selected_row['Sheet_Row_Index']

    st.divider()

    if selected_row is not None and row_index is not None:
        def get_val(col, default): return selected_row.get(col, default)
        
        original_qty_ordered = int(get_val('Qty_Ordered', 1))
        try: d_ord = datetime.strptime(str(get_val('Order_Date', date.today())), "%Y-%m-%d").date()
        except: d_ord = date.today()
        
        try: 
            raw_recv = str(get_val('Received_Date', ''))
            if raw_recv and raw_recv.lower() != 'nat' and raw_recv.strip() != '':
                d_recv_default = datetime.strptime(raw_recv, "%Y-%m-%d").date()
            else: d_recv_default = date.today()
        except: d_recv_default = date.today()

        with st.container(border=True):
            pid_current = str(get_val('Product_ID', '')).strip()
            current_img = get_val('Image', '')
            current_name = get_val('Product_Name', '')

            if not df_master.empty:
                match_row = df_master[df_master['Product_ID'] == pid_current]
                if not match_row.empty:
                    current_img = match_row.iloc[0].get('Image', current_img)
                    current_name = match_row.iloc[0].get('Product_Name', current_name)

            st.subheader(f"2. รายละเอียดสินค้า (ยอดสั่งซื้อ: {original_qty_ordered} ชิ้น)")
            col_img, col_info = st.columns([1, 3])
            with col_img:
                if current_img and str(current_img).startswith('http'): 
                    st.image(current_img, width=120)
                else: st.info("No Image")
            
            with col_info:
                st.markdown(f"**รหัสสินค้า:** `{pid_current}`")
                st.markdown(f"**ชื่อสินค้า:** {current_name}")

            st.divider()
            st.markdown("#### 📦 บันทึกการรับของ")
            r_col1, r_col2, r_col3 = st.columns([1.5, 1.5, 2])
            
            with r_col1:
                e_qty_received = st.number_input("จำนวนที่ได้รับจริง (ชิ้น)", min_value=1, value=original_qty_ordered, key="e_qty_recv")
            with r_col2:
                e_recv_date = st.date_input("วันที่ได้รับของ", value=d_recv_default, key="e_recv_date")
            with r_col3:
                remaining_qty = original_qty_ordered - e_qty_received
                default_note = get_val('Note', '')
                if not default_note:
                    if remaining_qty > 0: default_note = f"รับบางส่วน {e_qty_received} (ค้าง {remaining_qty})"
                    elif remaining_qty < 0: default_note = f"ได้รับเกิน {abs(remaining_qty)} ชิ้น"
                    else: default_note = "ได้รับครบ"
                e_note = st.text_input("หมายเหตุ", value=default_note, key="e_note")
            
            if remaining_qty > 0:
                st.warning(f"⚠️ สั่ง {original_qty_ordered} -> รับจริง {e_qty_received} | **ระบบจะสร้างยอดค้างส่ง {remaining_qty} ชิ้น**")
            elif remaining_qty < 0:
                st.info(f"ℹ️ สั่ง {original_qty_ordered} -> รับจริง {e_qty_received} | **ได้รับเกินมา {abs(remaining_qty)} ชิ้น**")
            else:
                st.success(f"✅ รับครบตามจำนวนสั่ง ({original_qty_ordered} ชิ้น)")

            st.divider()
            with st.expander("💰 แก้ไขต้นทุน / ราคา / ข้อมูลอื่นๆ (กดเพื่อเปิด)"):
                r2c1, r2c2, r2c3 = st.columns(3)
                e_yuan = r2c1.number_input("ราคารวม (หยวน)", min_value=0.0, value=float(get_val('Total_Yuan', 0)), step=0.01, key="e_yuan")
                e_rate = r2c2.number_input("เรทเงิน", min_value=0.0, value=float(get_val('Yuan_Rate', 5.0)), step=0.01, key="e_rate")
                
                cbm_val = float(get_val('CBM', 0))
                suggested_cbm = (cbm_val / original_qty_ordered) * e_qty_received if original_qty_ordered > 0 else cbm_val
                
                m1, m2 = st.columns(2)
                e_cbm = m1.number_input(f"CBM (ของยอด {e_qty_received} ชิ้น)", min_value=0.0, value=float(suggested_cbm), step=0.001, format="%.4f", key="e_cbm")
                e_ship_rate = m2.number_input("เรทขนส่ง", min_value=0.0, value=float(get_val('Ship_Rate', 5000)), step=100.0, key="e_ship_rate")
                e_weight = st.number_input("น้ำหนัก KG", min_value=0.0, value=float(get_val('Transport_Weight', 0)), step=0.1, key="e_weight")
                
                x1, x2 = st.columns(2)
                e_link = x1.text_input("Link", value=get_val('Link', ''), key="e_link")
                e_wechat = x2.text_input("WeChat", value=get_val('WeChat', ''), key="e_wechat")

        if st.button("💾 บันทึกรับของ", type="primary"):
            qty_actual = e_qty_received
            qty_target = original_qty_ordered
            
            total_yuan_original = float(get_val('Total_Yuan', 0))
            if e_yuan == total_yuan_original: 
                 yuan_received = (total_yuan_original / qty_target) * qty_actual if qty_target > 0 else 0
            else:
                 yuan_received = e_yuan

            if e_cbm == float(get_val('CBM', 0)) and qty_target > 0:
                 cbm_received = (float(get_val('CBM', 0)) / qty_target) * qty_actual
            else:
                 cbm_received = e_cbm

            total_thb_received = (yuan_received * e_rate) + (cbm_received * e_ship_rate)
            unit_cost_received = total_thb_received / qty_actual if qty_actual > 0 else 0

            qty_remaining = qty_target - qty_actual
            yuan_remaining = total_yuan_original - yuan_received
            cbm_remaining = float(get_val('CBM', 0)) - cbm_received
            if cbm_remaining < 0: cbm_remaining = 0 

            e_po = get_val('PO_Number', '')
            e_trans = get_val('Transport_Type', '')
            recv_date_str = e_recv_date.strftime("%Y-%m-%d")
            wait_days = (e_recv_date - d_ord).days

            # [STRUCT A: Remaining] (Size 23 for A:W)
            data_remaining_update = [
                get_val('Product_ID', ''), e_po, e_trans, d_ord.strftime("%Y-%m-%d"), 
                None, # Recv Date
                0,    # Wait
                qty_remaining, # Qty Order (Left)
                0,             # Qty Recv
                0, round(yuan_remaining, 2), 0,
                e_rate, e_ship_rate, round(cbm_remaining, 4), 0, e_weight, 
                0, get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), 
                f"รอรับส่วนที่เหลือ ({qty_remaining})", e_link, e_wechat
            ]

            # [STRUCT B: Received] (Size 23 for A:W)
            data_received_log = [
                get_val('Product_ID', ''), e_po, e_trans, d_ord.strftime("%Y-%m-%d"), 
                recv_date_str, 
                wait_days,
                qty_actual,    # Qty Order (Target)
                qty_actual,    # Qty Recv (Actual)
                unit_cost_received,
                round(yuan_received, 2),
                round(total_thb_received, 2),
                e_rate, e_ship_rate, round(cbm_received, 4), round(cbm_received*e_ship_rate, 2), e_weight,
                round(yuan_received/qty_actual, 4) if qty_actual else 0,
                get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), 
                e_note, e_link, e_wechat
            ]

            if qty_remaining > 0:
                success = save_po_edit_split(row_index, data_remaining_update, data_received_log)
                msg = f"✅ บันทึกรับของ {qty_actual} ชิ้น (เหลือค้าง {qty_remaining})"
            else:
                if qty_remaining < 0: data_received_log[6] = qty_target 
                success = save_po_edit_update(row_index, data_received_log)
                msg = f"✅ บันทึกรับของเรียบร้อย ({qty_actual} ชิ้น)"

            if success:
                st.success(msg)
                st.session_state.active_dialog = None
                time.sleep(1)
                st.rerun()
    else:
        st.info("👈 กรุณาเลือกรายการที่ต้องการรับของจากด้านบน")

@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    st.caption("💡 กรอกข้อมูลสินค้า -> กดเพิ่มลงตระกร้า -> กดบันทึก (รายการจะถูกบันทึกเป็น 'รอรับของ')")

    # --- 0. ส่วนจัดการ Reset ค่า ---
    if st.session_state.get("need_reset_inputs", False):
        keys_to_reset = ["bp_sel_prod", "bp_qty", "bp_cost_yuan", "bp_cbm", "bp_weight", "bp_note", "bp_shop_s", "bp_shop_l", "bp_shop_t"]
        for key in keys_to_reset:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state["need_reset_inputs"] = False

    # --- 1. Header (ข้อมูลเอกสาร) ---
    with st.container(border=True):
        st.subheader("1. ข้อมูลเอกสาร (Header)")
        c1, c2, c3 = st.columns(3)
        po_number = c1.text_input("เลข PO", placeholder="PO-XXXX", key="bp_po_num")
        transport_type = c2.selectbox("การขนส่ง", ["ทางรถ🚚", "ทางเรือ🚤", "ทางอากาศ✈️"], key="bp_trans")
        order_date = c3.date_input("วันที่สั่งซื้อ", date.today(), key="bp_ord_date")

    # --- 2. ข้อมูลสินค้า ---
    with st.container(border=True):
        st.subheader("2. รายละเอียดสินค้า")
        prod_list = []
        if not df_master.empty:
            prod_list = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        
        sel_prod = st.selectbox("เลือกสินค้า", prod_list, index=None, key="bp_sel_prod")
        
        pid = ""
        img_url = ""
        if sel_prod:
            pid = sel_prod.split(" : ")[0]
            item_data = df_master[df_master['Product_ID'] == pid]
            if not item_data.empty: img_url = item_data.iloc[0].get('Image', '')

        col_img, col_input = st.columns([1, 3])
        with col_img:
            if img_url: st.image(img_url, width=120)
            else: st.markdown('<div style="background:#333;height:120px;border-radius:8px;"></div>', unsafe_allow_html=True)
        
        with col_input:
            # ใช้ value=None เพื่อให้ช่องว่าง ไม่ต้องลบเลข 0
            r1c1, r1c2, r1c3 = st.columns(3)
            total_qty = r1c1.number_input("จำนวนสั่งซื้อ (ชิ้น)", min_value=1, value=None, placeholder="0", key="bp_qty")
            cost_yuan = r1c2.number_input("ต้นทุนสินค้า (หยวน)", min_value=0.0, step=0.01, value=None, format="%.2f", placeholder="0.00", key="bp_cost_yuan")
            rate_money = r1c3.number_input("เรทเงิน (หยวน)", min_value=0.0, step=0.01, value=5.0, format="%.2f", key="bp_rate") # เรทเงินมักจะคงที่ ใส่ 5.0 ไว้ช่วยอำนวยความสะดวก

            r2c1, r2c2, r2c3 = st.columns(3)
            # CBM ขอคง 4 ตำแหน่งไว้เผื่อคำนวณละเอียด แต่ถ้าต้องการ 2 จริงๆ แก้ format="%.2f" ได้เลย
            cbm_val = r2c1.number_input("ขนาด (คิว) ", min_value=0.0, step=0.0001, value=None, format="%.4f", placeholder="0.0000", key="bp_cbm")
            ship_rate = r2c2.number_input("เรทขนส่ง", min_value=0.0, step=10.0, value=None, format="%.2f", placeholder="0.00", key="bp_ship_rate")
            weight_val = r2c3.number_input("น้ำหนัก (KG)", min_value=0.0, step=0.1, value=None, format="%.2f", placeholder="0.00", key="bp_weight")
            
            is_cbm_per_piece = st.checkbox("ขนาด(คิว) 'ต่อชิ้น' (ไม่ติ๊ก=รวม)", value=False)
            st.markdown("---")
            po_note = st.text_input("หมายเหตุ (Note)", placeholder="ระบุรายละเอียดเพิ่มเติม (ถ้ามี)", key="bp_note")

            with st.expander("ข้อมูลเพิ่มเติม (Link / ราคาขาย)"):
                x1, x2 = st.columns(2)
                link_shop = x1.text_input("Link", key="bp_link")
                wechat = x2.text_input("WeChat", key="bp_wechat")
                m1, m2, m3 = st.columns(3)
                p_shopee = m1.number_input("Shopee", value=None, placeholder="0.00", key="bp_shop_s")
                p_lazada = m2.number_input("Lazada", value=None, placeholder="0.00", key="bp_shop_l")
                p_tiktok = m3.number_input("TikTok", value=None, placeholder="0.00", key="bp_shop_t")

    st.divider()
    # ปุ่มจะกดได้เมื่อมีเลข PO และ เลือกสินค้าแล้ว
    btn_disabled = (not po_number) or (not sel_prod)

    if st.button("➕ เพิ่มรายการลงตระกร้า", type="primary", disabled=btn_disabled):
        # 1. จัดการค่า None ให้เป็น 0 เพื่อคำนวณ (Safety Check)
        c_qty = total_qty if total_qty is not None else 0
        c_cost_yuan = cost_yuan if cost_yuan is not None else 0.0
        c_rate = rate_money if rate_money is not None else 0.0
        c_cbm = cbm_val if cbm_val is not None else 0.0
        c_ship_rate = ship_rate if ship_rate is not None else 0.0
        c_weight = weight_val if weight_val is not None else 0.0
        
        # 2. คำนวณ
        unit_yuan = c_cost_yuan / c_qty if c_qty > 0 else 0
        
        if is_cbm_per_piece:
            total_cbm = c_cbm * c_qty
        else:
            total_cbm = c_cbm
        
        total_ship_cost = total_cbm * c_ship_rate
        total_thb = (c_cost_yuan * c_rate) 
        unit_thb_final = ((total_thb) + total_ship_cost) / c_qty if c_qty > 0 else 0

        # ราคาขาย (จัดการ None)
        s_price = p_shopee if p_shopee is not None else 0
        l_price = p_lazada if p_lazada is not None else 0
        t_price = p_tiktok if p_tiktok is not None else 0

        item = {
            "SKU": pid, "PO": po_number, "Trans": transport_type,
            "Ord": str(order_date), "Recv": "", "Wait": 0,
            "Qty": int(c_qty), 
            "UnitTHB": round(unit_thb_final, 2),
            "TotYuan": round(c_cost_yuan, 2), 
            "TotTHB": round(total_thb, 2), 
            "Rate": c_rate, 
            "ShipRate": c_ship_rate,
            "CBM": round(total_cbm, 4), 
            "ShipCost": round(total_ship_cost, 2), 
            "W": c_weight, 
            "UnitYuan": round(unit_yuan, 4), 
            "Shopee": s_price, "Laz": l_price, "Tik": t_price, 
            "Note": po_note, "Link": link_shop, "WeChat": wechat
        }
        st.session_state.po_temp_cart.append(item)
        st.toast(f"✅ เพิ่ม {pid} ลงตระกร้าแล้ว", icon="🛒")
        
        # เปิด Flag Reset ค่า
        st.session_state["need_reset_inputs"] = True
        st.rerun()

    # --- 3. ตระกร้า ---
    if st.session_state.po_temp_cart:
        st.divider()
        st.write(f"🛒 ตระกร้า ({len(st.session_state.po_temp_cart)} รายการ)")
        
        # สร้าง DataFrame แสดงผล
        cart_df = pd.DataFrame(st.session_state.po_temp_cart)
        
        # กำหนด Column Config เพื่อบังคับทศนิยม 2 ตำแหน่ง
        st.dataframe(
            cart_df[["SKU", "Qty", "TotYuan", "UnitTHB", "Note"]], 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "TotYuan": st.column_config.NumberColumn("รวม (หยวน)", format="%.2f"),
                "UnitTHB": st.column_config.NumberColumn("ต้นทุน/ชิ้น (บาท)", format="%.2f"),
                "Qty": st.column_config.NumberColumn("จำนวน", format="%d"),
            }
        )
        
        c1, c2 = st.columns([1, 4])
        if c1.button("🗑️ ล้างตระกร้า"):
            st.session_state.po_temp_cart = []
            st.rerun()
            
        if c2.button("💾 บันทึก PO ทั้งหมด", type="primary"):
            rows_to_save = []
            for i in st.session_state.po_temp_cart:
                 row = [
                     i["SKU"], i["PO"], i["Trans"], i["Ord"], 
                     i["Recv"], i["Wait"], 
                     i["Qty"],  
                     0,         
                     0,         
                     i["TotYuan"], 
                     0,         
                     i["Rate"], i["ShipRate"], i["CBM"], i["ShipCost"], i["W"], 
                     i["UnitYuan"], 
                     i["Shopee"], i["Laz"], i["Tik"], 
                     i["Note"], i["Link"], i["WeChat"]
                 ]
                 rows_to_save.append(row)

            if save_po_batch_to_sheet(rows_to_save):
                st.success("✅ เปิด PO เรียบร้อย!")
                st.session_state.po_temp_cart = []
                if "bp_po_num" in st.session_state: del st.session_state["bp_po_num"]
                st.session_state.active_dialog = None 
                time.sleep(1)
                st.rerun()

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])
dialog_data = None

# ==========================================
# TAB 1: Daily Sales Report
# ==========================================
with tab1:
    st.subheader("📅 สรุปยอดขายรายวัน")
    if "history_pid" in st.query_params:
        hist_pid = st.query_params["history_pid"]
        st.query_params.clear() 
        show_history_dialog(fixed_product_id=hist_pid)

    thai_months = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน", 
                   "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
    today = date.today()
    all_years = [today.year - i for i in range(3)]

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
                    
                    html_table = """
                    <div class="daily-sales-table-wrapper"><table class="daily-sales-table"><thead><tr>
                        <th class="col-history">ประวัติ</th><th class="col-small">รหัส</th><th class="col-image">รูป</th><th class="col-name">ชื่อสินค้า</th><th class="col-small">คงเหลือ</th><th class="col-medium">ยอดรวม</th><th class="col-medium">สถานะ</th>
                    """
                    for day_col in sorted_day_cols: html_table += f'<th class="col-small">{day_col}</th>'
                    html_table += "</tr></thead><tbody>"
                    
                    for idx, row in final_df.iterrows():
                        current_stock_class = "negative-value" if row['Current_Stock'] < 0 else ""
                        html_table += f'<tr><td class="col-history"><a class="history-link" href="?history_pid={row["Product_ID"]}" target="_self">📜</a></td>'
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

# ==========================================
# TAB 2: Purchase Orders (Sorted by Order Date)
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า (Custom Table)")
    with col_action:
        b1, b2 = st.columns(2)
        with b1:
            if st.button("➕ เพิ่ม PO ใหม่", type="primary", key="btn_add_po_tab2"): 
                st.session_state.active_dialog = "po_batch"
                st.rerun()
        with b2:
            if st.button("🔍 ค้นหา & แก้ไข", type="secondary", key="btn_search_po_tab2"): 
                st.session_state.active_dialog = "po_search"
                st.rerun()

    if not df_po.empty and not df_master.empty:
        # --- 1. เตรียมข้อมูล ---
        df_po_filter = df_po.copy()
        
        if 'Order_Date' in df_po_filter.columns:
            df_po_filter['Order_Date'] = pd.to_datetime(df_po_filter['Order_Date'], errors='coerce')
        if 'Received_Date' in df_po_filter.columns:
            df_po_filter['Received_Date'] = pd.to_datetime(df_po_filter['Received_Date'], errors='coerce')

        df_po_filter['Product_ID'] = df_po_filter['Product_ID'].astype(str)
        df_master['Product_ID'] = df_master['Product_ID'].astype(str)
        
        cols_to_use = ['Product_ID', 'Product_Name', 'Image', 'Product_Type']
        valid_cols = [c for c in cols_to_use if c in df_master.columns]
        df_display = pd.merge(df_po_filter, df_master[valid_cols], on='Product_ID', how='left')

        # --- 2. ส่วนตัวกรอง (Filters) ---
        with st.container(border=True):
            st.markdown("##### 🔍 ตัวกรองรายการสั่งซื้อ")
            
            def update_po_dates():
                y = st.session_state.po_y
                m_index = thai_months.index(st.session_state.po_m) + 1
                _, last_day = calendar.monthrange(y, m_index)
                st.session_state.po_d_start = date(y, m_index, 1)
                st.session_state.po_d_end = date(y, m_index, last_day)

            if "po_d_start" not in st.session_state: st.session_state.po_d_start = date(today.year, today.month, 1)
            if "po_d_end" not in st.session_state: 
                _, last_day = calendar.monthrange(today.year, today.month)
                st.session_state.po_d_end = date(today.year, today.month, last_day)

            c1, c2, c3, c4 = st.columns([1, 1.5, 1.5, 1.5])
            with c1: st.selectbox("ปี", all_years, key="po_y", on_change=update_po_dates)
            with c2: st.selectbox("เดือน", thai_months, index=today.month-1, key="po_m", on_change=update_po_dates)
            with c3: st.date_input("วันที่เริ่มต้น", key="po_d_start")
            with c4: st.date_input("วันที่สิ้นสุด", key="po_d_end")

            st.divider()
            
            f_col1, f_col2, f_col3 = st.columns([2, 2, 3])
            with f_col1:
                sel_status = st.radio("สถานะการรับของ:", ["ทั้งหมด", "รอจัดส่งสินค้า", "ได้รับสินค้าครบแล้ว"], horizontal=True, index=0)
            
            with f_col2:
                cat_opts = ["แสดงทั้งหมด"] + sorted(df_display['Product_Type'].astype(str).unique().tolist()) if 'Product_Type' in df_display.columns else ["แสดงทั้งหมด"]
                sel_cat_po = st.selectbox("หมวดหมู่สินค้า", cat_opts, key="po_cat_filter")
                
            with f_col3:
                sku_opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x.get('Product_Name', '')}", axis=1).tolist()
                sel_skus_po = st.multiselect("รายการที่เลือก:", sku_opts, key="po_sku_filter")

        # --- 3. Apply Filters ---
        mask_date = (df_display['Order_Date'].dt.date >= st.session_state.po_d_start) & \
                    (df_display['Order_Date'].dt.date <= st.session_state.po_d_end)
        df_final = df_display[mask_date].copy()

        if sel_status == "รอจัดส่งสินค้า":
            df_final = df_final[df_final['Received_Date'].isna()]
        elif sel_status == "ได้รับสินค้าครบแล้ว":
            df_final = df_final[df_final['Received_Date'].notna()]

        if sel_cat_po != "แสดงทั้งหมด": df_final = df_final[df_final['Product_Type'] == sel_cat_po]
        if sel_skus_po:
            selected_ids = [s.split(" : ")[0] for s in sel_skus_po]
            df_final = df_final[df_final['Product_ID'].isin(selected_ids)]

        # --- 4. Render Custom HTML Table ---
        if not df_final.empty:
            
            # [แก้ไขจุดสำคัญ] Sort by Order Date (Past -> Present)
            # เรียง: วันที่สั่งซื้อ -> เลข PO -> รหัสสินค้า -> วันที่รับ
            df_final = df_final.sort_values(
                by=['Order_Date', 'PO_Number', 'Product_ID', 'Received_Date'], 
                ascending=[True, True, True, True]
            )

            def calc_wait(row):
                if pd.notna(row['Received_Date']) and pd.notna(row['Order_Date']):
                    return (row['Received_Date'] - row['Order_Date']).days
                return "-"
            df_final['Calc_Wait'] = df_final.apply(calc_wait, axis=1)

            st.markdown("""
            <style>
                .po-table-container {
                    overflow-x: auto;
                    border-radius: 8px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.3);
                    margin-top: 10px;
                }
                .custom-po-table {
                    width: 100%;
                    border-collapse: separate; 
                    border-spacing: 0;
                    font-family: 'Sarabun', sans-serif;
                    font-size: 13px;
                    color: #e0e0e0;
                    min-width: 1800px; 
                }
                .custom-po-table th {
                    background-color: #1e3c72;
                    color: white;
                    padding: 10px;
                    text-align: center;
                    border-bottom: 2px solid #fff;
                    border-right: 1px solid #4a4a4a;
                    position: sticky;
                    top: 0;
                    z-index: 10;
                    font-weight: 600;
                    white-space: nowrap;
                }
                .custom-po-table td {
                    padding: 8px 5px;
                    border-bottom: 1px solid #111; 
                    border-right: 1px solid #444;
                    vertical-align: middle;
                    text-align: center; 
                }
                .td-merged {
                    border-right: 2px solid #666 !important; 
                }
                .td-img img {
                    border-radius: 4px;
                    object-fit: cover;
                    border: 1px solid #555;
                }
                .status-waiting { color: #ffa726; font-weight: bold; }
                .status-done { color: #66bb6a; font-weight: bold; }
                .num-val { font-family: 'Courier New', monospace; }
                a.table-link { text-decoration: none; font-size: 16px; }
                a.table-link:hover { transform: scale(1.2); display:inline-block; }
            </style>
            """, unsafe_allow_html=True)

            table_html = """
            <div class="po-table-container">
            <table class="custom-po-table">
                <thead>
                    <tr>
                        <th>รหัสสินค้า</th>
                        <th>รูปสินค้า</th>
                        <th>เลข PO</th>
                        <th>ขนส่ง</th>
                        <th>วันที่สั่งซื้อ</th>
                        <th style="background-color: #2c3e50;">วันที่ได้รับ</th>
                        <th style="background-color: #2c3e50;">ระยะเวลา</th>
                        <th style="background-color: #2c3e50;">จำนวนสั่งซื้อ</th>
                        <th style="background-color: #2c3e50;">จำนวนที่ได้รับ</th>
                        <th>ราคา/ชิ้น</th>
                        <th>ราคา (หยวน)</th>
                        <th>ราคา (บาท)</th>
                        <th>เรทเงิน</th>
                        <th>เรทค่าขนส่ง</th>
                        <th>ขนาด (คิว)</th>
                        <th>ค่าส่ง</th>
                        <th>น้ำหนัก (KG)</th>
                        <th>ราคา/ชิ้น (หยวน)</th>
                        <th>Shopee</th>
                        <th>Lazada</th>
                        <th>TikTok</th>
                        <th>หมายเหตุ</th>
                        <th>Link</th>
                        <th>WeChat</th>
                    </tr>
                </thead>
                <tbody>
            """

            def fmt_num(val, decimals=2):
                try:
                    v = float(val)
                    return f"{v:,.{decimals}f}"
                except:
                    return "0.00"

            def fmt_date(d):
                if pd.isna(d) or str(d) == 'NaT': return "-"
                return d.strftime("%d/%m/%Y")

            # Grouping จะรักษาลำดับตามที่เรา Sort ไว้ (Order Date มาก่อน)
            grouped = df_final.groupby(['PO_Number', 'Product_ID'], sort=False)

            for group_idx, ((po, pid), group) in enumerate(grouped):
                row_count = len(group)
                
                # คำนวณยอดสั่งซื้อรวมของกลุ่มนี้
                total_order_qty = group['Qty_Ordered'].sum()

                # สลับสีพื้นหลัง
                bg_color = "#222222" if group_idx % 2 == 0 else "#2e2e2e"
                
                for idx, (i, row) in enumerate(group.iterrows()):
                    table_html += f'<tr style="background-color: {bg_color};">'
                    
                    # --- [Merged Columns] แสดงแค่รอบเดียว ---
                    if idx == 0:
                        img_src = row.get('Image', '')
                        img_html = f'<img src="{img_src}" width="50" height="50">' if str(img_src).startswith('http') else ''
                        
                        try: price_unit_thb = float(row.get('Total_THB', 0)) / float(row.get('Qty_Ordered', 1)) if float(row.get('Qty_Ordered', 1)) > 0 else 0
                        except: price_unit_thb = 0
                        try: price_unit_yuan = float(row.get('Total_Yuan', 0)) / float(row.get('Qty_Ordered', 1)) if float(row.get('Qty_Ordered', 1)) > 0 else 0
                        except: price_unit_yuan = 0

                        table_html += f'<td rowspan="{row_count}" class="td-merged"><b>{row["Product_ID"]}</b><br><small>{row.get("Product_Name","")[:15]}..</small></td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged td-img">{img_html}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged">{row["PO_Number"]}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Transport_Type", "-")}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_date(row["Order_Date"])}</td>'
                    
                    # --- [Split Columns] แสดงทุกบรรทัด ---
                    recv_d = fmt_date(row['Received_Date'])
                    status_cls = "status-done" if recv_d != "-" else "status-waiting"
                    table_html += f'<td class="{status_cls}">{recv_d}</td>'
                    
                    wait_val = row['Calc_Wait']
                    wait_show = f"{wait_val} วัน" if wait_val != "-" else "-"
                    table_html += f'<td>{wait_show}</td>'
                    
                    # --- [Qty Ordered] แสดงยอดรวม (Merged) ---
                    if idx == 0:
                        table_html += f'<td rowspan="{row_count}" class="td-merged">{int(total_order_qty):,}</td>'

                    # --- [Qty Received] แสดงแยกตามจริง ---
                    qty_recv = int(row.get('Qty_Received', 0))
                    qty_row_ord = int(row.get('Qty_Ordered', 0))
                    q_style = "color: #ff4b4b;" if (qty_recv > 0 and qty_recv != qty_row_ord) else ""
                    table_html += f'<td style="{q_style} font-weight:bold;">{qty_recv:,}</td>'

                    # --- [Pricing Info] Merged Columns ---
                    if idx == 0:
                        p_yuan = fmt_num(row.get('Total_Yuan', 0))
                        p_thb = fmt_num(row.get('Total_THB', 0))
                        rate = fmt_num(row.get('Yuan_Rate', 0))
                        ship_rate = fmt_num(row.get('Ship_Rate', 0))
                        cbm = fmt_num(row.get('CBM', 0), 2) 
                        ship_cost = fmt_num(row.get('Ship_Cost', 0))
                        weight = fmt_num(row.get('Transport_Weight', 0))
                        shop_s = fmt_num(row.get('Shopee_Price', 0))
                        shop_l = fmt_num(row.get('Lazada_Price', 0))
                        shop_t = fmt_num(row.get('TikTok_Price', 0))
                        note = row.get('Note', '')
                        
                        link = row.get('Link', '')
                        wechat = row.get('WeChat', '')
                        
                        link_html = f'<a href="{link}" target="_blank" class="table-link">🔗</a>' if link else '-'
                        wechat_html = f'<a href="{wechat}" target="_blank" class="table-link">💬</a>' if wechat else '-'
                        
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{fmt_num(price_unit_thb)}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{p_yuan}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{p_thb}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{rate}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{ship_rate}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{cbm}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{ship_cost}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{weight}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{fmt_num(price_unit_yuan)}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{shop_s}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{shop_l}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged num-val">{shop_t}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged" style="max-width: 150px; overflow:hidden;">{note}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged">{link_html}</td>'
                        table_html += f'<td rowspan="{row_count}" class="td-merged">{wechat_html}</td>'
                    
                    table_html += "</tr>"

            table_html += "</tbody></table></div>"
            st.markdown(table_html, unsafe_allow_html=True)

        else: st.warning("⚠️ ไม่พบรายการ (ลองเปลี่ยนตัวกรองวันที่ หรือ สถานะ)")
    else: st.info("ยังไม่มีข้อมูล PO")

# ==========================================
# TAB 3: Stock Report
# ==========================================
with tab3:
    st.subheader("📈 รายงาน Stock & ตั้งค่าการเตือน")
    if not df_master.empty and 'Product_ID' in df_master.columns:
        df_po_latest = pd.DataFrame()
        if not df_po.empty and 'Product_ID' in df_po.columns:
            df_po_latest = df_po.drop_duplicates(subset=['Product_ID'], keep='last')
        
        df_stock_report = pd.merge(df_master, df_po_latest, on='Product_ID', how='left')
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
            with col_filter:
                selected_status = st.multiselect("ตัวกรองสถานะ", options=["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"], default=[])
            with col_search:
                search_text = st.text_input("🔍 ค้นหา (ชื่อสินค้า / รหัส)", value="")
            with col_reset:
                if st.button("❌ ล้าง", use_container_width=True): st.rerun()

        edit_df = df_stock_report.copy()
        if selected_status: edit_df = edit_df[edit_df['Status'].isin(selected_status)]
        if search_text:
            edit_df = edit_df[edit_df['Product_Name'].str.contains(search_text, case=False) | edit_df['Product_ID'].str.contains(search_text, case=False)]

        col_ctrl1, col_ctrl2 = st.columns([3, 1])
        with col_ctrl1: st.info(f"💡 คงเหลือ = Master Stock - ขายล่าสุด ({latest_date_str})")
        with col_ctrl2: 
             if st.button("💾 บันทึกค่าจุดเตือน", type="primary", use_container_width=True):
                 if "edited_stock_data" in st.session_state:
                     update_master_limits(st.session_state.edited_stock_data)
                     st.rerun()

        final_cols = ["Product_ID", "Image", "Product_Name", "Current_Stock", "Recent_Sold", "Total_Sold_All", "PO_Number", "Status", "Min_Limit"]
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
# 🛑 EXECUTE DIALOGS
# ==========================================
if st.session_state.active_dialog == "po_batch":
    po_batch_dialog()
elif st.session_state.active_dialog == "po_search":
    po_edit_dialog_v2() 
elif st.session_state.active_dialog == "history" and dialog_data:
    show_history_dialog(fixed_product_id=dialog_data)