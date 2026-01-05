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
            # แปลงตัวเลขให้ชัวร์
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
        
        # 1. Update แถวเดิม (A:X)
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_curr.append("")
            else: formatted_curr.append(item)
        
        range_name = f"A{row_index}:X{row_index}" 
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
# 4. Main App & Data Loading
# ==========================================
st.title("📊 JST Hybrid Management System")

# --- 2. Sidebar (เมนูซ้าย + ปุ่มรีเฟรช) ---
with st.sidebar:
    # ปุ่มรีเฟรช (วางไว้บนสุด)
    if st.button("🔄 รีเฟรชข้อมูลล่าสุด", type="primary", use_container_width=True):
        st.cache_data.clear() # ล้าง Cache
        st.rerun()            # โหลดหน้าเว็บใหม่
    
    st.divider()

    # เมนูจัดการไฟล์
    st.subheader("📂 เมนูจัดการไฟล์")
    
    st.link_button(
        label="📂 ไฟล์ยอดขาย JST (Drive)", 
        url="https://drive.google.com/drive/folders/12jyMKgFHoc9-_eRZ-VN9QLsBZ31ZJP4T",
        use_container_width=True
    )
    
    st.link_button(
        label="📦 ไฟล์คลังสินค้าคงเหลือ JST (Drive)", 
        url="https://drive.google.com/drive/folders/1-hXu2RG2gNKMkW3ZFBFfhjQEhTacVYzk",
        use_container_width=True
    )
    
    st.divider()
    
    # ตั้งค่าระบบ
    st.subheader("⚙️ ตั้งค่าระบบ")
    st.link_button(
        label="🔗 เพิ่ม SKU / Master", 
        url="https://docs.google.com/spreadsheets/d/1SC_Dpq2aiMWsS3BGqL_Rdf7X4qpTFkPA0wPV6mqqosI/edit?gid=0#gid=0",
        type="secondary", 
        use_container_width=True
    )
    
    st.caption("💡 กด 'รีเฟรช' หากข้อมูลไม่แสดง")

# --- 3. Session State (ส่วนนี้เก็บไว้เหมือนเดิม ห้ามลบ) ---
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
    
    # ปุ่มปิด: กดแล้วจะลบ Link ด้านบนทิ้ง ทำให้หน้าเว็บกลับมาปกติ
    if st.button("❌ ปิดหน้าต่าง", type="primary", use_container_width=True):
        st.query_params.clear()
        st.rerun()

@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    # --- [เริ่ม] ส่วนที่แก้ไข: CSS บังคับขยายเต็มจอ ---
    st.markdown("""
    <style>
        /* 1. บังคับตัว Dialog หลักให้กว้าง 98% ของหน้าจอ */
        div[data-testid="stDialog"] {
            width: 98vw !important;
            min-width: 98vw !important;
            max-width: 98vw !important;
            left: 1vw !important;
            margin: 0 !important;
        }
        
        /* 2. บังคับเนื้อหาข้างในให้ขยายตาม */
        div[data-testid="stDialog"] > div {
            width: 100% !important;
            max-width: 100% !important;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # 1. Selection Section
    selected_pid = fixed_product_id
    if not selected_pid:
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty: return
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox("🔍 ค้นหาสินค้า", options=product_options, index=None)
        if selected_product: selected_pid = selected_product.split(" : ")[0]
    
    if selected_pid:
        # 2. Filter Data
        if not df_po.empty:
            # Filter PO for specific product
            df_history = df_po[df_po['Product_ID'] == selected_pid].copy()
            
            if not df_history.empty:
                # Merge with Master Data
                df_history['Product_ID'] = df_history['Product_ID'].astype(str)
                df_master_t = df_master.copy()
                df_master_t['Product_ID'] = df_master_t['Product_ID'].astype(str)
                
                cols_to_use = ['Product_ID', 'Product_Name', 'Image', 'Product_Type']
                valid_cols = [c for c in cols_to_use if c in df_master_t.columns]
                df_final = pd.merge(df_history, df_master_t[valid_cols], on='Product_ID', how='left')
                
                # Convert Dates
                for col in ['Order_Date', 'Received_Date', 'Expected_Date']:
                    if col in df_final.columns:
                        df_final[col] = pd.to_datetime(df_final[col], errors='coerce')

                # --- Calculate Status (Copy Logic from Tab 2) ---
                def get_status_hist(row):
                    qty_ord = float(row.get('Qty_Ordered', 0))
                    qty_recv = float(row.get('Qty_Received', 0))
                    
                    if qty_recv >= qty_ord and qty_ord > 0:
                        return "ได้รับสินค้าเรียบร้อย", "#d4edda", "#155724"
                    if qty_recv > 0 and qty_recv < qty_ord:
                        return "สินค้าไม่ครบ", "#fff3cd", "#856404"
                    
                    exp_date = row.get('Expected_Date')
                    if pd.notna(exp_date):
                        today_date = pd.Timestamp.today().normalize()
                        diff_days = (exp_date - today_date).days
                        if 0 <= diff_days <= 4:
                            return "สินค้าใกล้ถึง", "#cce5ff", "#004085"
                    
                    return "รอจัดส่ง", "#f8f9fa", "#333333"

                status_results = df_final.apply(get_status_hist, axis=1)
                df_final['Status_Text'] = status_results.apply(lambda x: x[0])
                df_final['Status_BG'] = status_results.apply(lambda x: x[1])
                df_final['Status_Color'] = status_results.apply(lambda x: x[2])

                # Sort
                df_final = df_final.sort_values(by=['Order_Date', 'PO_Number', 'Received_Date'], ascending=[False, False, True])

                # --- 3. Render Table (Layout Matches Tab 2) ---
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

                table_html = """
                <div class="po-table-container"><table class="custom-po-table"><thead><tr>
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

                    # Logic Calculation (Same as Tab 2)
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
                        
                        # --- Merged Columns ---
                        if idx == 0:
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

                        # --- Split Columns ---
                        recv_d = fmt_date(row['Received_Date'])
                        table_html += f'<td>{recv_d}</td>'
                        
                        wait_val = "-"
                        if pd.notna(row['Received_Date']) and pd.notna(row['Order_Date']):
                            wait_val = f"{(row['Received_Date'] - row['Order_Date']).days} วัน"
                        table_html += f'<td>{wait_val}</td>'

                        qty_recv = int(row.get('Qty_Received', 0))
                        q_style = "color: #ff4b4b; font-weight:bold;" if (qty_recv > 0 and qty_recv != int(row.get('Qty_Ordered', 0))) else "font-weight:bold;"
                        table_html += f'<td style="{q_style}">{qty_recv:,}</td>'

                        # --- Merged Financials & Details ---
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
                            
                            if link_val and link_val.lower() not in ['nan', 'none', '']:
                                safe_link = urllib.parse.quote(link_val)
                                icons_html.append(f"""<a href="?view_info={safe_link}" target="_self" style="text-decoration:none; font-size:16px; margin-right:5px; color:#007bff;">🔗</a>""")

                            if wechat_val and wechat_val.lower() not in ['nan', 'none', '']:
                                safe_wechat = urllib.parse.quote(wechat_val)
                                icons_html.append(f"""<a href="?view_info={safe_wechat}" target="_self" style="text-decoration:none; font-size:16px; color:#25D366;">💬</a>""")
                            
                            final_store_html = "".join(icons_html) if icons_html else "-"
                            table_html += f'<td rowspan="{row_count}" class="td-merged">{final_store_html}</td>'
                        table_html += "</tr>"
                table_html += "</tbody></table></div>"
                st.markdown(table_html, unsafe_allow_html=True)
            else: st.warning("❌ ไม่พบประวัติการสั่งซื้อสำหรับสินค้านี้")
        else: st.warning("❌ ยังไม่มีข้อมูล PO ในระบบ")

@st.dialog("📝 บันทึกรับของ / แก้ไข PO", width="large")
def po_edit_dialog_v2():
    st.caption("📦 เลือกรายการ -> ระบุจำนวนที่ได้รับจริง -> บันทึก")
    
    # --- 1. SEARCH SECTION ---
    selected_row = None
    row_index = None
    if not df_po.empty:
        po_map = {}
        for idx, row in df_po.iterrows():
            qty_ord = int(row.get('Qty_Ordered', 0))
            recv_date = str(row.get('Received_Date', '')).strip()
            # Check Status
            is_received = (recv_date != '' and recv_date.lower() != 'nat')
            status_icon = "✅ รับแล้ว" if is_received else ("✅ ครบ/ปิด" if qty_ord <= 0 else "⏳ รอของ")
            
            # Create Display Text
            display_text = f"[{status_icon}] {row.get('PO_Number','-')} : {row.get('Product_ID','-')} (สั่ง: {qty_ord})"
            po_map[display_text] = row
            
        # Sort: Waiting first
        sorted_keys = sorted(po_map.keys(), key=lambda x: "⏳" not in x)
        search_key = st.selectbox("🔍 ค้นหารายการ", options=sorted_keys, index=None, placeholder="พิมพ์เลข PO หรือ รหัสสินค้า...")
        
        if search_key:
            selected_row = po_map[search_key]
            if 'Sheet_Row_Index' in selected_row: row_index = selected_row['Sheet_Row_Index']

    st.divider()

    # --- 2. EDIT FORM ---
    if selected_row is not None and row_index is not None:
        def get_val(col, default): return selected_row.get(col, default)
        
        # Basic Data
        original_qty = int(get_val('Qty_Ordered', 1))
        trans_type = str(get_val('Transport_Type', '')).strip()
        is_internal = (trans_type == "สินค้าภายใน")
        
        try: d_ord = datetime.strptime(str(get_val('Order_Date', date.today())), "%Y-%m-%d").date()
        except: d_ord = date.today()
        
        # --- Product Header ---
        with st.container(border=True):
            pid_current = str(get_val('Product_ID', '')).strip()
            img_url = get_val('Image', '')
            pname = get_val('Product_Name', '')
            
            # Fetch Master Image/Name if missing
            if not df_master.empty:
                m_row = df_master[df_master['Product_ID'] == pid_current]
                if not m_row.empty: 
                    img_url = m_row.iloc[0].get('Image', img_url)
                    pname = m_row.iloc[0].get('Product_Name', pname)
            
            c1, c2 = st.columns([1, 4])
            if img_url: c1.image(img_url, width=100)
            else: c1.info("No Image")
            
            c2.markdown(f"### {pid_current}")
            c2.markdown(f"**ชื่อ:** {pname}")
            c2.caption(f"PO: {get_val('PO_Number','-')} | ประเภท: **{trans_type}**")
            
        # --- Edit Section ---
        with st.form(key="edit_po_form"):
            st.markdown("#### 📦 1. ข้อมูลการรับของ")
            
            # Row 1: Qty & Date
            r1_c1, r1_c2, r1_c3 = st.columns(3)
            qty_recv = r1_c1.number_input("ได้รับจริง (ชิ้น)", min_value=1, value=original_qty, key="e_qty_recv")
            d_recv = r1_c2.date_input("วันที่รับ", value=date.today(), key="e_recv_date")
            
            rem_qty = original_qty - qty_recv
            note_def = get_val('Note', '')
            if not note_def and rem_qty > 0: note_def = f"รับบางส่วน {qty_recv} (ค้าง {rem_qty})"
            e_note = r1_c3.text_input("หมายเหตุ", value=note_def, key="e_note")

            # Row 2: Import Details (Show ONLY if NOT Internal)
            # Init Variables
            e_cbm_final = 0.0
            e_weight_final = 0.0
            
            if not is_internal:
                st.markdown("#### ⚓ 2. ข้อมูลนำเข้า (ขนาด/น้ำหนัก)")
                
                # Logic: Calculate Proportional CBM
                old_cbm_total = float(get_val('CBM', 0))
                old_weight_total = float(get_val('Transport_Weight', 0))
                
                # ถ้าเคยรับของแล้ว หรือ CBM เป็น 0 อาจจะไม่ต้องคำนวณ
                if original_qty > 0:
                    suggest_cbm = (old_cbm_total / original_qty) * qty_recv
                    suggest_weight = (old_weight_total / original_qty) * qty_recv
                else:
                    suggest_cbm = old_cbm_total
                    suggest_weight = old_weight_total
                
                i_c1, i_c2 = st.columns(2)
                e_cbm_final = i_c1.number_input("ขนาดคิว (CBM) *ของยอดที่รับ", min_value=0.0, value=float(suggest_cbm), step=0.001, format="%.4f", key="e_cbm_main")
                e_weight_final = i_c2.number_input("น้ำหนัก (KG) *ของยอดที่รับ", min_value=0.0, value=float(suggest_weight), step=0.1, format="%.2f", key="e_weight_main")
            
            st.divider()
            
            # Row 3: Financials (Expander)
            with st.expander("💰 3. แก้ไขต้นทุน / ราคา (กดเพื่อเปิด)", expanded=False):
                if is_internal:
                    # Case: Internal -> Edit Total THB directly
                    curr_total_thb = float(get_val('Total_THB', 0))
                    
                    # [เพิ่มใหม่] คำนวณราคาตามสัดส่วนจำนวนที่รับ (Proportional Logic)
                    if original_qty > 0 and qty_recv != original_qty:
                         # ปรับราคาเริ่มต้นให้ตรงกับจำนวนที่รับจริง
                         curr_total_thb = (curr_total_thb / original_qty) * qty_recv

                    new_total_thb = st.number_input("ยอดเงินบาทรวม (฿)", min_value=0.0, value=curr_total_thb, step=1.0, format="%.2f", key="e_thb_int")
                    
                    # Variables for internal
                    e_yuan = 0.0
                    e_rate = 0.0
                    e_ship_rate = 0.0
                else:
                    # Case: Import -> Edit Yuan / Rate / ShipRate
                    f_c1, f_c2, f_c3 = st.columns(3)
                    
                    curr_total_yuan = float(get_val('Total_Yuan', 0))
                    # Adjust Yuan proportional to received qty if splitting
                    if original_qty > 0 and qty_recv != original_qty:
                         curr_total_yuan = (curr_total_yuan / original_qty) * qty_recv

                    e_yuan = f_c1.number_input("ราคารวม (หยวน) *เฉพาะยอดรับ", min_value=0.0, value=float(curr_total_yuan), step=0.01, format="%.2f", key="e_yuan_imp")
                    e_rate = f_c2.number_input("เรทเงิน", min_value=0.0, value=float(get_val('Yuan_Rate', 5.0)), step=0.01, format="%.2f", key="e_rate_imp")
                    e_ship_rate = f_c3.number_input("เรทขนส่ง", min_value=0.0, value=float(get_val('Ship_Rate', 6000)), step=100.0, format="%.2f", key="e_ship_rate_imp")
                    
                    new_total_thb = 0.0 # Will calculate later

                st.markdown("**ข้อมูลเพิ่มเติม (Link)**")
                l_c1, l_c2 = st.columns(2)
                e_link = l_c1.text_input("Link", value=get_val('Link', ''), key="e_link")
                e_wechat = l_c2.text_input("WeChat", value=get_val('WeChat', ''), key="e_wechat")

            # --- SUBMIT BUTTON ---
            if st.form_submit_button("💾 บันทึกรับของ / แก้ไข", type="primary"):
                
                # 1. Calculate Costs
                if is_internal:
                    # Internal: Use THB input directly
                    final_total_yuan = 0
                    final_rate = 0
                    final_ship_rate = 0
                    final_cbm = 0
                    final_weight = 0
                    final_total_thb = new_total_thb
                    
                    # Cost per unit (THB)
                    unit_cost_thb = final_total_thb / qty_recv if qty_recv > 0 else 0
                    
                    # Ship cost (Internal often included or 0)
                    total_ship_cost = 0 
                    
                else:
                    # Import: Calculate from Yuan & CBM
                    final_total_yuan = e_yuan
                    final_rate = e_rate
                    final_ship_rate = e_ship_rate
                    final_cbm = e_cbm_final
                    final_weight = e_weight_final
                    
                    total_ship_cost = final_cbm * final_ship_rate
                    final_total_thb = (final_total_yuan * final_rate) + total_ship_cost
                    
                    unit_cost_thb = final_total_thb / qty_recv if qty_recv > 0 else 0

                recv_date_str = d_recv.strftime("%Y-%m-%d")
                wait_days = (d_recv - d_ord).days
                
                # --- PREPARE DATA ---
                # [STRUCT A: Remaining Part] (ถ้ามีการแยกของ)
                # ต้องคำนวณส่วนที่เหลือ (Original - Received)
                
                orig_yuan = float(get_val('Total_Yuan', 0))
                orig_cbm = float(get_val('CBM', 0))
                orig_thb = float(get_val('Total_THB', 0)) # <--- 1. ดึงยอดบาทเดิมมา
                
                # คำนวณส่วนต่าง
                rem_yuan = orig_yuan - final_total_yuan if (orig_yuan > final_total_yuan) else 0
                rem_cbm = orig_cbm - final_cbm if (orig_cbm > final_cbm) else 0
                
                # <--- 2. เพิ่ม Logic คำนวณบาทคงเหลือ (เฉพาะ Internal)
                rem_thb = 0
                if is_internal:
                    rem_thb = orig_thb - final_total_thb
                    if rem_thb < 0: rem_thb = 0
                
                data_rem = [
                    get_val('Product_ID', ''), get_val('PO_Number', ''), trans_type, d_ord.strftime("%Y-%m-%d"), 
                    None, 0, rem_qty, 
                    0, # Recv Qty
                    0, # Unit Cost (unknown yet)
                    round(rem_yuan, 2), # Remaining Yuan
                    round(rem_thb, 2) if is_internal else 0, # <--- 3. ใส่ rem_thb ตรงนี้แทนเลข 0
                    e_rate if not is_internal else 0, 
                    e_ship_rate if not is_internal else 0, 
                    round(rem_cbm, 4), 
                    0, 0, # ShipCost, Weight
                    0, # Unit Yuan
                    get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), 
                    f"รอรับส่วนที่เหลือ ({rem_qty})", e_link, e_wechat, get_val('Expected_Date', '')
                ]

                # [STRUCT B: Received Part] (ส่วนที่รับจริง)
                data_recv = [
                    get_val('Product_ID', ''), get_val('PO_Number', ''), trans_type, d_ord.strftime("%Y-%m-%d"), 
                    recv_date_str, wait_days, qty_recv, 
                    qty_recv, # Recv Qty
                    round(unit_cost_thb, 2),
                    round(final_total_yuan, 2), 
                    round(final_total_thb, 2),
                    final_rate, final_ship_rate, round(final_cbm, 4), round(total_ship_cost, 2), final_weight,
                    round(final_total_yuan/qty_recv, 4) if (qty_recv > 0 and not is_internal) else 0,
                    get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), 
                    e_note, e_link, e_wechat, get_val('Expected_Date', '')
                ]

                # --- SAVE ---
                if rem_qty > 0: success = save_po_edit_split(row_index, data_rem, data_recv)
                else: success = save_po_edit_update(row_index, data_recv)
                
                if success:
                    st.success("✅ บันทึกข้อมูลเรียบร้อย")
                    st.session_state.active_dialog = None
                    time.sleep(1)
                    st.rerun()

# ==========================================
# [MODIFIED] PO BATCH DIALOG (NEW LAYOUT)
# ==========================================
@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    if st.session_state.get("need_reset_inputs", False):
        keys_to_reset = ["bp_sel_prod", "bp_qty", "bp_cost_yuan", "bp_cbm", "bp_weight", 
                         "bp_note", "bp_shop_s", "bp_shop_l", "bp_shop_t", "bp_expected_date", 
                         "bp_recv_date", "bp_ship_rate"]
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]
        st.session_state["need_reset_inputs"] = False

    # --- 1. Header ---
    with st.container(border=True):
        st.subheader("1. ข้อมูลเอกสาร (Header)")
        c1, c2, c3 = st.columns(3)
        po_number = c1.text_input("เลข PO", placeholder="XXXXX", key="bp_po_num")
        transport_type = c2.selectbox("การขนส่ง", ["ทางรถ", "ทางเรือ", "สินค้าภายใน"], key="bp_trans")
        order_date = c3.date_input("วันที่สั่งซื้อ", date.today(), key="bp_ord_date")

    # --- 2. Details ---
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
                expected_date = r1_c1.date_input("วันที่คาดว่าจะได้รับ", value=None, key="bp_expected_date")
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
        
        # แสดงผลโดยเปลี่ยนชื่อหัวตารางเป็นภาษาไทย
        st.dataframe(
            cart_df[["SKU", "Qty", "TotYuan", "Exp", "Recv"]], 
            use_container_width=True, 
            hide_index=True,
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
            # ... (ส่วนบันทึกเหมือนเดิม) ...
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
# ==========================================
# [NEW] PO INTERNAL BATCH DIALOG
# ==========================================
@st.dialog("📝 บันทึก PO สินค้าภายใน (Internal)", width="large")
def po_internal_batch_dialog():
    if st.session_state.get("need_reset_inputs_int", False):
        keys_to_reset = ["int_sel_prod", "int_qty", "int_total_thb", "int_note", 
                         "int_link", "int_contact", "int_shop_s", "int_shop_l", "int_shop_t", 
                         "int_expected_date", "int_recv_date"]
        for key in keys_to_reset:
            if key in st.session_state: del st.session_state[key]
        st.session_state["need_reset_inputs_int"] = False

    # --- 1. Header ---
    with st.container(border=True):
        st.subheader("1. ข้อมูลเอกสาร (Header)")
        c1, c2 = st.columns(2)
        po_number = c1.text_input("เลข PO", placeholder="XXXXX", key="int_po_num")
        order_date = c2.date_input("วันที่สั่งซื้อ", date.today(), key="int_ord_date")

    # --- 2. Details ---
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
            
            # Row 1: Image | Exp Date | Qty | Recv Date
            col_img, col_data = st.columns([1, 4])
            with col_img:
                if img_url: st.image(img_url, width=100)
                else: st.info("No Image")
            
            with col_data:
                st.markdown('<span style="color:#2ecc71; font-weight:bold;">(กรอกตอนสั่งซื้อ)</span>', unsafe_allow_html=True)
                r1_c1, r1_c2, r1_c3 = st.columns(3)
                expected_date = r1_c1.date_input("วันที่คาดว่าจะได้รับ", value=None, key="int_expected_date")
                qty = r1_c2.number_input("จำนวนสั่งซื้อ (ชิ้น)", min_value=1, value=None, placeholder="XXXXX", key="int_qty")
                recv_date = r1_c3.date_input("วันที่ได้รับ (ถ้าได้เลย)", value=None, key="int_recv_date")

                # Row 2: Total THB | Note
                r2_c1, r2_c2 = st.columns(2)
                total_thb = r2_c1.number_input("ราคาสินค้าที่สั่ง (บาท)", min_value=0.0, step=1.0, value=None, format="%.2f", placeholder="XXXXX", key="int_total_thb")
                note = r2_c2.text_input("หมายเหตุ (ถ้ามี)", placeholder="XXXXX", key="int_note")

                # Row 3: Info (Link / Contact)
                st.markdown("**ข้อมูลเพิ่มเติม (Link / ราคาขาย)**")
                r3_c1, r3_c2 = st.columns(2)
                link_shop = r3_c1.text_input("Link", key="int_link")
                contact_other = r3_c2.text_input("ช่องทางติดต่ออื่นๆ (WeChat)", key="int_contact")

                # Row 4: Prices
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

                    # Map Internal Data to Standard Structure
                    # Fields not used = 0 or Empty
                    item = {
                        "SKU": pid, "PO": po_number, 
                        "Trans": "สินค้าภายใน",  # Fix Transport Type
                        "Ord": str(order_date), 
                        "Exp": str(expected_date) if expected_date else "",   
                        "Recv": str(recv_date) if recv_date else "", "Wait": wait_days,
                        "Qty": int(c_qty), 
                        "UnitTHB": round(unit_thb, 2),
                        "TotYuan": 0,    # Unused
                        "TotTHB": round(c_total_thb, 2), 
                        "Rate": 0,       # Unused
                        "ShipRate": 0,   # Unused
                        "CBM": 0,        # Unused
                        "ShipCost": 0,   # Unused
                        "W": 0,          # Unused
                        "UnitYuan": 0,   # Unused
                        "Shopee": p_shopee if p_shopee else 0, 
                        "Laz": p_lazada if p_lazada else 0, 
                        "Tik": p_tiktok if p_tiktok else 0, 
                        "Note": note, 
                        "Link": link_shop, 
                        "WeChat": contact_other # Map to WeChat column
                    }
                    st.session_state.po_temp_cart.append(item)
                    st.toast(f"✅ เพิ่ม {pid} (Internal) แล้ว", icon="🛒")
                    st.session_state["need_reset_inputs_int"] = True
                    st.rerun()

    if st.session_state.po_temp_cart:
        st.divider()
        st.write(f"🛒 ตระกร้า ({len(st.session_state.po_temp_cart)} รายการ)")
        
        cart_df = pd.DataFrame(st.session_state.po_temp_cart)
        
        # Display Cart (Simplified cols for internal)
        st.dataframe(
            cart_df[["SKU", "Qty", "TotTHB", "Trans"]], 
            use_container_width=True, 
            hide_index=True,
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

# ==========================================
# 6. TABS & LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])

# --- TAB 1 (Daily Sales) ---
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

# --- TAB 2: Purchase Orders ---
with tab2:
    if "view_info" in st.query_params:
        val_to_show = st.query_params["view_info"]
        show_info_dialog(val_to_show)

    col_head, col_action = st.columns([4, 3]) # ปรับขนาด Column เป็น 4:3 เพื่อให้พอดีปุ่ม
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
    with col_action:
        b1, b2, b3 = st.columns(3) # เปลี่ยนเป็น 3 คอลัมน์
        if b1.button("➕ เพิ่ม PO นำเข้า", type="primary"): 
            st.session_state.active_dialog = "po_batch"
            st.rerun()
        if b2.button("➕ PO ภายใน", type="secondary"): # ปุ่มใหม่
            st.session_state.active_dialog = "po_internal"
            st.rerun()
        if b3.button("🔍 ค้นหา & แก้ไข", type="secondary"): 
            st.session_state.active_dialog = "po_search"
            st.rerun()

    if not df_po.empty and not df_master.empty:
        # --- 1. Filter Section (คงเดิม) ---
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
                # ปรับตัวเลือก Filter สถานะให้ตรงกับ Logic ใหม่
                sel_status = st.selectbox("สถานะ:", ["ทั้งหมด", "สินค้าใกล้ถึง", "รอจัดส่ง", "สินค้าไม่ครบ", "ได้รับสินค้าเรียบร้อย"])
            
            with f_col2:
                all_types = ["แสดงทั้งหมด"]
                if not df_master.empty and 'Product_Type' in df_master.columns:
                    all_types += sorted(df_master['Product_Type'].astype(str).unique().tolist())
                sel_cat_po = st.selectbox("หมวดหมู่สินค้า", all_types, key="po_cat_filter")
                
            with f_col3:
                sku_opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x.get('Product_Name', '')}", axis=1).tolist()
                sel_skus_po = st.multiselect("รายการที่เลือก:", sku_opts, key="po_sku_filter")

        # --- Prepare Data ---
        df_po_filter = df_po.copy()
        if 'Order_Date' in df_po_filter.columns: df_po_filter['Order_Date'] = pd.to_datetime(df_po_filter['Order_Date'], errors='coerce')
        if 'Received_Date' in df_po_filter.columns: df_po_filter['Received_Date'] = pd.to_datetime(df_po_filter['Received_Date'], errors='coerce')
        if 'Expected_Date' in df_po_filter.columns: df_po_filter['Expected_Date'] = pd.to_datetime(df_po_filter['Expected_Date'], errors='coerce')
        
        df_po_filter['Product_ID'] = df_po_filter['Product_ID'].astype(str)
        df_display = pd.merge(df_po_filter, df_master[['Product_ID','Product_Name','Image','Product_Type']], on='Product_ID', how='left')
        
        # Filter Date Range
        mask_date = (df_display['Order_Date'].dt.date >= st.session_state.po_d_start) & (df_display['Order_Date'].dt.date <= st.session_state.po_d_end)
        df_display = df_display[mask_date]

        # Filter Category / SKU
        if sel_cat_po != "แสดงทั้งหมด":
            df_display = df_display[df_display['Product_Type'] == sel_cat_po]
        if sel_skus_po:
            selected_ids = [s.split(" : ")[0] for s in sel_skus_po]
            df_display = df_display[df_display['Product_ID'].isin(selected_ids)]

        # --- Calculate Status for Filter & Display ---
        def get_status(row):
            qty_ord = float(row.get('Qty_Ordered', 0))
            qty_recv = float(row.get('Qty_Received', 0))
            
            # 4. ได้รับสินค้าเรียบร้อย
            if qty_recv >= qty_ord and qty_ord > 0:
                return "ได้รับสินค้าเรียบร้อย", "#d4edda", "#155724" # Green bg/text
            
            # 3. สินค้าไม่ครบ
            if qty_recv > 0 and qty_recv < qty_ord:
                return "สินค้าไม่ครบ", "#fff3cd", "#856404" # Yellow/Orange
            
            # เช็ควันที่คาดการณ์เทียบกับวันนี้ (สำหรับเคสยังไม่ได้รับ หรือรับเป็น 0)
            exp_date = row.get('Expected_Date')
            if pd.notna(exp_date):
                today_date = pd.Timestamp.today().normalize()
                diff_days = (exp_date - today_date).days
                # 1. สินค้าใกล้ถึง (วันนี้ถึงอีก 4 วันข้างหน้า)
                if 0 <= diff_days <= 4:
                    return "สินค้าใกล้ถึง", "#cce5ff", "#004085" # Blue
            
            # 2. รอจัดส่ง (Default / Overdue / หรือยังไม่ถึงกำหนดใกล้ๆ)
            return "รอจัดส่ง", "#f8f9fa", "#333333" # Gray/White

        # Apply Status to DataFrame for Filtering
        status_results = df_display.apply(get_status, axis=1)
        df_display['Status_Text'] = status_results.apply(lambda x: x[0])
        df_display['Status_BG'] = status_results.apply(lambda x: x[1])
        df_display['Status_Color'] = status_results.apply(lambda x: x[2])

        if sel_status != "ทั้งหมด":
            df_display = df_display[df_display['Status_Text'] == sel_status]

        # Sort
        df_display = df_display.sort_values(by=['Order_Date', 'PO_Number', 'Product_ID'], ascending=False)
        
        # --- HTML Table Construction ---
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
            first_row = group.iloc[0] # ใช้สำหรับดึงข้อมูลทั่วไป (รูป, ชื่อ, Link)
            
            is_internal = (str(first_row.get('Transport_Type', '')).strip() == "สินค้าภายใน")

            # 1. คำนวณยอดรวม (Sum) ของทุกบรรทัดใน Group
            total_order_qty = group['Qty_Ordered'].sum()
            if total_order_qty == 0: total_order_qty = 1 
            
            total_yuan = group['Total_Yuan'].sum()
            total_ship_cost = group['Ship_Cost'].sum()
            
            # 2. คำนวณยอดบาทรวม (Total THB Used)
            calc_total_thb_used = 0
            if is_internal:
                calc_total_thb_used = group['Total_THB'].sum()
            else:
                # นำเข้า: (หยวน * เรท) รวมกันทุกบรรทัด
                for _, r in group.iterrows():
                    calc_total_thb_used += (float(r.get('Total_Yuan',0)) * float(r.get('Yuan_Rate',0)))

            # 3. ต้นทุนต่อชิ้น (รวมค่าส่ง)
            cost_per_unit_thb = (calc_total_thb_used + total_ship_cost) / total_order_qty if total_order_qty > 0 else 0
            
            # ราคาหยวนต่อชิ้น (เฉลี่ย)
            price_per_unit_yuan = total_yuan / total_order_qty if total_order_qty > 0 else 0

            # ดึงค่าเรท (โชว์ของบรรทัดแรก)
            rate = float(first_row.get('Yuan_Rate', 0))

            bg_color = "#222222" if group_idx % 2 == 0 else "#2e2e2e"
            s_text = first_row['Status_Text']
            s_bg = first_row['Status_BG']
            s_col = first_row['Status_Color']

            for idx, (i, row) in enumerate(group.iterrows()):
                table_html += f'<tr style="background-color: {bg_color};">'
                
                # --- Merged Columns ---
                if idx == 0:
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

                # --- Split Columns (บรรทัดย่อย) ---
                recv_d = fmt_date(row['Received_Date'])
                table_html += f'<td>{recv_d}</td>'
                
                wait_val = "-"
                if pd.notna(row['Received_Date']) and pd.notna(row['Order_Date']):
                    wait_val = f"{(row['Received_Date'] - row['Order_Date']).days} วัน"
                table_html += f'<td>{wait_val}</td>'

                qty_recv = int(row.get('Qty_Received', 0))
                q_style = "color: #ff4b4b; font-weight:bold;" if (qty_recv > 0 and qty_recv != int(row.get('Qty_Ordered', 0))) else "font-weight:bold;"
                table_html += f'<td style="{q_style}">{qty_recv:,}</td>'

                # --- Merged Columns (Financials) ---
                if idx == 0:
                    # 11. จำนวนสั่งซื้อ (ยอดรวม)
                    table_html += f'<td rowspan="{row_count}" class="td-merged" style="color:#AED6F1; font-weight:bold;">{int(total_order_qty):,}</td>'
                    
                    # 12. ต้นทุน/ชิ้น (฿) (เฉลี่ย)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(cost_per_unit_thb)}</td>'
                    
                    # 13. ยอดเงินหยวน (¥) (รวม)
                    val_yuan = "-" if is_internal else fmt_num(total_yuan)
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{val_yuan}</td>'
                    
                    # 14. ยอดเงินบาทที่ใช้ (฿) (รวม) **จุดสำคัญ**
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(calc_total_thb_used)}</td>'
                    
                    # 15-20 (เหมือนเดิม)
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
                    
                    # 21-25 Other Details
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("Shopee_Price",0))}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("Lazada_Price",0))}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{fmt_num(row.get("TikTok_Price",0))}</td>'
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{row.get("Note","")}</td>'
                    
                    link_val = str(row.get("Link", "")).strip()
                    wechat_val = str(row.get("WeChat", "")).strip()
                    
                    icons_html = []
                    import time, urllib.parse
                    ts = int(time.time() * 1000) 
                    
                    if link_val and link_val.lower() not in ['nan', 'none', '']:
                        safe_link = urllib.parse.quote(link_val)
                        icons_html.append(f"""<a href="?view_info={safe_link}&t={ts}_{idx}" target="_self" style="text-decoration:none; font-size:16px; margin-right:5px; color:#007bff;">🔗</a>""")

                    if wechat_val and wechat_val.lower() not in ['nan', 'none', '']:
                        safe_wechat = urllib.parse.quote(wechat_val)
                        icons_html.append(f"""<a href="?view_info={safe_wechat}&t={ts}_{idx}" target="_self" style="text-decoration:none; font-size:16px; color:#25D366;">💬</a>""")
                    
                    final_store_html = "".join(icons_html) if icons_html else "-"
                    table_html += f'<td rowspan="{row_count}" class="td-merged">{final_store_html}</td>'

        st.markdown(table_html, unsafe_allow_html=True)
    else: st.info("ยังไม่มีข้อมูล PO")

# --- TAB 3: Stock ---
with tab3:
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
elif st.session_state.active_dialog == "po_internal": po_internal_batch_dialog() # เพิ่มบรรทัดนี้
elif st.session_state.active_dialog == "po_search": po_edit_dialog_v2() 
elif st.session_state.active_dialog == "history": show_history_dialog(fixed_product_id=st.session_state.get("selected_product_history"))