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
# 1. ตั้งค่า Page & CSS Styles (Original)
# ==========================================
st.set_page_config(page_title="JST Hybrid System", layout="wide", page_icon="📦")

st.markdown("""
<style>
    .block-container { padding-top: 1rem !important; padding-bottom: 2rem !important; }
    .metric-card { background-color: #1a1a1a; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    .metric-title { color: #b0b0b0; font-size: 14px; font-weight: 500; margin-bottom: 5px; }
    .metric-value { color: #ffffff; font-size: 28px; font-weight: bold; }
    
    /* --- CSS หัวตาราง (Original) --- */
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
            'วันที่สั่งซื้อ': 'Order_Date', 'วันที่ได้รับ': 'Received_Date', 'จำนวน': 'Qty_Ordered',
            'ราคา/ชิ้น': 'Price_Unit_NoVAT', 'ราคา (หยวน)': 'Total_Yuan', 'เรทเงิน': 'Yuan_Rate',
            'เรทค่าขนส่ง': 'Ship_Rate', 'ขนาด (คิว)': 'CBM', 'ค่าส่ง': 'Ship_Cost', 'น้ำหนัก / KG': 'Transport_Weight',
            'SHOPEE': 'Shopee_Price', 'LAZADA': 'Lazada_Price', 'TIKTOK': 'TikTok_Price', 'หมายเหตุ': 'Note',
            'ราคา (บาท)': 'Total_THB', 'Link_Shop': 'Link', 'WeChat': 'WeChat'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})

        if not df.empty:
            df['Sheet_Row_Index'] = range(2, len(df) + 2)
            for col in ['Qty_Ordered', 'Qty_Remaining', 'Total_Yuan', 'Yuan_Rate']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'Qty_Remaining' not in df.columns and 'Qty_Ordered' in df.columns:
                 df['Qty_Remaining'] = df['Qty_Ordered']
                 
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
        
        # 1. Update แถวเดิม (A:V)
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_curr.append("")
            else: formatted_curr.append(item)
        
        range_name = f"A{row_index}:V{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        # 2. Append แถวใหม่ (ยอดที่เหลือ)
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
        
        # จัดรูปแบบข้อมูลให้เป็น String หรือค่าว่าง ก่อนบันทึก
        formatted_curr = []
        for item in current_row_data:
            if isinstance(item, (date, datetime)): 
                formatted_curr.append(item.strftime("%Y-%m-%d"))
            elif item is None: 
                formatted_curr.append("")
            else: 
                formatted_curr.append(item)
        
        # Update ข้อมูลทับแถวเดิม (A ถึง V)
        range_name = f"A{row_index}:V{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Update ไม่สำเร็จ: {e}")
        return False

# --- [NEW] ฟังก์ชันบันทึกแบบ Batch (สำหรับ Add New) ---
def save_po_batch_to_sheet(rows_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        # Append rows (22 Columns structure)
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
# --- [เพิ่มบรรทัดนี้] : กำหนดตัวแปรจำสถานะหน้าต่าง ---
if "active_dialog" not in st.session_state:
    st.session_state.active_dialog = None 
# ------------------------------------------------

if "selected_product_history" not in st.session_state: st.session_state.selected_product_history = None
if 'po_temp_cart' not in st.session_state: st.session_state.po_temp_cart = [] # ตระกร้าสินค้า

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
# 5. DIALOG FUNCTIONS
# ==========================================

# --- Original History Dialog ---
@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    selected_pid = fixed_product_id
    if not selected_pid:
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty: return
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox("🔍 ค้นหาสินค้า", options=product_options, index=None)
        if selected_product: selected_pid = selected_product.split(" : ")[0]
    
    if selected_pid:
        if not df_po.empty:
            history_df = df_po[df_po['Product_ID'] == selected_pid].copy()
            if not history_df.empty:
                st.dataframe(history_df, use_container_width=True, hide_index=True)
            else: st.warning("ยังไม่มีประวัติการสั่งซื้อ")

# ==========================================
# [NEW] ฟังก์ชันแก้ไข V4 (ย้ายวันที่รับของมาไว้จุดเดียวกับจำนวน)
# ==========================================
@st.dialog("📝 บันทึกรับของ / แก้ไข PO", width="large")
def po_edit_dialog_v2():
    st.caption("📦 เลือกรายการที่ 'รอรับของ' -> ระบุจำนวนและวันที่ -> บันทึก (ระบบจัดการยอดค้างให้เอง)")

    # --- ส่วนที่ 1: ค้นหา ---
    selected_row = None
    row_index = None
    
    if not df_po.empty:
        po_map = {}
        for idx, row in df_po.iterrows():
            qty = int(row.get('Qty_Ordered', 0))
            recv_date = str(row.get('Received_Date', '')).strip()
            
            # --- [จุดที่แก้ไข] ปรับเงื่อนไขการเช็คสถานะ ---
            # 1. ถ้ามีวันที่รับแล้ว = ✅ รับแล้ว (เป็นรายการประวัติ)
            # 2. หรือ ถ้าจำนวนเหลือ 0 = ✅ ครบ (ปิดจ็อบแล้ว)
            # 3. ถ้าไม่มีวันที่ และจำนวน > 0 = ⏳ รอของ
            
            is_received = (recv_date != '' and recv_date.lower() != 'nat')
            is_closed = (qty <= 0)

            if is_received:
                status_icon = "✅ รับแล้ว" # รายการที่เป็น History
            elif is_closed:
                status_icon = "✅ ครบ"     # รายการที่ปิดจ็อบแล้ว (Qty 0)
            else:
                status_icon = "⏳ รอของ"   # รายการที่ต้องกดรับ
            # ----------------------------------------
            
            display_text = f"[{status_icon}] {row.get('PO_Number','-')} : {row.get('Product_ID','-')} (จำนวน: {qty} ชิ้น)"
            po_map[display_text] = row
        
        # เรียงลำดับ: เอา 'รอของ' ขึ้นก่อน
        # (เทคนิค: False มาก่อน True ดังนั้นเราเช็คว่า "ไม่ใช่รอของ" จะไปอยู่ข้างหลัง)
        sorted_keys = sorted(po_map.keys(), key=lambda x: "⏳" not in x)
        
        search_key = st.selectbox("🔍 ค้นหารายการ", options=sorted_keys, index=None, placeholder="พิมพ์เลข PO หรือ รหัสสินค้า...")

    st.divider()

    if selected_row is not None and row_index is not None:
        def get_val(col, default): return selected_row.get(col, default)
        
        original_qty = int(get_val('Qty_Ordered', 1))
        
        # วันที่สั่งซื้อ
        try: d_ord = datetime.strptime(str(get_val('Order_Date', date.today())), "%Y-%m-%d").date()
        except: d_ord = date.today()
        
        # วันที่รับของ: ถ้ามีค่าเดิมให้โชว์ ถ้าไม่มี (รอของ) ให้ Default เป็นวันนี้
        try: 
            raw_recv = str(get_val('Received_Date', ''))
            if raw_recv and raw_recv.lower() != 'nat' and raw_recv.strip() != '':
                d_recv = datetime.strptime(raw_recv, "%Y-%m-%d").date()
            else: 
                d_recv = date.today()
        except: d_recv = date.today()

        # --- ส่วนที่ 2: ฟอร์มแก้ไข ---
        with st.container(border=True):
            # --- [เพิ่ม] ส่วนดึงข้อมูลล่าสุดจาก Master Stock ---
            pid_current = str(get_val('Product_ID', '')).strip()
            
            # ตั้งค่าเริ่มต้นเป็นค่าจาก PO เดิม
            current_img = get_val('Image', '')
            current_name = get_val('Product_Name', '')

            # ถ้าเจอใน Master ให้ใช้ข้อมูลจาก Master แทน (เพื่อได้รูป/ชื่อล่าสุด)
            if not df_master.empty:
                match_row = df_master[df_master['Product_ID'] == pid_current]
                if not match_row.empty:
                    current_img = match_row.iloc[0].get('Image', current_img)
                    current_name = match_row.iloc[0].get('Product_Name', current_name)
            # --------------------------------------------------

            st.subheader(f"2. รายละเอียดสินค้า (ยอดรอรับสินค้าปัจจุบัน: {original_qty} ชิ้น)")
            
            col_img, col_info = st.columns([1, 3])
            with col_img:
                # ใช้ current_img ที่ดึงมาใหม่
                if current_img and str(current_img).startswith('http'): 
                    st.image(current_img, width=120)
                else: 
                    st.info("No Image")
            
            with col_info:
                st.markdown(f"**รหัสสินค้า:** `{pid_current}`")
                # ใช้ current_name ที่ดึงมาใหม่
                st.markdown(f"**ชื่อสินค้า:** {current_name}")

            st.divider()
            
            # --- [UPDATED] ย้ายวันที่รับของมาไว้ตรงนี้ ---
            st.markdown("#### 📦 บันทึกการรับของ")
            
            # จัด Layout: จำนวน | วันที่ | หมายเหตุ
            r_col1, r_col2, r_col3 = st.columns([1.5, 1.5, 2])
            
            with r_col1:
                e_qty_received = st.number_input("จำนวนที่รับ (ชิ้น)", min_value=1, max_value=original_qty, value=original_qty, key="e_qty")
            
            with r_col2:
                # ช่องวันที่รับของ
                e_recv_date = st.date_input("วันที่ได้รับของ", value=d_recv, key="e_recv_date")

            with r_col3:
                # Auto Note Logic
                default_note = get_val('Note', '')
                remaining_qty = original_qty - e_qty_received
                if remaining_qty > 0 and not default_note:
                    default_note = f"รับบางส่วน {e_qty_received} (ค้าง {remaining_qty})"
                elif not default_note:
                    default_note = "ได้รับครบ"
                e_note = st.text_input("หมายเหตุ", value=default_note, key="e_note")
            
            # แจ้งเตือนยอดค้าง
            if remaining_qty > 0:
                st.warning(f"⚠️ ยอดเดิม {original_qty} -> รับจริง {e_qty_received} | **เหลือค้างส่งอีก {remaining_qty} ชิ้น** (ระบบจะสร้างรายการรอรับของให้อัตโนมัติ)")
            else:
                st.success(f"✅ รับครบจำนวน ({original_qty} ชิ้น)")

            st.divider()
            
            # ส่วนแก้ไขต้นทุน (ซ่อนไว้ใน Expander)
            with st.expander("💰 แก้ไขต้นทุน / ราคา / ข้อมูลอื่นๆ (กดเพื่อเปิด)"):
                r2c1, r2c2, r2c3 = st.columns(3)
                e_yuan = r2c1.number_input("ราคารวม (หยวน)", min_value=0.0, value=float(get_val('Total_Yuan', 0)), step=0.01, key="e_yuan")
                e_rate = r2c2.number_input("เรทเงิน", min_value=0.0, value=float(get_val('Yuan_Rate', 5.0)), step=0.01, key="e_rate")
                
                # CBM Logic
                cbm_val = float(get_val('CBM', 0))
                suggested_cbm = (cbm_val / original_qty) * e_qty_received if original_qty > 0 else cbm_val
                
                m1, m2 = st.columns(2)
                e_cbm = m1.number_input(f"CBM (ของยอด {e_qty_received} ชิ้น)", min_value=0.0, value=float(suggested_cbm), step=0.001, format="%.4f", key="e_cbm")
                e_ship_rate = m2.number_input("เรทขนส่ง", min_value=0.0, value=float(get_val('Ship_Rate', 5000)), step=100.0, key="e_ship_rate")
                e_weight = st.number_input("น้ำหนัก KG", min_value=0.0, value=float(get_val('Transport_Weight', 0)), step=0.1, key="e_weight")
                
                x1, x2 = st.columns(2)
                e_link = x1.text_input("Link", value=get_val('Link', ''), key="e_link")
                e_wechat = x2.text_input("WeChat", value=get_val('WeChat', ''), key="e_wechat")

        # --- Calculation & Save ---
        # 1. หายอดหยวนของรอบนี้
        total_yuan_original = float(get_val('Total_Yuan', 0))
        # ถ้า user ไม่ได้แก้ e_yuan ให้คำนวณ Pro-rate
        if remaining_qty > 0 and e_yuan == total_yuan_original:
             final_calc_yuan = (total_yuan_original / original_qty) * e_qty_received if original_qty > 0 else 0
        else:
             final_calc_yuan = e_yuan

        calc_ship_cost = e_cbm * e_ship_rate
        calc_total_thb = (final_calc_yuan * e_rate)
        calc_unit_cost = ((final_calc_yuan * e_rate) + calc_ship_cost) / e_qty_received if e_qty_received > 0 else 0
        
        st.markdown(f"""
        <div style="background-color: #1e2a3a; padding: 10px; border-radius: 8px; border-left: 5px solid #4CAF50; margin-bottom: 10px;">
            💰 <b>สรุปยอดรอบนี้:</b> ต้นทุนต่อชิ้น <b>{calc_unit_cost:,.2f} บาท</b> (รวมส่ง)
        </div>
        """, unsafe_allow_html=True)

        if st.button("💾 บันทึกรับของ (สร้างประวัติใหม่)", type="primary"):
            # 1. เตรียมตัวแปร
            e_ord_date = d_ord 
            e_po = get_val('PO_Number', '')
            e_trans = get_val('Transport_Type', '')

            # 2. คำนวณยอด
            # ยอดรับจริง (รายการใหม่ที่จะสร้าง)
            qty_received = e_qty_received
            yuan_received = (total_yuan_original / original_qty) * qty_received if original_qty > 0 else 0
            
            # ยอดคงเหลือ (ที่จะอัปเดตแถวเดิม)
            qty_remaining = original_qty - qty_received
            yuan_remaining = total_yuan_original - yuan_received
            
            # คำนวณค่า CBM/Weight สำหรับยอดรับ
            # (ถ้า User ไม่ได้แก้ e_cbm ระบบจะ Pro-rate ให้ตามสัดส่วน)
            if e_cbm == float(get_val('CBM', 0)): # ถ้าค่าเท่าเดิม แสดงว่า User ไม่ได้แก้
                 cbm_received = (float(get_val('CBM', 0)) / original_qty) * qty_received
            else:
                 cbm_received = e_cbm # ใช้ค่าที่ User กรอก
            
            cbm_remaining = float(get_val('CBM', 0)) - cbm_received
            if cbm_remaining < 0: cbm_remaining = 0

            # คำนวณบาท
            total_thb_received = (yuan_received * e_rate) + (cbm_received * e_ship_rate)
            unit_cost_received = total_thb_received / qty_received if qty_received > 0 else 0
            
            # --- เตรียมข้อมูล 2 ชุด ---
            
            # ชุด A: แถวเดิม (ให้กลายเป็นยอดค้างส่ง / หรือปิดจ็อบหากเหลือ 0)
            # เราจะรักษา Order Date เดิมไว้ แต่ลบวันที่รับออก (เพราะมันยังรออยู่)
            note_remaining = f"รอรับส่วนที่เหลือ ({qty_remaining})" if qty_remaining > 0 else "✅ ปิดรายการ (รับครบแล้ว)"
            
            data_remaining_update = [
                get_val('Product_ID', ''), e_po, e_trans, e_ord_date, 
                None, # วันที่รับ (ว่างไว้ เพราะคือยอดรอ)
                0,    # จำนวนวันรอ (0 เพราะยังไม่รับ)
                qty_remaining, # *จำนวนที่เหลือ*
                0, # ราคาต่อหน่วย (ไม่ต้องแสดงของยอดรอ)
                round(yuan_remaining, 2), 
                0, # Total THB (รอคำนวณเมื่อรับจริง)
                e_rate, e_ship_rate, round(cbm_remaining, 4), 0, e_weight, 
                0, get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), 
                note_remaining, e_link, e_wechat
            ]

            # ชุด B: แถวใหม่ (History Log ของการรับของรอบนี้)
            # อันนี้จะไปต่อท้าย Database
            recv_date_str = e_recv_date
            wait_days = (e_recv_date - e_ord_date).days
            
            data_received_log = [
                get_val('Product_ID', ''), e_po, e_trans, e_ord_date, 
                recv_date_str, # วันที่รับจริง
                wait_days,
                qty_received, # *จำนวนที่รับรอบนี้*
                unit_cost_received,
                round(yuan_received, 2),
                round(total_thb_received, 2),
                e_rate, e_ship_rate, round(cbm_received, 4), round(cbm_received*e_ship_rate, 2), e_weight,
                round(yuan_received/qty_received, 4) if qty_received else 0,
                get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), 
                e_note, e_link, e_wechat
            ]

            # 3. บันทึกข้อมูล (ใช้ฟังก์ชัน Split เดิมได้เลย แต่สลับข้อมูล)
            # logic เดิม: update(current), append(new)
            # logic ใหม่: update(ยอดรอ), append(ยอดรับ) -> ทำให้ยอดรับไปอยู่บรรทัดล่างสุดเสมอ
            
            success = save_po_edit_split(row_index, data_remaining_update, data_received_log)
            
            if success:
                st.success(f"✅ บันทึกรับของ {qty_received} ชิ้น ลงท้ายตารางเรียบร้อยแล้ว!")
                st.session_state.active_dialog = None
                time.sleep(1)
                st.rerun()
                
    else:
        st.info("👈 กรุณาเลือกรายการที่ต้องการรับของจากด้านบน")

# ==========================================
# [NEW] ฟังก์ชันบันทึกแบบ Batch (Fix หน้าขาว + Reset ค่าเสถียร)
# ==========================================
@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    st.caption("💡 กรอกข้อมูลสินค้า -> กดเพิ่มลงตระกร้า (รายการจะไปกองรวมด้านล่าง) -> กดบันทึก Database ทีเดียว")

    # --- 0. ส่วนจัดการ Reset ค่า (ทำงานก่อนวาดหน้าจอเสมอ) ---
    # ตรวจสอบว่ารอบที่แล้วมีการสั่งให้ Reset หรือไม่
    if st.session_state.get("need_reset_inputs", False):
        # ล้างค่าใน Session State ที่ผูกกับ Widget
        keys_to_reset = ["bp_sel_prod", "bp_qty", "bp_cost_yuan", "bp_cbm", "bp_weight"]
        for key in keys_to_reset:
            if key in st.session_state:
                del st.session_state[key] # ลบทิ้งเพื่อให้ Widget กลับไปใช้ค่า Default
        
        # ปิด Flag เมื่อทำเสร็จ
        st.session_state["need_reset_inputs"] = False

    # --- 1. Header (ค่าพวกนี้จะไม่ถูกลบ) ---
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
        
        # Widget เหล่านี้จะถูก Reset ได้เพราะเราจัดการที่ข้อ 0.
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
            r1c1, r1c2, r1c3 = st.columns(3)
            # กำหนด value=... เพื่อให้เวลา Reset กลับมาเป็นค่านี้
            total_qty = r1c1.number_input("จำนวนสั่งซื้อ (ชิ้น)", min_value=1, value=100, key="bp_qty")
            cost_yuan = r1c2.number_input("ต้นทุนสินค้า (หยวน)", min_value=0.0, step=0.01, key="bp_cost_yuan")
            rate_money = r1c3.number_input("เรทเงิน (หยวน)", min_value=0.0, step=0.01, value=5.0, key="bp_rate")

            r2c1, r2c2, r2c3 = st.columns(3)
            cbm_val = r2c1.number_input("ขนาด (คิว) ", min_value=0.0, format="%.4f", key="bp_cbm")
            ship_rate = r2c2.number_input("เรทขนส่ง", min_value=0.0, value=5000.0, key="bp_ship_rate")
            weight_val = r2c3.number_input("น้ำหนัก (KG)", min_value=0.0, key="bp_weight")
            is_cbm_per_piece = st.checkbox("ขนาด(คิว) 'ต่อชิ้น' (ไม่ติ๊ก=รวม)", value=False)

            with st.expander("ข้อมูลเพิ่มเติม"):
                x1, x2 = st.columns(2)
                link_shop = x1.text_input("Link", key="bp_link")
                wechat = x2.text_input("WeChat", key="bp_wechat")
                m1, m2, m3 = st.columns(3)
                p_shopee = m1.number_input("Shopee", value=0, key="bp_shop_s")
                p_lazada = m2.number_input("Lazada", value=0, key="bp_shop_l")
                p_tiktok = m3.number_input("TikTok", value=0, key="bp_shop_t")

    # --- 3. การรับของ ---
    st.subheader("3. 📦 การรับสินค้า")
    recv_mode = st.radio("รูปแบบการรับของ:", ["✅ รับสินค้าครบภายในรอบเดียว", "🚚 ทยอยรับ (Split / ของขาด)"], horizontal=True)
    
    split_rows_to_process = [] 

    if recv_mode == "✅ รับสินค้าครบภายในรอบเดียว":
        col_simple_date, col_simple_note = st.columns([1, 2])
        s_date = col_simple_date.date_input("วันที่ของเข้า", date.today())
        s_note = col_simple_note.text_input("หมายเหตุ", value="ได้รับครบ")
        split_rows_to_process = [{"วันที่ได้รับ": s_date, "จำนวนที่เข้า": total_qty, "หมายเหตุ": s_note}]
    else:
        st.info("💡 กรุณาระบุยอดที่ได้รับจริง (เว้นวันที่ว่างไว้ = ยังไม่ได้รับ / รอส่ง)")
        default_data = [{"วันที่ได้รับ": date.today(), "จำนวนที่เข้า": 0, "หมายเหตุ": ""}]
        df_split_input = pd.DataFrame(default_data)
        edited_split_df = st.data_editor(
            df_split_input,
            column_config={
                "วันที่ได้รับ": st.column_config.DateColumn("วันที่ (เว้นว่าง=รอ)", format="YYYY-MM-DD"),
                "จำนวนที่เข้า": st.column_config.NumberColumn("จำนวนสินค้ารอรับ", min_value=0, required=True),
                "หมายเหตุ": st.column_config.TextColumn("หมายเหตุ", width="large")
            },
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True
        )
        current_sum = edited_split_df["จำนวนที่เข้า"].sum()
        diff = total_qty - current_sum
        
        if diff == 0:
            st.success(f"✅ ยอดครบถ้วน: {current_sum} / {total_qty}")
            for _, row in edited_split_df.iterrows():
                if row['จำนวนที่เข้า'] > 0:
                    d_val = row['วันที่ได้รับ']
                    if pd.isna(d_val) or str(d_val) == 'NaT': d_val = None
                    split_rows_to_process.append({"วันที่ได้รับ": d_val, "จำนวนที่เข้า": row['จำนวนที่เข้า'], "หมายเหตุ": row['หมายเหตุ']})
        else:
            if diff > 0: st.warning(f"⚠️ ยอดยังไม่ครบ: ขาดอีก {diff} ชิ้น")
            else: st.error(f"❌ ยอดเกิน: เกินมา {-diff} ชิ้น")
            split_rows_to_process = [] 

    # --- 4. ปุ่มเพิ่มรายการ (Logic แบบ Flag) ---
    st.divider()
    btn_disabled = (not po_number) or (not sel_prod) or (len(split_rows_to_process) == 0)

    if st.button("➕ เพิ่มรายการลงตระกร้า", type="primary", disabled=btn_disabled):
        # คำนวณค่าต่างๆ
        unit_yuan = cost_yuan / total_qty if total_qty > 0 else 0
        cbm_per_piece = cbm_val if is_cbm_per_piece else (cbm_val / total_qty if total_qty > 0 else 0)

        for row in split_rows_to_process:
            q_split = row['จำนวนที่เข้า']
            d_recv = row['วันที่ได้รับ']
            note_split = row['หมายเหตุ']
            
            recv_str = d_recv.strftime("%Y-%m-%d") if d_recv else ""
            wait_days = (d_recv - order_date).days if d_recv and order_date else 0
            
            split_cbm = cbm_per_piece * q_split
            split_ship_cost = split_cbm * ship_rate
            split_yuan = unit_yuan * q_split
            split_thb = split_yuan * rate_money
            unit_thb_final = (split_thb + split_ship_cost) / q_split if q_split > 0 else 0

            item = {
                "SKU": pid, "PO": po_number, "Trans": transport_type,
                "Ord": str(order_date), "Recv": recv_str, "Wait": wait_days,
                "Qty": int(q_split), "UnitTHB": round(unit_thb_final, 2),
                "TotYuan": round(split_yuan, 2), "TotTHB": round(split_thb, 2), 
                "Rate": rate_money, "ShipRate": ship_rate,
                "CBM": round(split_cbm, 4), "ShipCost": round(split_ship_cost, 2), 
                "W": weight_val, "UnitYuan": round(unit_yuan, 4), 
                "Shopee": p_shopee, "Laz": p_lazada, "Tik": p_tiktok, 
                "Note": note_split, "Link": link_shop, "WeChat": wechat
            }
            st.session_state.po_temp_cart.append(item)

        st.toast(f"✅ เพิ่ม {pid} เรียบร้อย!", icon="🛒")
        
        # 🟢 เปิด Flag สั่งให้รอบหน้า Reset ค่า Input
        st.session_state["need_reset_inputs"] = True
        # สั่ง Rerun เพื่อให้กลับไปทำงานที่บรรทัดบนสุดใหม่ (และล้างค่าตาม Flag)
        st.rerun()

    # --- 5. ตระกร้า ---
    if st.session_state.po_temp_cart:
        st.divider()
        st.write(f"🛒 ตระกร้า ({len(st.session_state.po_temp_cart)} รายการ)")
        st.dataframe(pd.DataFrame(st.session_state.po_temp_cart)[["SKU", "Qty", "Recv", "TotTHB", "Note"]], use_container_width=True, hide_index=True)
        
        c1, c2 = st.columns([1, 4])
        if c1.button("🗑️ ล้างตระกร้า"):
            st.session_state.po_temp_cart = []
            st.rerun()
            
        if c2.button("💾 บันทึกทั้งหมด", type="primary"):
            # ... (โค้ดเตรียม data rows_to_save เหมือนเดิม) ...
            rows_to_save = []
            for i in st.session_state.po_temp_cart:
                 # ... (logic เดิม) ...
                 row = [i["SKU"], i["PO"], i["Trans"], i["Ord"], i["Recv"], i["Wait"], i["Qty"], 0, i["TotYuan"], i["TotTHB"],
                       i["Rate"], i["ShipRate"], i["CBM"], i["ShipCost"], i["W"], i["UnitYuan"], i["Shopee"], i["Laz"], i["Tik"], i["Note"], i["Link"], i["WeChat"]]
                 rows_to_save.append(row)

            if save_po_batch_to_sheet(rows_to_save):
                st.success("บันทึกสำเร็จ!")
                st.session_state.po_temp_cart = []
                if "bp_po_num" in st.session_state: del st.session_state["bp_po_num"]
                
                # --- [เพิ่มบรรทัดนี้] : สั่งปิดหน้าต่างเมื่อทำงานจบ ---
                st.session_state.active_dialog = None 
                # -----------------------------------------------
                
                time.sleep(1)
                st.rerun()
# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])

dialog_action = None 
dialog_data = None

# ==========================================
# TAB 1: Daily Sales Report (Code เดิม 100%)
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
                    
                    # --- HTML Table Injection (Code เดิม) ---
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
# TAB 2: Purchase Orders (แก้ไขข้อความตัวกรองสถานะ)
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
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
        # Merge ข้อมูลเพื่อแสดงผล
        df_po_filter = df_po.copy()
        if 'Order_Date' in df_po_filter.columns:
            df_po_filter['Order_Date'] = pd.to_datetime(df_po_filter['Order_Date'], errors='coerce')
        
        cols_to_use = ['Product_ID', 'Product_Name', 'Image', 'Product_Type']
        valid_cols = [c for c in cols_to_use if c in df_master.columns]
        df_display = pd.merge(df_po_filter, df_master[valid_cols], on='Product_ID', how='left')
        
        # --- Filter Section ---
        with st.container(border=True):
            st.markdown("##### 🔍 ตัวกรองรายการสั่งซื้อ")
            
            # 1. ตัวกรองวันที่
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
            
            # 2. ตัวกรองสถานะ + หมวดหมู่ + SKU
            f_col1, f_col2, f_col3 = st.columns([2, 2, 3])
            
            with f_col1:
                # --- [แก้ไขข้อความตรงนี้] ---
                sel_status = st.radio("สถานะการรับของ:", ["ทั้งหมด", "รอจัดส่งสินค้า", "ได้รับสินค้าครบแล้ว"], horizontal=True, index=0)
            
            with f_col2:
                cat_opts = ["แสดงทั้งหมด"] + sorted(df_display['Product_Type'].astype(str).unique().tolist()) if 'Product_Type' in df_display.columns else ["แสดงทั้งหมด"]
                sel_cat_po = st.selectbox("หมวดหมู่สินค้า", cat_opts, key="po_cat_filter")
                
            with f_col3:
                sku_opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x.get('Product_Name', '')}", axis=1).tolist()
                sel_skus_po = st.multiselect("รายการที่เลือก:", sku_opts, key="po_sku_filter")

        # --- Apply Filters ---
        # 1. Filter Date
        mask_date = (df_display['Order_Date'].dt.date >= st.session_state.po_d_start) & \
                    (df_display['Order_Date'].dt.date <= st.session_state.po_d_end)
        df_final = df_display[mask_date].copy()

        # 2. [UPDATED Logic] Filter Status
        if sel_status == "รอจัดส่งสินค้า":
            # กรองเอาเฉพาะที่ วันที่รับ (Received_Date) เป็นค่าว่าง
            df_final = df_final[
                (df_final['Received_Date'] == "") | 
                (df_final['Received_Date'].isna())
            ]
        elif sel_status == "ได้รับสินค้าครบแล้ว":
            # กรองเอาเฉพาะที่มีวันที่รับ
            df_final = df_final[
                (df_final['Received_Date'] != "") & 
                (df_final['Received_Date'].notna())
            ]

        # 3. Filter Category & SKU
        if sel_cat_po != "แสดงทั้งหมด": df_final = df_final[df_final['Product_Type'] == sel_cat_po]
        if sel_skus_po:
            selected_ids = [s.split(" : ")[0] for s in sel_skus_po]
            df_final = df_final[df_final['Product_ID'].isin(selected_ids)]

        # Render Table
        if not df_final.empty:
            if 'Order_Date' in df_final.columns: 
                df_final['Order_Date'] = df_final['Order_Date'].dt.strftime('%Y-%m-%d')

            col_rename_map = {
                'Product_ID': 'รหัสสินค้า', 'PO_Number': 'เลข PO', 'Transport_Type': 'ขนส่ง',
                'Order_Date': 'วันที่สั่งซื้อ', 'Received_Date': 'วันที่ได้รับ', 'Qty_Ordered': 'จำนวน',
                'Price_Unit_NoVAT': 'ราคา/ชิ้น', 'Total_Yuan': 'ราคา (หยวน)', 'Total_THB': 'ราคา (บาท)',
                'Yuan_Rate': 'เรทเงิน', 'Ship_Rate': 'เรทค่าขนส่ง', 'CBM': 'ขนาด (คิว)',
                'Ship_Cost': 'ค่าส่ง', 'Transport_Weight': 'น้ำหนัก / KG',
                'Shopee_Price': 'SHOPEE', 'Lazada_Price': 'LAZADA', 'TikTok_Price': 'TIKTOK',
                'Note': 'หมายเหตุ', 'Link': 'Link_Shop', 'Product_Name': 'ชื่อสินค้า',
                'Image': 'รูป', 'Product_Type': 'หมวดหมู่'
            }
            
            df_show = df_final.rename(columns=col_rename_map)

            st.dataframe(
                df_show.style.map(highlight_negative),
                column_config={
                    "รูป": st.column_config.ImageColumn("รูปสินค้า", width=80), 
                    "เลข PO": st.column_config.TextColumn("เลข PO"),
                    "ราคา (หยวน)": st.column_config.NumberColumn("ราคา (หยวน)", format="%.2f"),
                    "ราคา (บาท)": st.column_config.NumberColumn("ราคา (บาท)", format="%.2f"),
                },
                use_container_width=True, 
                hide_index=True
            )
        else: st.warning("⚠️ ไม่พบรายการ (ลองเปลี่ยนตัวกรองวันที่ หรือ สถานะ)")
    else: st.info("ยังไม่มีข้อมูล PO")

# ==========================================
# TAB 3: Stock Report (Code เดิม 100%)
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
    # เปลี่ยนมาใช้ตัวใหม่ v2
    po_edit_dialog_v2() 
elif st.session_state.active_dialog == "history" and dialog_data:
    show_history_dialog(fixed_product_id=dialog_data)