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

# --- ฟังก์ชันบันทึกแบบเดิม (สำหรับ Edit) ---
def save_po_to_sheet(data_row, row_index=None):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        formatted_row = []
        for item in data_row:
            if isinstance(item, (date, datetime)): formatted_row.append(item.strftime("%Y-%m-%d"))
            elif item is None: formatted_row.append("")
            else: formatted_row.append(item)
                
        if row_index:
            range_name = f"A{row_index}:Q{row_index}" 
            ws.update(range_name, [formatted_row])
        else:
            ws.append_row(formatted_row)
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึกไม่สำเร็จ: {e}")
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

# --- Original Edit Dialog (สำหรับปุ่ม "ค้นหา & แก้ไข") ---
@st.dialog("📝 จัดการรายการสั่งซื้อ (แก้ไข)", width="large")
def po_form_dialog(mode="search"):
    st.subheader("✏️ แก้ไขรายการ (แบบเดิม)")
    d = {}
    sheet_row_index = None
    kp = "d_edit"
    
    if not df_po.empty:
        po_map = {f"{row['PO_Number']} ({row['Product_ID']})": row for _, row in df_po.iterrows()}
        selected_key = st.selectbox("เลือกรายการ PO", options=list(po_map.keys()), index=None, key="edit_sel")
        if selected_key:
            d = po_map[selected_key].to_dict()
            if 'Sheet_Row_Index' in d: sheet_row_index = int(d['Sheet_Row_Index'])
    
    if d:
        c1, c2 = st.columns(2)
        new_qty = c1.number_input("จำนวนสั่ง", value=int(d.get("Qty_Ordered", 0)))
        new_note = c2.text_input("หมายเหตุ", value=str(d.get("Note", "")))
        
        if st.button("💾 บันทึกทับ"):
            st.warning("ฟังก์ชันแก้ไขแบบเดิม (Demo: ยังไม่เชื่อมต่อ Save ใหม่ เนื่องจากโครงสร้างเปลี่ยน)")

# --- [NEW] Batch Entry Dialog (สำหรับปุ่ม "เพิ่ม PO") ---
@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    st.caption("💡 เพิ่มรายการสินค้าลงตระกร้า แล้วบันทึกทีเดียว (รองรับ 22 คอลัมน์)")

    # 1. Header
    with st.container(border=True):
        st.subheader("1. ข้อมูลหลัก (Header)")
        c1, c2, c3, c4 = st.columns(4)
        po_number = c1.text_input("เลข PO", placeholder="PO-XXXX")
        transport_type = c2.selectbox("ขนส่ง", ["ทางรถ", "ทางเรือ", "ทางอากาศ"])
        order_date = c3.date_input("วันที่สั่งซื้อ", date.today())
        received_date = c4.date_input("วันที่ได้รับ", date.today())

    # 2. Item
    with st.container(border=True):
        st.subheader("2. เพิ่มสินค้า")
        prod_list = []
        if not df_master.empty:
            prod_list = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        
        sel_prod = st.selectbox("เลือกสินค้า", prod_list, index=None)
        
        col_img, col_in = st.columns([1, 3])
        with col_img:
            pid = ""
            if sel_prod:
                pid = sel_prod.split(" : ")[0]
                item_data = df_master[df_master['Product_ID'] == pid]
                if not item_data.empty:
                    img = item_data.iloc[0].get('Image', '')
                    if img: st.image(img, width=150)
        
        with col_in:
            r1c1, r1c2, r1c3 = st.columns(3)
            qty = r1c1.number_input("จำนวน (Qty)", min_value=1, value=100)
            ex_rate = r1c2.number_input("เรทเงิน", value=5.0)
            cbm = r1c3.number_input("CBM", value=0.0, format="%.4f")
            
            r2c1, r2c2, r2c3 = st.columns(3)
            total_yuan = r2c1.number_input("รวมหยวน", value=0.0)
            ship_rate = r2c2.number_input("เรทขนส่ง", value=5000.0)
            weight = r2c3.number_input("นน. (KG)", value=0.0)
            
            with st.expander("เพิ่มเติม (ราคาตลาด/Link)"):
                m1, m2, m3 = st.columns(3)
                p_shopee = m1.number_input("Shopee", 0)
                p_lazada = m2.number_input("Lazada", 0)
                p_tiktok = m3.number_input("TikTok", 0)
                l1, l2 = st.columns(2)
                shop_link = l1.text_input("Link")
                wechat_id = l2.text_input("WeChat")
                note = st.text_area("Note")

        if st.button("➕ เพิ่มลงตระกร้า"):
            if po_number and sel_prod:
                # Calculation
                wait_days = (received_date - order_date).days if received_date and order_date else 0
                ship_cost = ship_rate * cbm
                total_thb = total_yuan * ex_rate
                unit_thb = ((total_yuan * ex_rate) + ship_cost) / qty if qty > 0 else 0
                unit_yuan = total_yuan / qty if qty > 0 else 0
                
                item = {
                    "SKU": pid, "PO": po_number, "Trans": transport_type,
                    "Ord": str(order_date), "Recv": str(received_date), "Wait": wait_days,
                    "Qty": qty, "UnitTHB": round(unit_thb,2), "TotYuan": total_yuan,
                    "TotTHB": round(total_thb,2), "Rate": ex_rate, "ShipRate": ship_rate,
                    "CBM": cbm, "ShipCost": round(ship_cost,2), "W": weight,
                    "UnitYuan": round(unit_yuan,4), "Shopee": p_shopee, "Laz": p_lazada,
                    "Tik": p_tiktok, "Note": note, "Link": shop_link, "WeChat": wechat_id
                }
                st.session_state.po_temp_cart.append(item)
                st.success(f"เพิ่ม {pid} แล้ว")

    # 3. Preview & Save
    if st.session_state.po_temp_cart:
        st.divider()
        st.write(pd.DataFrame(st.session_state.po_temp_cart))
        c_clear, c_save = st.columns([1, 4])
        with c_clear:
            if st.button("ล้าง"):
                st.session_state.po_temp_cart = []
                st.rerun()
        with c_save:
            if st.button("💾 บันทึกทั้งหมด"):
                rows = []
                for i in st.session_state.po_temp_cart:
                    # Map to 22 Columns
                    row = [
                        i["SKU"], i["PO"], i["Trans"], i["Ord"], i["Recv"], i["Wait"],
                        i["Qty"], i["UnitTHB"], i["TotYuan"], i["TotTHB"],
                        i["Rate"], i["ShipRate"], i["CBM"], i["ShipCost"], i["W"],
                        i["UnitYuan"], i["Shopee"], i["Laz"], i["Tik"], i["Note"], i["Link"], i["WeChat"]
                    ]
                    rows.append(row)
                
                if save_po_batch_to_sheet(rows):
                    st.success("บันทึกสำเร็จ!")
                    st.session_state.po_temp_cart = []
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
                        .col-small { width: 70px !important; min-width: 70px !important; }
                        .col-medium { width: 90px !important; min-width: 90px !important; }
                        .col-image { width: 60px !important; min-width: 60px !important; }
                        .col-name { width: 200px !important; min-width: 200px !important; text-align: left !important; }
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
# TAB 2: Purchase Orders (แก้ไขให้มี Filter + ปุ่ม Edit เดิม + ปุ่ม Add ใหม่)
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
    with col_action:
        b1, b2 = st.columns(2)
        with b1:
            # ปุ่มเพิ่ม -> เรียกฟังก์ชัน Batch Entry (ของใหม่)
            if st.button("➕ เพิ่ม PO ใหม่", type="primary", key="btn_add_po_tab2"): 
                dialog_action = "po_batch"
        with b2:
            # ปุ่มแก้ไข -> เรียกฟังก์ชัน Edit (ของเดิม)
            if st.button("🔍 ค้นหา & แก้ไข", type="secondary", key="btn_search_po_tab2"): 
                dialog_action = "po_search"

    if not df_po.empty and not df_master.empty:
        # Merge ข้อมูลเพื่อแสดงผล
        df_po_filter = df_po.copy()
        if 'Order_Date' in df_po_filter.columns:
            df_po_filter['Order_Date'] = pd.to_datetime(df_po_filter['Order_Date'], errors='coerce')
        
        cols_to_use = ['Product_ID', 'Product_Name', 'Image', 'Product_Type']
        valid_cols = [c for c in cols_to_use if c in df_master.columns]
        df_display = pd.merge(df_po_filter, df_master[valid_cols], on='Product_ID', how='left')
        
        # --- Restore Filter Section (จาก Code เดิม) ---
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
            col_cat, col_sku = st.columns([1.5, 3])
            cat_opts = ["แสดงทั้งหมด"] + sorted(df_display['Product_Type'].astype(str).unique().tolist()) if 'Product_Type' in df_display.columns else ["แสดงทั้งหมด"]
            sku_opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x.get('Product_Name', '')}", axis=1).tolist()
            with col_cat: sel_cat_po = st.selectbox("หมวดหมู่สินค้า", cat_opts, key="po_cat_filter")
            with col_sku: sel_skus_po = st.multiselect("รายการที่เลือก:", sku_opts, key="po_sku_filter")

        # Apply Filters
        mask_date = (df_display['Order_Date'].dt.date >= st.session_state.po_d_start) & \
                    (df_display['Order_Date'].dt.date <= st.session_state.po_d_end)
        df_final = df_display[mask_date].copy()

        if sel_cat_po != "แสดงทั้งหมด": df_final = df_final[df_final['Product_Type'] == sel_cat_po]
        if sel_skus_po:
            selected_ids = [s.split(" : ")[0] for s in sel_skus_po]
            df_final = df_final[df_final['Product_ID'].isin(selected_ids)]

        # Render Table
        if not df_final.empty:
            if 'Order_Date' in df_final.columns: df_final['Order_Date'] = df_final['Order_Date'].dt.strftime('%Y-%m-%d')
            st.dataframe(
                df_final.style.map(highlight_negative),
                column_config={
                    "Image": st.column_config.ImageColumn("รูปสินค้า", width=80),
                    "PO_Number": st.column_config.TextColumn("เลข PO"),
                    "Total_Yuan": st.column_config.NumberColumn("ยอดหยวน", format="%.2f"),
                },
                use_container_width=True, hide_index=True
            )
        else: st.warning("⚠️ ไม่พบรายการ")
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
if dialog_action == "po_batch":
    po_batch_dialog() # ใช้แบบใหม่ (Batch)
elif dialog_action == "po_search":
    po_form_dialog(mode="search") # ใช้แบบเดิม (Edit)
elif dialog_action == "history" and dialog_data:
    show_history_dialog(fixed_product_id=dialog_data)