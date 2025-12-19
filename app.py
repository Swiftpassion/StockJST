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
    
    /* --- CSS ตาราง --- */
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
            'Type': 'Product_Type', 'หมวดหมู่': 'Product_Type', 'Category': 'Product_Type'
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
        
        # --- FIX: Rename Columns (Thai -> English) ---
        # ป้องกัน KeyError โดยการเปลี่ยนชื่อไทยให้เป็นชื่อที่ระบบรู้จักทันที
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'SKU': 'Product_ID',
            'เลข PO': 'PO_Number', 'PO Number': 'PO_Number',
            'ขนส่ง': 'Transport',
            'วันที่สั่งซื้อ': 'Order_Date', 'Order Date': 'Order_Date',
            'วันที่ได้รับ': 'Received_Date', 'Received Date': 'Received_Date',
            'ระยะเวลา': 'Wait_Days',
            'จำนวน': 'Qty', 'Qty': 'Qty',
            'ราคา/ชิ้น': 'Unit_Cost_THB',
            'ราคา (หยวน)': 'Total_Yuan',
            'ราคา (บาท)': 'Total_THB',
            'เรทเงิน': 'Ex_Rate', 'Exchange Rate': 'Ex_Rate',
            'เรทค่าขนส่ง': 'Ship_Rate', 'Shipping Rate': 'Ship_Rate',
            'ขนาด (คิว)': 'CBM',
            'ค่าส่ง': 'Ship_Cost',
            'น้ำหนัก / KG': 'Weight', 'Weight': 'Weight',
            'ราคา / ชิ้น (หยวน)': 'Unit_Price_Yuan',
            'SHOPEE': 'Shopee', 'ราคาตลาด': 'Shopee',
            'LAZADA': 'Lazada',
            'TIKTOK': 'Tiktok',
            'หมายเหตุ': 'Note',
            'Link_Shop': 'Link',
            'WeChat': 'WeChat'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
        # Ensure Critical Columns Exist
        if 'Product_ID' not in df.columns: 
             # ถ้ายังไม่มีแสดงว่าชื่อใน Sheet ไม่ตรงกับที่ map ไว้เลย ให้สร้าง column ว่างเพื่อกัน error
             df['Product_ID'] = "" 
        
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
        st.error(f"❌ บันทึกไม่สำเร็จ: {e}")
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
if 'po_temp_cart' not in st.session_state: st.session_state.po_temp_cart = [] 

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    # Safe convert to string only if column exists
    if not df_master.empty and 'Product_ID' in df_master.columns: 
        df_master['Product_ID'] = df_master['Product_ID'].astype(str)
        
    if not df_po.empty and 'Product_ID' in df_po.columns: 
        df_po['Product_ID'] = df_po['Product_ID'].astype(str)
        
    if not df_sale.empty and 'Product_ID' in df_sale.columns: 
        df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

# Calculate Sales for Stock
recent_sales_map = {}
latest_date_str = "-"
if not df_sale.empty and 'Date_Only' in df_sale.columns:
    max_date = df_sale['Date_Only'].max()
    latest_date_str = max_date.strftime("%d/%m/%Y")
    recent_sales_map = df_sale[df_sale['Date_Only'] == max_date].groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()

# ==========================================
# 5. DIALOG FUNCTIONS
# ==========================================

# --- Dialog: History ---
@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    selected_pid = fixed_product_id
    if not selected_pid:
        if df_master.empty: st.warning("ไม่มีข้อมูล Master"); return
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox("🔍 ค้นหาสินค้า", options=product_options, index=None, placeholder="พิมพ์เพื่อค้นหา...")
        if selected_product: selected_pid = selected_product.split(" : ")[0]
    
    if selected_pid:
        st.divider()
        st.markdown(f"### ประวัติ: {selected_pid}")
        if not df_po.empty and 'Product_ID' in df_po.columns:
            hist = df_po[df_po['Product_ID'] == selected_pid].copy()
            if not hist.empty:
                # เลือกโชว์เฉพาะคอลัมน์สำคัญ
                cols_show = [c for c in ["PO_Number", "Order_Date", "Received_Date", "Qty", "Total_THB", "Transport", "Note"] if c in hist.columns]
                st.dataframe(hist[cols_show], use_container_width=True, hide_index=True)
            else:
                st.info("ไม่พบประวัติการสั่งซื้อ")
        else:
            st.info("ยังไม่มีข้อมูล PO")

# --- Dialog: PO Entry (Batch Mode) ---
@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    st.caption("💡 เพิ่มรายการสินค้าลงตระกร้า แล้วบันทึกทีเดียว (รองรับรับของไม่ครบ/แบ่งรับ)")

    # 1. Header (ใช้ร่วมกันทุกรายการในรอบนี้)
    with st.container(border=True):
        st.subheader("1. ข้อมูลหลัก (Header)")
        col_h1, col_h2, col_h3, col_h4 = st.columns(4)
        po_number = col_h1.text_input("เลข PO", placeholder="เช่น PO-2412001")
        transport_type = col_h2.selectbox("ขนส่ง", ["ทางรถ", "ทางเรือ", "ทางอากาศ"])
        order_date = col_h3.date_input("วันที่สั่งซื้อ", date.today())
        received_date = col_h4.date_input("วันที่ได้รับ", date.today())

    # 2. Item Entry
    with st.container(border=True):
        st.subheader("2. เพิ่มสินค้า")
        
        prod_list = []
        if not df_master.empty:
            prod_list = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        
        sel_prod = st.selectbox("เลือกสินค้า (SKU)", prod_list, index=None, placeholder="พิมพ์รหัสสินค้า...")
        
        c_img, c_form = st.columns([1, 3])
        
        with c_img:
            pid = ""
            if sel_prod:
                pid = sel_prod.split(" : ")[0]
                item_data = df_master[df_master['Product_ID'] == pid]
                if not item_data.empty:
                    img_url = item_data.iloc[0].get('Image', '')
                    if img_url: st.image(img_url, caption=pid, width=150)
                    else: st.warning("No Image")
            else:
                st.info("เลือกสินค้าเพื่อดูรูป")

        with c_form:
            r1c1, r1c2, r1c3 = st.columns(3)
            qty = r1c1.number_input("จำนวนที่รับ (Qty)", min_value=1, value=100, help="ใส่ยอดที่ได้รับจริงรอบนี้")
            ex_rate = r1c2.number_input("เรทเงิน (Rate)", min_value=0.0, value=5.0, format="%.4f")
            cbm = r1c3.number_input("ขนาด คิว (CBM)", min_value=0.0, value=0.0, format="%.4f")
            
            r2c1, r2c2, r2c3 = st.columns(3)
            total_yuan = r2c1.number_input("ราคารวม (หยวน)", min_value=0.0, value=0.0, help="ราคารวมของสินค้านี้ทั้งหมด")
            ship_rate = r2c2.number_input("เรทขนส่ง", min_value=0.0, value=5000.0)
            weight = r2c3.number_input("น้ำหนัก (KG)", min_value=0.0, value=0.0)
            
            with st.expander("ราคาตลาด & ข้อมูลเพิ่มเติม"):
                m1, m2, m3 = st.columns(3)
                p_shopee = m1.number_input("Shopee", 0)
                p_lazada = m2.number_input("Lazada", 0)
                p_tiktok = m3.number_input("TikTok", 0)
                l1, l2 = st.columns(2)
                link_shop = l1.text_input("Link Shop")
                wechat = l2.text_input("WeChat ID")
                note = st.text_area("หมายเหตุ")

        if st.button("➕ เพิ่มลงรายการ", type="primary"):
            if not po_number or not sel_prod:
                st.error("⚠️ กรุณากรอกเลข PO และเลือกสินค้า")
            else:
                wait_days = (received_date - order_date).days if received_date and order_date else 0
                ship_cost = ship_rate * cbm
                total_thb = total_yuan * ex_rate
                unit_cost_thb = ((total_yuan * ex_rate) + ship_cost) / qty if qty > 0 else 0
                unit_price_yuan = total_yuan / qty if qty > 0 else 0

                new_item = {
                    "Product_ID": pid,
                    "PO_Number": po_number,
                    "Transport": transport_type,
                    "Order_Date": str(order_date),
                    "Received_Date": str(received_date),
                    "Wait_Days": wait_days,
                    "Qty": qty,
                    "Unit_Cost_THB": round(unit_cost_thb, 2),
                    "Total_Yuan": total_yuan,
                    "Total_THB": round(total_thb, 2),
                    "Ex_Rate": ex_rate,
                    "Ship_Rate": ship_rate,
                    "CBM": cbm,
                    "Ship_Cost": round(ship_cost, 2),
                    "Weight": weight,
                    "Unit_Price_Yuan": round(unit_price_yuan, 4),
                    "Shopee": p_shopee,
                    "Lazada": p_lazada,
                    "Tiktok": p_tiktok,
                    "Note": note,
                    "Link": link_shop,
                    "WeChat": wechat
                }
                st.session_state.po_temp_cart.append(new_item)
                st.success(f"เพิ่ม {pid} แล้ว!")

    # 3. Preview & Save
    if len(st.session_state.po_temp_cart) > 0:
        st.divider()
        st.markdown(f"##### 🛒 รายการรอการบันทึก ({len(st.session_state.po_temp_cart)})")
        df_cart = pd.DataFrame(st.session_state.po_temp_cart)
        st.dataframe(df_cart[["Product_ID", "Qty", "Total_Yuan", "Unit_Cost_THB"]], use_container_width=True, hide_index=True)
        
        col_s1, col_s2 = st.columns([1, 4])
        with col_s1:
            if st.button("🗑️ ล้างรายการ", type="secondary"):
                st.session_state.po_temp_cart = []
                st.rerun()
        with col_s2:
            if st.button("💾 บันทึกทั้งหมดลง Google Sheets", type="primary"):
                rows_to_add = []
                for item in st.session_state.po_temp_cart:
                    row = [
                        item["Product_ID"], item["PO_Number"], item["Transport"],
                        item["Order_Date"], item["Received_Date"], item["Wait_Days"],
                        item["Qty"], item["Unit_Cost_THB"], item["Total_Yuan"],
                        item["Total_THB"], item["Ex_Rate"], item["Ship_Rate"],
                        item["CBM"], item["Ship_Cost"], item["Weight"],
                        item["Unit_Price_Yuan"], item["Shopee"], item["Lazada"],
                        item["Tiktok"], item["Note"], item["Link"], item["WeChat"]
                    ]
                    rows_to_add.append(row)
                
                if save_po_batch_to_sheet(rows_to_add):
                    st.success("✅ บันทึกข้อมูลสำเร็จเรียบร้อย!")
                    st.session_state.po_temp_cart = []
                    time.sleep(1)
                    st.rerun()

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ (PO)", "📈 รายงาน Stock"])

dialog_action = None 
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

    # Filter
    with st.container(border=True):
        st.markdown("##### 🔍 ตัวกรองข้อมูล")
        c_y, c_m, c_s, c_e = st.columns([1, 1.5, 1.5, 1.5])
        with c_y: st.selectbox("ปี", all_years, key="m_y", on_change=update_m_dates)
        with c_m: st.selectbox("เดือน", thai_months, index=today.month-1, key="m_m", on_change=update_m_dates)
        with c_s: st.date_input("วันที่เริ่มต้น", key="m_d_start")
        with c_e: st.date_input("วันที่สิ้นสุด", key="m_d_end")
        
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
        if not df_sale.empty and 'Date_Only' in df_sale.columns:
            mask_range = (df_sale['Date_Only'] >= start_date) & (df_sale['Date_Only'] <= end_date)
            df_sale_range = df_sale.loc[mask_range].copy()
            
            if not df_sale_range.empty:
                thai_abbr = ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
                df_sale_range['Day_Col'] = df_sale_range['Order_Time'].apply(lambda x: f"{x.day} {thai_abbr[x.month]}")
                df_sale_range['Day_Sort'] = df_sale_range['Order_Time'].dt.strftime('%Y%m%d')
                
                pivot_data = df_sale_range.groupby(['Product_ID', 'Day_Col', 'Day_Sort'])['Qty_Sold'].sum().reset_index()
                df_pivot = pivot_data.pivot(index='Product_ID', columns='Day_Col', values='Qty_Sold').fillna(0).astype(int)
                
                df_pivot = df_pivot.reset_index()
                final_report = pd.merge(df_master, df_pivot, on='Product_ID', how='left')
                
                day_cols = [c for c in final_report.columns if c not in df_master.columns]
                final_report[day_cols] = final_report[day_cols].fillna(0).astype(int)
                
                if selected_category != "แสดงทั้งหมด":
                    final_report = final_report[final_report['Product_Type'] == selected_category]
                if selected_skus:
                    selected_ids = [item.split(" : ")[0] for item in selected_skus]
                    final_report = final_report[final_report['Product_ID'].isin(selected_ids)]
                
                if not final_report.empty:
                    final_report['Total_Sales_Range'] = final_report[day_cols].sum(axis=1).astype(int)
                    stock_map = df_master.set_index('Product_ID')['Initial_Stock'].to_dict()
                    final_report['Current_Stock'] = final_report['Product_ID'].apply(lambda x: stock_map.get(x, 0) - recent_sales_map.get(x, 0)).astype(int)
                    final_report['Status'] = final_report['Current_Stock'].apply(lambda x: "🔴 หมด" if x<=0 else ("⚠️ ต่ำ" if x<10 else "🟢 ปกติ"))
                    
                    pivot_data_temp = df_sale_range.groupby(['Product_ID', 'Day_Col', 'Day_Sort'])['Qty_Sold'].sum().reset_index()
                    sorted_day_cols = sorted(day_cols, key=lambda x: pivot_data_temp[pivot_data_temp['Day_Col'] == x]['Day_Sort'].values[0] if x in pivot_data_temp['Day_Col'].values else 0)

                    fixed_cols = ['Product_ID', 'Image', 'Product_Name', 'Product_Type', 'Current_Stock', 'Total_Sales_Range', 'Status']
                    available_fixed = [c for c in fixed_cols if c in final_report.columns]
                    final_df = final_report[available_fixed + sorted_day_cols]
                    
                    st.divider()
                    st.dataframe(final_df, use_container_width=True, hide_index=True,
                        column_config={"Image": st.column_config.ImageColumn(width=40)})
                else:
                    st.warning("ไม่พบข้อมูลตามตัวกรอง")
            else:
                st.info("ไม่มีข้อมูลการขายในช่วงวันที่เลือก")
        else:
            st.warning("ไม่มีข้อมูลการขาย")

# ==========================================
# TAB 2: Purchase Orders
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า (PO)")
    with col_action:
        if st.button("➕ เพิ่ม PO ใหม่ (POP-UP)", type="primary", key="btn_add_po_popup"): 
            dialog_action = "po_batch"
    
    if not df_po.empty:
        # Show key columns
        st.dataframe(df_po, use_container_width=True, hide_index=True)
    else:
        st.info("ยังไม่มีข้อมูล PO ในระบบ")

# ==========================================
# TAB 3: Stock Report
# ==========================================
with tab3:
    st.subheader("📈 รายงาน Stock & ตั้งค่าการเตือน")
    
    if not df_master.empty and 'Product_ID' in df_master.columns:
        df_stock_report = df_master.copy()
        df_stock_report['Recent_Sold'] = df_stock_report['Product_ID'].map(recent_sales_map).fillna(0).astype(int)
        df_stock_report['Current_Stock'] = df_stock_report['Initial_Stock'] - df_stock_report['Recent_Sold']
        
        if 'Min_Limit' not in df_stock_report.columns: df_stock_report['Min_Limit'] = 10
        else: df_stock_report['Min_Limit'] = pd.to_numeric(df_stock_report['Min_Limit'], errors='coerce').fillna(10).astype(int)

        def calc_status(row):
            if row['Current_Stock'] <= 0: return "🔴 หมดเกลี้ยง"
            elif row['Current_Stock'] < row['Min_Limit']: return "⚠️ ใกล้หมด"
            return "🟢 มีของ"
            
        df_stock_report['Status'] = df_stock_report.apply(calc_status, axis=1)

        c_filt, c_srch = st.columns([2, 2])
        with c_filt: 
            status_options = ["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"]
            sel_stat = st.multiselect("สถานะ", status_options)
        with c_srch:
            srch_txt = st.text_input("ค้นหา", placeholder="ชื่อ/รหัส")

        edit_df = df_stock_report.copy()
        if sel_stat: edit_df = edit_df[edit_df['Status'].isin(sel_stat)]
        if srch_txt: 
            edit_df = edit_df[edit_df['Product_ID'].str.contains(srch_txt, case=False) | edit_df['Product_Name'].str.contains(srch_txt, case=False)]

        col_c1, col_c2 = st.columns([4, 1])
        with col_c1: st.info(f"ขายล่าสุดถึง: {latest_date_str}")
        with col_c2: 
            if st.button("💾 บันทึก Limit"): 
                if "edited_stock_data" in st.session_state:
                    update_master_limits(st.session_state.edited_stock_data)
                    st.rerun()

        final_cols = ["Product_ID", "Image", "Product_Name", "Current_Stock", "Recent_Sold", "Status", "Min_Limit"]
        edited_df = st.data_editor(
            edit_df[final_cols],
            column_config={
                "Image": st.column_config.ImageColumn(width=50),
                "Product_ID": st.column_config.TextColumn(disabled=True),
                "Product_Name": st.column_config.TextColumn(disabled=True),
                "Current_Stock": st.column_config.NumberColumn(disabled=True),
                "Min_Limit": st.column_config.NumberColumn("🔔 เตือนเมื่อต่ำกว่า", min_value=0)
            },
            use_container_width=True, hide_index=True, key="edited_stock_data", height=800
        )
    else:
        st.warning("ไม่พบข้อมูล Master")

# ==========================================
# 🛑 EXECUTE DIALOGS
# ==========================================
if dialog_action == "po_batch":
    po_batch_dialog()