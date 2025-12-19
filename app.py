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
    
    /* CSS หัวตาราง */
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
        
        # --- FIX KEYERROR ---
        # 1. Map ชื่อไทย -> อังกฤษ (ถ้ามี)
        col_map = {
            'รูปภาพ': 'Image',
            'รหัสสินค้า': 'Product_ID', 'รหัส': 'Product_ID',
            'ชื่อสินค้า': 'Product_Name', 'ชื่อ': 'Product_Name',
            'หมวดหมู่': 'Product_Type', 'Type': 'Product_Type',
            'จุดเตือน': 'Min_Limit', 'Min': 'Min_Limit',
            'จำนวน': 'Initial_Stock', 'สินค้าคงเหลือ': 'Initial_Stock'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns and v not in df.columns})

        # 2. จัดการเรื่อง Stock vs Initial_Stock
        # ถ้ามีทั้งคู่ ให้ใช้ Initial_Stock ถ้ามีแค่ Stock ให้แก้ชื่อเป็น Initial_Stock
        if 'Stock' in df.columns:
            if 'Initial_Stock' not in df.columns:
                df = df.rename(columns={'Stock': 'Initial_Stock'})
            else:
                # ถ้ามีทั้งคู่ อาจจะลบ Stock ทิ้ง หรือปล่อยไว้ แต่ต้องมั่นใจว่ามี Initial_Stock
                pass 

        # 3. ตรวจสอบและสร้างคอลัมน์ที่ขาดหายไป (สำคัญมาก ป้องกัน KeyError)
        required_cols = ['Product_ID', 'Product_Name', 'Image', 'Initial_Stock', 'Product_Type', 'Min_Limit']
        for col in required_cols:
            if col not in df.columns:
                if col == 'Initial_Stock': df[col] = 0
                elif col == 'Min_Limit': df[col] = 10
                elif col == 'Product_Type': df[col] = "ทั่วไป"
                else: df[col] = ""

        # Format Data
        df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'], errors='coerce').fillna(0).astype(int)
        df['Min_Limit'] = pd.to_numeric(df['Min_Limit'], errors='coerce').fillna(10).astype(int)
        df['Product_ID'] = df['Product_ID'].astype(str)
            
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล Master Stock ไม่ได้: {e}")
        # Return empty DF with required columns to prevent crash
        return pd.DataFrame(columns=['Product_ID', 'Product_Name', 'Image', 'Initial_Stock', 'Product_Type', 'Min_Limit'])

@st.cache_data(ttl=60)
def get_po_data():
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล PO ไม่ได้: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_sale_from_folder():
    try:
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        
        results = service.files().list(
            q=f"'{FOLDER_ID_DATA_SALE}' in parents and trashed=false",
            orderBy='modifiedTime desc', pageSize=100, fields="files(id, name)").execute()
        
        items = results.get('files', [])
        if not items: return pd.DataFrame()
        
        all_dfs = [] 
        for item in items:
            try:
                file_id = item['id']
                file_name = item['name']
                if not file_name.endswith(('.xlsx', '.xls')): continue

                request = service.files().get_media(fileId=file_id)
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
            except Exception as file_err:
                continue

        if all_dfs: return pd.concat(all_dfs, ignore_index=True)
        else: return pd.DataFrame()

    except Exception as e:
        st.warning(f"⚠️ อ่านไฟล์ Excel Sale ไม่ทัน: {e}")
        return pd.DataFrame()

def save_po_batch(data_rows):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        ws.append_rows(data_rows)
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
        
        # Check if Min_Limit exists, if not check for "จุดเตือน" or create new
        col_index = -1
        if target_col_name in headers:
            col_index = headers.index(target_col_name) + 1
        elif "จุดเตือน" in headers:
            col_index = headers.index("จุดเตือน") + 1
        else:
            ws.update_cell(1, len(headers) + 1, target_col_name)
            col_index = len(headers) + 1
            
        all_rows = ws.get_all_values()
        if len(all_rows) < 2: return
        
        header_row = all_rows[0]
        try:
            pid_idx = -1
            for i, h in enumerate(header_row):
                if h in ['รหัสสินค้า', 'รหัส', 'ID', 'Product_ID']:
                    pid_idx = i
                    break
            if pid_idx == -1: return 
            
            limit_map = df_edited.set_index('Product_ID')['Min_Limit'].to_dict()
            values_to_update = []
            
            for row in all_rows[1:]:
                if len(row) <= pid_idx: 
                    values_to_update.append([10])
                    continue
                pid = str(row[pid_idx])
                old_val = 10
                if len(row) >= col_index:
                    try: old_val = int(row[col_index-1])
                    except: old_val = 10
                
                if pid in limit_map:
                    val = limit_map[pid]
                    values_to_update.append([int(val)])
                else:
                    values_to_update.append([old_val])

            range_name = f"{gspread.utils.rowcol_to_a1(2, col_index)}:{gspread.utils.rowcol_to_a1(len(values_to_update)+1, col_index)}"
            ws.update(range_name, values_to_update)
            st.toast("✅ บันทึกจุดเตือนเรียบร้อยแล้ว!", icon="💾")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการ map ข้อมูล: {e}")
    except Exception as e:
        st.error(f"❌ บันทึกจุดเตือนไม่สำเร็จ: {e}")

# ==========================================
# 4. Main App & Data Loading
# ==========================================
st.title("📊 JST Hybrid Management System")

# Init States
if 'po_cart' not in st.session_state: st.session_state.po_cart = []

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    if not df_master.empty and 'Product_ID' in df_master.columns: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty and 'รหัสสินค้า' in df_po.columns: df_po['รหัสสินค้า'] = df_po['รหัสสินค้า'].astype(str)
    if not df_sale.empty and 'Product_ID' in df_sale.columns: df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

# 🛠️ PREPARE DATA: หาข้อมูลยอดขาย "วันล่าสุด"
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
@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    if fixed_product_id and not df_po.empty and 'รหัสสินค้า' in df_po.columns:
        history_df = df_po[df_po['รหัสสินค้า'] == fixed_product_id].copy()
        if not history_df.empty:
            st.subheader(f"ประวัติ PO: {fixed_product_id}")
            cols = ["เลข PO", "วันที่สั่งซื้อ", "วันที่ได้รับ", "จำนวน", "ราคา/ชิ้น", "ราคา (บาท)", "ขนส่ง"]
            valid_cols = [c for c in cols if c in history_df.columns]
            st.dataframe(history_df[valid_cols].sort_values(by="วันที่ได้รับ", ascending=False), hide_index=True, use_container_width=True)
        else:
            st.warning(f"ไม่พบประวัติการสั่งซื้อของ {fixed_product_id}")
    else:
        st.error("ไม่พบข้อมูล")

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])

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

    with st.container(border=True):
        st.markdown("##### 🔍 ตัวกรองข้อมูล")
        c_y, c_m, c_s, c_e = st.columns([1, 1.5, 1.5, 1.5])
        with c_y: st.selectbox("ปี", all_years, key="m_y")
        with c_m: st.selectbox("เดือน", thai_months, index=today.month-1, key="m_m")
        # Logic วันที่
        m_index = thai_months.index(st.session_state.m_m) + 1
        _, last_day = calendar.monthrange(st.session_state.m_y, m_index)
        d_start = date(st.session_state.m_y, m_index, 1)
        d_end = date(st.session_state.m_y, m_index, last_day)
        
        with c_s: d_s_input = st.date_input("วันที่เริ่มต้น", value=d_start)
        with c_e: d_e_input = st.date_input("วันที่สิ้นสุด", value=d_end)
        
    if not df_sale.empty and 'Date_Only' in df_sale.columns:
        mask_range = (df_sale['Date_Only'] >= d_s_input) & (df_sale['Date_Only'] <= d_e_input)
        df_range = df_sale.loc[mask_range].copy()
        
        if not df_range.empty:
            df_range['Day_Sort'] = df_range['Order_Time'].dt.strftime('%d')
            pivot = df_range.groupby(['Product_ID', 'Day_Sort'])['Qty_Sold'].sum().unstack(fill_value=0)
            
            # Merge (Safe Mode: Ensure columns exist before merge)
            master_cols = ['Product_ID', 'Product_Name', 'Image', 'Initial_Stock']
            # Filter only existing columns in df_master
            existing_master_cols = [c for c in master_cols if c in df_master.columns]
            
            report = pd.merge(df_master[existing_master_cols], pivot, on='Product_ID', how='inner')
            
            st.markdown(f"**แสดงผล:** {len(report)} รายการ")
            st.dataframe(
                report, 
                column_config={"Image": st.column_config.ImageColumn("รูป", width=60)},
                use_container_width=True, hide_index=True
            )
        else:
            st.warning("ไม่มียอดขายในช่วงนี้")
    else:
        st.info("ยังไม่พบไฟล์ยอดขาย")

# ==========================================
# TAB 2: Purchase Orders
# ==========================================
with tab2:
    st.header("🚢 บันทึกข้อมูลการสั่งซื้อ (PO Import)")

    with st.expander("📝 ฟอร์มกรอกข้อมูลสินค้า (คลิกเพื่อเปิด/ปิด)", expanded=True):
        with st.form("po_cart_form", clear_on_submit=True):
            st.markdown("##### 1. ข้อมูลหลัก (Header)")
            c1, c2, c3, c4 = st.columns(4)
            po_number = c1.text_input("เลข PO", placeholder="เช่น 000001")
            transport = c2.selectbox("ขนส่ง", ["ส่งทางเรือ", "ส่งทางรถ", "ทางอากาศ"])
            order_date = c3.date_input("วันที่สั่งซื้อ")
            received_date = c4.date_input("วันที่ได้รับ")

            st.divider()
            st.markdown("##### 2. รายละเอียดสินค้า")
            col_prod, col_img = st.columns([3, 1])
            with col_prod:
                prod_opts = []
                if not df_master.empty:
                    prod_opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
                sel_prod_str = st.selectbox("เลือกสินค้า (SKU)", prod_opts)
                sel_sku = sel_prod_str.split(" : ")[0] if sel_prod_str else ""

            with col_img:
                img_url = ""
                if not df_master.empty and sel_sku:
                    found = df_master[df_master['Product_ID'] == sel_sku]
                    if not found.empty and 'Image' in found.columns:
                        img_url = found['Image'].values[0]
                if img_url: st.image(img_url, width=100)

            r1, r2, r3, r4 = st.columns(4)
            qty = r1.number_input("จำนวน", min_value=1, value=100)
            price_rmb_total = r2.number_input("ราคา (หยวน) *ราคารวม*", min_value=0.0)
            exchange_rate = r3.number_input("เรทเงิน", min_value=0.0, value=5.0)
            shipping_rate = r4.number_input("เรทค่าขนส่ง", min_value=0.0, value=4500.0)

            r2_1, r2_2, r2_3 = st.columns(3)
            size_cbm = r2_1.number_input("ขนาด (คิว)", min_value=0.0, value=0.1, format="%.4f")
            weight_kg = r2_2.number_input("น้ำหนัก (KG)", min_value=0.0, value=10.0)
            
            st.markdown("---")
            m1, m2, m3 = st.columns(3)
            p_shopee = m1.number_input("ราคา Shopee", 0.0)
            p_lazada = m2.number_input("ราคา Lazada", 0.0)
            p_tiktok = m3.number_input("ราคา Tiktok", 0.0)
            
            note = st.text_input("หมายเหตุ")
            l1, l2 = st.columns(2)
            link_shop = l1.text_input("Link Shop")
            wechat = l2.text_input("WeChat")

            if st.form_submit_button("➕ เพิ่มรายการลงตะกร้า"):
                if not po_number or not sel_sku:
                    st.error("กรุณาระบุเลข PO และเลือกสินค้า")
                else:
                    wait_days = (received_date - order_date).days
                    shipping_cost = shipping_rate * size_cbm
                    total_thb = price_rmb_total * exchange_rate
                    unit_cost_thb = (total_thb + shipping_cost) / qty if qty > 0 else 0
                    unit_price_rmb = price_rmb_total / qty if qty > 0 else 0

                    st.session_state.po_cart.append({
                        "รหัสสินค้า": sel_sku, "เลข PO": po_number, "ขนส่ง": transport,
                        "วันที่สั่งซื้อ": str(order_date), "วันที่ได้รับ": str(received_date),
                        "ระยะเวลา": f"{wait_days} วัน", "จำนวน": qty, "ราคา/ชิ้น": round(unit_cost_thb, 2),
                        "ราคา (หยวน)": price_rmb_total, "ราคา (บาท)": round(total_thb, 2),
                        "เรทเงิน": exchange_rate, "เรทค่าขนส่ง": shipping_rate,
                        "ขนาด (คิว)": size_cbm, "ค่าส่ง": round(shipping_cost, 2),
                        "น้ำหนัก / KG": weight_kg, "ราคา / ชิ้น (หยวน)": round(unit_price_rmb, 4),
                        "SHOPEE": p_shopee, "LAZADA": p_lazada, "TIKTOK": p_tiktok,
                        "หมายเหตุ": note, "Link_Shop": link_shop, "WeChat": wechat
                    })
                    st.success(f"เพิ่ม {sel_sku} เรียบร้อย!")

    if st.session_state.po_cart:
        st.info(f"🛒 รายการในตะกร้า: {len(st.session_state.po_cart)} รายการ")
        st.dataframe(pd.DataFrame(st.session_state.po_cart))

        col_save, col_clear = st.columns([1, 4])
        if col_save.button("💾 บันทึกทั้งหมดลง Sheet", type="primary"):
            target_cols = ["รหัสสินค้า", "เลข PO", "ขนส่ง", "วันที่สั่งซื้อ", "วันที่ได้รับ", "ระยะเวลา", "จำนวน", "ราคา/ชิ้น", "ราคา (หยวน)", "ราคา (บาท)", "เรทเงิน", "เรทค่าขนส่ง", "ขนาด (คิว)", "ค่าส่ง", "น้ำหนัก / KG", "ราคา / ชิ้น (หยวน)", "SHOPEE", "LAZADA", "TIKTOK", "หมายเหตุ", "Link_Shop", "WeChat"]
            data_to_save = [[item.get(c, "") for c in target_cols] for item in st.session_state.po_cart]
            if save_po_batch(data_to_save):
                st.success("บันทึกเรียบร้อย!"); st.session_state.po_cart = []; time.sleep(1); st.rerun()

        if col_clear.button("ล้างรายการ"):
            st.session_state.po_cart = []; st.rerun()

    st.markdown("---")
    st.subheader("📜 ประวัติการสั่งซื้อ (ล่าสุด)")
    hist_cols = ["รหัสสินค้า", "เลข PO", "ขนส่ง", "วันที่สั่งซื้อ", "วันที่ได้รับ", "ระยะเวลา", "จำนวน", "ราคา/ชิ้น", "ราคา (หยวน)", "ราคา (บาท)", "เรทเงิน", "เรทค่าขนส่ง", "ขนาด (คิว)", "ค่าส่ง", "น้ำหนัก / KG", "ราคา / ชิ้น (หยวน)", "SHOPEE", "LAZADA", "TIKTOK", "หมายเหตุ", "Link_Shop", "WeChat"]
    if not df_po.empty:
        available_cols = [c for c in hist_cols if c in df_po.columns]
        st.dataframe(df_po[available_cols].sort_values(by="วันที่ได้รับ", ascending=False).head(100), hide_index=True, use_container_width=True)


# ==========================================
# TAB 3: Stock Report
# ==========================================
with tab3:
    st.subheader("📈 รายงาน Stock & ตั้งค่าการเตือน")
    
    if not df_master.empty and 'Product_ID' in df_master.columns:
        df_po_latest = pd.DataFrame()
        if not df_po.empty and 'รหัสสินค้า' in df_po.columns:
             temp_po = df_po.rename(columns={'รหัสสินค้า': 'Product_ID', 'จำนวน': 'Qty_Ordered', 'เลข PO': 'PO_Number'})
             df_po_latest = temp_po.drop_duplicates(subset=['Product_ID'], keep='last')
        
        # Merge Safe
        master_cols_stock = ['Product_ID', 'Product_Name', 'Image', 'Initial_Stock', 'Min_Limit']
        existing_cols = [c for c in master_cols_stock if c in df_master.columns]
        
        df_stock_report = pd.merge(df_master[existing_cols], df_po_latest[['Product_ID', 'Qty_Ordered', 'PO_Number']], on='Product_ID', how='left')
        
        # Fill missing numeric cols
        for c in ['Initial_Stock', 'Qty_Ordered', 'Min_Limit']:
            if c not in df_stock_report.columns: df_stock_report[c] = 0
            
        df_stock_report['Recent_Sold'] = df_stock_report['Product_ID'].map(recent_sales_map).fillna(0).astype(int)
        df_stock_report['Current_Stock'] = df_stock_report['Initial_Stock'] - df_stock_report['Recent_Sold']

        def calc_status(row):
            limit = row['Min_Limit'] if pd.notna(row['Min_Limit']) else 10
            if row['Current_Stock'] <= 0: return "🔴 หมดเกลี้ยง"
            elif row['Current_Stock'] < limit: return "⚠️ ใกล้หมด"
            return "🟢 มีของ"
            
        df_stock_report['Status'] = df_stock_report.apply(calc_status, axis=1)

        col_search, col_reset = st.columns([4, 1])
        with col_search: search_text = st.text_input("🔍 ค้นหา Stock", key="stock_search")
        with col_reset: 
            st.write(""); st.write("")
            if st.button("Refresh"): st.rerun()

        edit_df = df_stock_report.copy()
        if search_text:
            mask = edit_df['Product_ID'].str.contains(search_text, case=False) | edit_df['Product_Name'].str.contains(search_text, case=False)
            edit_df = edit_df[mask]

        cols_final = ["Product_ID", "Image", "Product_Name", "Current_Stock", "Recent_Sold", "Qty_Ordered", "PO_Number", "Status", "Min_Limit"]
        existing_final = [c for c in cols_final if c in edit_df.columns]
        
        edited_df = st.data_editor(
            edit_df[existing_final],
            column_config={
                "Image": st.column_config.ImageColumn("รูป"),
                "Min_Limit": st.column_config.NumberColumn("🔔 จุดเตือน (แก้ได้)", min_value=0),
            },
            height=800, use_container_width=True, hide_index=True, key="edited_stock_data"
        )

        if st.button("💾 บันทึกค่าจุดเตือน"):
            update_master_limits(st.session_state.edited_stock_data)
            st.rerun()
    else:
        st.warning("ไม่พบข้อมูล Master Product")