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
    .metric-card { background-color: #1a1a1a; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    
    /* --- CSS หัวตาราง --- */
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
        
        # Clean Headers
        df.columns = df.columns.astype(str).str.strip()
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'รหัส': 'Product_ID', 'ID': 'Product_ID', 'SKU': 'Product_ID',
            'ชื่อสินค้า': 'Product_Name', 'ชื่อ': 'Product_Name', 'Name': 'Product_Name',
            'รูป': 'Image', 'รูปภาพ': 'Image', 'Link รูป': 'Image',
            'Stock': 'Initial_Stock', 'จำนวน': 'Initial_Stock', 'สต็อก': 'Initial_Stock',
            'Min_Limit': 'Min_Limit', 'Min': 'Min_Limit',
            'Type': 'Product_Type', 'หมวดหมู่': 'Product_Type'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
        if 'Initial_Stock' not in df.columns: df['Initial_Stock'] = 0
        if 'Product_ID' not in df.columns: df['Product_ID'] = "Unknown"
        if 'Product_Name' not in df.columns: df['Product_Name'] = df['Product_ID']
        if 'Product_Type' not in df.columns: df['Product_Type'] = "ทั่วไป"
        
        df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'], errors='coerce').fillna(0).astype(int)
        df['Product_ID'] = df['Product_ID'].astype(str)
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล Master Stock ไม่ได้: {e}")
        return pd.DataFrame()

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
                if not item['name'].endswith(('.xlsx', '.xls')): continue
                request = service.files().get_media(fileId=item['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False: status, done = downloader.next_chunk()
                fh.seek(0)
                
                temp_df = pd.read_excel(fh)
                col_map = {'รหัสสินค้า':'Product_ID', 'จำนวน':'Qty_Sold', 'เวลาสั่งซื้อ':'Order_Time'}
                temp_df = temp_df.rename(columns={k:v for k,v in col_map.items() if k in temp_df.columns})
                
                if 'Qty_Sold' in temp_df.columns: 
                    temp_df['Qty_Sold'] = pd.to_numeric(temp_df['Qty_Sold'], errors='coerce').fillna(0).astype(int)
                if 'Order_Time' in temp_df.columns:
                    temp_df['Order_Time'] = pd.to_datetime(temp_df['Order_Time'], errors='coerce')
                    temp_df['Date_Only'] = temp_df['Order_Time'].dt.date
                if 'Product_ID' in temp_df.columns:
                    temp_df['Product_ID'] = temp_df['Product_ID'].astype(str)

                if not temp_df.empty: all_dfs.append(temp_df)
            except: continue

        if all_dfs: return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()
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
        col_index = headers.index("Min_Limit") + 1 if "Min_Limit" in headers else len(headers) + 1
        if "Min_Limit" not in headers: ws.update_cell(1, col_index, "Min_Limit")
            
        all_rows = ws.get_all_values()
        pid_idx = next((i for i, h in enumerate(all_rows[0]) if h in ['รหัสสินค้า', 'Product_ID']), -1)
        if pid_idx == -1: return

        limit_map = df_edited.set_index('Product_ID')['Min_Limit'].to_dict()
        values_to_update = []
        for row in all_rows[1:]:
            pid = str(row[pid_idx]) if len(row) > pid_idx else ""
            val = limit_map.get(pid, row[col_index-1] if len(row) >= col_index else 10)
            values_to_update.append([int(val)])

        range_name = f"{gspread.utils.rowcol_to_a1(2, col_index)}:{gspread.utils.rowcol_to_a1(len(values_to_update)+1, col_index)}"
        ws.update(range_name, values_to_update)
        st.toast("✅ บันทึกจุดเตือนเรียบร้อย!", icon="💾")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"❌ บันทึกจุดเตือนไม่สำเร็จ: {e}")

# ==========================================
# 4. Main App & Data Loading
# ==========================================
st.title("📊 JST Hybrid Management System")

if "po_cart" not in st.session_state: st.session_state.po_cart = []

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    if not df_master.empty and 'Product_ID' in df_master.columns: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty and 'รหัสสินค้า' in df_po.columns: df_po['รหัสสินค้า'] = df_po['รหัสสินค้า'].astype(str)
    if not df_sale.empty and 'Product_ID' in df_sale.columns: df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

# 🛠️ Prepare Data (Recent Sales)
recent_sales_map = {}
latest_date_str = "ไม่พบข้อมูล"
if not df_sale.empty and 'Date_Only' in df_sale.columns:
    max_date = df_sale['Date_Only'].max()
    latest_date_str = max_date.strftime("%d/%m/%Y")
    recent_sales_map = df_sale[df_sale['Date_Only'] == max_date].groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()

# ==========================================
# 5. Dialog Functions (เพิ่มกลับมาให้แล้วครับ)
# ==========================================
@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    if fixed_product_id and not df_po.empty:
        # กรองข้อมูลจาก df_po (ใช้ชื่อไทยตาม Google Sheet)
        history_df = df_po[df_po['รหัสสินค้า'] == fixed_product_id].copy()
        
        if not history_df.empty:
             # แสดงข้อมูล
            st.subheader(f"ประวัติ PO: {fixed_product_id}")
            
            # เลือกคอลัมน์ที่จะโชว์ใน Dialog (ปรับให้ตรงกับ Sheet ใหม่ของคุณ)
            cols_to_show = [
                "เลข PO", "วันที่สั่งซื้อ", "วันที่ได้รับ", "จำนวน", 
                "ราคา/ชิ้น", "ราคา (บาท)", "ขนส่ง", "สถานะ"
            ]
            # กรองเฉพาะคอลัมน์ที่มีอยู่จริง
            cols = [c for c in cols_to_show if c in history_df.columns]
            
            st.dataframe(
                history_df[cols].sort_values(by="วันที่ได้รับ", ascending=False),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.warning(f"ไม่พบประวัติการสั่งซื้อของ {fixed_product_id}")
    else:
        st.error("ไม่พบข้อมูล")

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "🚢 บันทึก & ประวัติ PO", "📈 รายงาน Stock"])

# --- TAB 1: Daily Sales ---
with tab1:
    st.subheader("📅 สรุปยอดขายรายวัน")
    
    # 🔗 ส่วนดักจับลิงก์ดูประวัติ (เรียกใช้ Dialog ตรงนี้)
    if "history_pid" in st.query_params:
        hist_pid = st.query_params["history_pid"]
        st.query_params.clear() # ล้างค่าหลังจากรับมาแล้ว
        show_history_dialog(fixed_product_id=hist_pid)

    thai_months = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน", "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
    today = date.today()
    
    with st.container(border=True):
        c_y, c_m = st.columns([1, 1.5])
        sel_y = c_y.selectbox("ปี", [today.year, today.year-1], key="m_y")
        sel_m = c_m.selectbox("เดือน", thai_months, index=today.month-1, key="m_m")
        
        # คำนวณวันที่เริ่ม-จบเดือน
        m_idx = thai_months.index(sel_m) + 1
        _, last_day = calendar.monthrange(sel_y, m_idx)
        start_date = date(sel_y, m_idx, 1)
        end_date = date(sel_y, m_idx, last_day)

    if not df_sale.empty and 'Date_Only' in df_sale.columns:
        mask = (df_sale['Date_Only'] >= start_date) & (df_sale['Date_Only'] <= end_date)
        df_range = df_sale.loc[mask].copy()
        
        if not df_range.empty:
            df_range['Day_Sort'] = df_range['Order_Time'].dt.strftime('%d')
            pivot = df_range.groupby(['Product_ID', 'Day_Sort'])['Qty_Sold'].sum().unstack(fill_value=0)
            
            # Merge with Master
            report = pd.merge(df_master[['Product_ID', 'Product_Name', 'Image', 'Initial_Stock']], pivot, on='Product_ID', how='inner')
            
            # สร้าง HTML Table (ย่อส่วนจาก Code เดิมเพื่อความกระชับ)
            st.markdown(f"**แสดงผล:** {len(report)} รายการ")
            
            # (ตรงนี้คุณสามารถใส่ Code HTML Table แบบละเอียดจากเวอร์ชั่นเก่าได้ถ้าต้องการ)
            # เพื่อความง่ายในเวอร์ชั่นนี้ขอแสดงเป็น DataFrame ปกติก่อน
            st.dataframe(
                report,
                column_config={"Image": st.column_config.ImageColumn("รูป", width=60)},
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("ไม่มียอดขายในช่วงนี้")

# --- TAB 2: PO System (New) ---
with tab2:
    st.header("🚢 บันทึกข้อมูลการสั่งซื้อ (PO Import)")

    # 1. Form Input
    with st.expander("📝 ฟอร์มกรอกข้อมูลสินค้า", expanded=True):
        with st.form("po_entry_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            po_number = c1.text_input("เลข PO", placeholder="เช่น 000001")
            transport = c2.selectbox("ขนส่ง", ["ส่งทางเรือ", "ส่งทางรถ", "ทางอากาศ"])
            order_date = c3.date_input("วันที่สั่งซื้อ")
            received_date = c4.date_input("วันที่ได้รับ")

            st.divider()
            col_prod, col_img = st.columns([3, 1])
            with col_prod:
                prod_opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist() if not df_master.empty else []
                sel_prod = st.selectbox("เลือกสินค้า (SKU)", prod_opts)
                sel_sku = sel_prod.split(" : ")[0] if sel_prod else ""
            with col_img:
                img_url = df_master[df_master['Product_ID']==sel_sku]['Image'].values[0] if sel_sku and not df_master.empty else ""
                if img_url: st.image(img_url, width=100)

            st.write("---")
            r1, r2, r3, r4 = st.columns(4)
            qty = r1.number_input("จำนวน", min_value=1, value=100)
            price_rmb_total = r2.number_input("ราคา (หยวน) *รวม*", min_value=0.0)
            exchange_rate = r3.number_input("เรทเงิน", value=5.0)
            shipping_rate = r4.number_input("เรทค่าขนส่ง", value=4500.0)
            
            size_cbm = st.number_input("ขนาด (คิว)", value=0.1)
            
            # ตลาดและ Note
            m1, m2, m3 = st.columns(3)
            p_shopee = m1.number_input("Shopee", 0.0)
            p_lazada = m2.number_input("Lazada", 0.0)
            p_tiktok = m3.number_input("Tiktok", 0.0)
            note = st.text_input("หมายเหตุ")
            link_shop = st.text_input("Link Shop")
            wechat = st.text_input("WeChat")

            if st.form_submit_button("➕ เพิ่มรายการ"):
                if po_number and sel_sku:
                    # Calculation
                    wait_days = (received_date - order_date).days
                    total_thb = price_rmb_total * exchange_rate
                    ship_cost = shipping_rate * size_cbm
                    unit_thb = (total_thb + ship_cost) / qty if qty else 0
                    unit_rmb = price_rmb_total / qty if qty else 0

                    st.session_state.po_cart.append({
                        "รหัสสินค้า": sel_sku, "เลข PO": po_number, "ขนส่ง": transport,
                        "วันที่สั่งซื้อ": str(order_date), "วันที่ได้รับ": str(received_date),
                        "ระยะเวลา": f"{wait_days} วัน", "จำนวน": qty, "ราคา/ชิ้น": round(unit_thb,2),
                        "ราคา (หยวน)": price_rmb_total, "ราคา (บาท)": round(total_thb,2),
                        "เรทเงิน": exchange_rate, "เรทค่าขนส่ง": shipping_rate,
                        "ขนาด (คิว)": size_cbm, "ค่าส่ง": round(ship_cost,2),
                        "น้ำหนัก / KG": 0, "ราคา / ชิ้น (หยวน)": round(unit_rmb,4),
                        "SHOPEE": p_shopee, "LAZADA": p_lazada, "TIKTOK": p_tiktok,
                        "หมายเหตุ": note, "Link_Shop": link_shop, "WeChat": wechat
                    })
                    st.success(f"เพิ่ม {sel_sku} แล้ว")

    # 2. Cart
    if st.session_state.po_cart:
        st.info(f"🛒 รอการบันทึก {len(st.session_state.po_cart)} รายการ")
        st.dataframe(pd.DataFrame(st.session_state.po_cart))
        if st.button("💾 บันทึกทั้งหมดลง Sheet", type="primary"):
            cols = ["รหัสสินค้า", "เลข PO", "ขนส่ง", "วันที่สั่งซื้อ", "วันที่ได้รับ", "ระยะเวลา", "จำนวน", "ราคา/ชิ้น", "ราคา (หยวน)", "ราคา (บาท)", "เรทเงิน", "เรทค่าขนส่ง", "ขนาด (คิว)", "ค่าส่ง", "น้ำหนัก / KG", "ราคา / ชิ้น (หยวน)", "SHOPEE", "LAZADA", "TIKTOK", "หมายเหตุ", "Link_Shop", "WeChat"]
            data = [[item.get(c,"") for c in cols] for item in st.session_state.po_cart]
            if save_po_batch(data):
                st.success("บันทึกเรียบร้อย!"); st.session_state.po_cart = []; time.sleep(1); st.rerun()
        if st.button("ล้างรายการ"): st.session_state.po_cart = []; st.rerun()

    # 3. History
    st.markdown("---")
    st.subheader("📜 ประวัติการสั่งซื้อล่าสุด")
    if not df_po.empty:
        st.dataframe(df_po.sort_values(by="วันที่ได้รับ", ascending=False).head(50), use_container_width=True, hide_index=True)

# --- TAB 3: Stock ---
with tab3:
    st.subheader("📈 รายงาน Stock")
    if not df_master.empty:
        # Prepare Stock Data
        stock_df = df_master.copy()
        stock_df['Recent_Sold'] = stock_df['Product_ID'].map(recent_sales_map).fillna(0)
        stock_df['Current_Stock'] = stock_df['Initial_Stock'] - stock_df['Recent_Sold']
        stock_df['Status'] = stock_df.apply(lambda x: "🔴 หมด" if x['Current_Stock']<=0 else ("⚠️ ต่ำ" if x['Current_Stock']<int(x['Min_Limit'] or 10) else "🟢 ปกติ"), axis=1)
        
        # Display & Edit
        search = st.text_input("🔍 ค้นหา (ชื่อ/รหัส)", key="search_stock")
        if search: stock_df = stock_df[stock_df['Product_ID'].str.contains(search) | stock_df['Product_Name'].str.contains(search)]
        
        edited = st.data_editor(
            stock_df[['Product_ID', 'Image', 'Product_Name', 'Current_Stock', 'Recent_Sold', 'Status', 'Min_Limit']],
            column_config={
                "Image": st.column_config.ImageColumn("รูป"),
                "Min_Limit": st.column_config.NumberColumn("🔔 จุดเตือน (แก้ไข)", min_value=0)
            },
            use_container_width=True, hide_index=True, key="stock_editor"
        )
        if st.button("💾 บันทึกค่าจุดเตือน"):
            update_master_limits(st.session_state.stock_editor)
            st.rerun()