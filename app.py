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
    .metric-title { color: #b0b0b0; font-size: 14px; font-weight: 500; margin-bottom: 5px; }
    .metric-value { color: #ffffff; font-size: 28px; font-weight: bold; }
    .border-cyan { border-left: 4px solid #00e5ff; }
    .border-gold { border-left: 4px solid #ffd700; }
    .border-red  { border-left: 4px solid #ff4d4d; }
    .text-cyan { color: #00e5ff !important; }
    .text-gold { color: #ffd700 !important; }
    .text-red  { color: #ff4d4d !important; }
    
    /* Custom Header for Table */
    [data-testid="stDataFrame"] th { text-align: center !important; background-color: #0047AB !important; color: white !important; vertical-align: middle !important; min-height: 60px; font-size: 14px; border-bottom: 2px solid #ffffff !important; }
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
        
        # --- FIX: Mapping Columns (Thai -> English) ---
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'รหัส': 'Product_ID', 'ID': 'Product_ID',
            'ชื่อสินค้า': 'Product_Name', 'ชื่อ': 'Product_Name', 'Name': 'Product_Name',
            'รูป': 'Image', 'รูปภาพ': 'Image', 'Link รูป': 'Image',
            'Stock': 'Initial_Stock', 'จำนวน': 'Initial_Stock', 'สต็อก': 'Initial_Stock'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
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
        if not df.empty:
            df['Sheet_Row_Index'] = range(2, len(df) + 2)
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
                    temp_df['Qty_Sold'] = pd.to_numeric(temp_df['Qty_Sold'], errors='coerce').fillna(0)
                if 'Order_Time' in temp_df.columns:
                    temp_df['Order_Time'] = pd.to_datetime(temp_df['Order_Time'], errors='coerce')
                    temp_df['Date_Only'] = temp_df['Order_Time'].dt.date
                
                if not temp_df.empty: all_dfs.append(temp_df)
            except Exception as file_err:
                st.warning(f"⚠️ อ่านไฟล์ {item['name']} ไม่สำเร็จ: {file_err}")
                continue

        if all_dfs: return pd.concat(all_dfs, ignore_index=True)
        else: return pd.DataFrame()

    except Exception as e:
        st.warning(f"⚠️ อ่านไฟล์ Excel Sale ไม่ทัน: {e}")
        return pd.DataFrame()

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

# ==========================================
# 4. Main App Structure & Data Loading
# ==========================================
st.title("📊 JST Hybrid Management System")

# Initialize Session State
if "active_dialog" not in st.session_state: st.session_state.active_dialog = None
if "selected_product_history" not in st.session_state: st.session_state.selected_product_history = None

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    if not df_master.empty and 'Product_ID' in df_master.columns: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty and 'Product_ID' in df_po.columns: df_po['Product_ID'] = df_po['Product_ID'].astype(str)
    if not df_sale.empty and 'Product_ID' in df_sale.columns: df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

# ==========================================
# 5. DIALOG FUNCTIONS
# ==========================================
@st.dialog("📜 ประวัติการสั่งซื้อ (PO History)", width="large")
def show_history_dialog(fixed_product_id=None):
    selected_pid = fixed_product_id
    if not selected_pid:
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty or df_po.empty:
            st.info("ไม่มีข้อมูลสินค้าหรือประวัติการสั่งซื้อ")
            return
        if 'Product_ID' in df_master.columns and 'Product_Name' in df_master.columns:
            product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
            selected_product = st.selectbox("🔍 ค้นหาสินค้า (ชื่อ/รหัส)", options=product_options, index=None, placeholder="พิมพ์เพื่อค้นหา...")
            if selected_product: selected_pid = selected_product.split(" : ")[0]
    
    if selected_pid:
        product_name = ""
        if not df_master.empty and 'Product_ID' in df_master.columns:
            match = df_master[df_master['Product_ID'] == selected_pid]
            if not match.empty and 'Product_Name' in match.columns: product_name = match.iloc[0]['Product_Name']

        if 'Product_ID' in df_po.columns:
            history_df = df_po[df_po['Product_ID'] == selected_pid].copy()
            if 'Sheet_Row_Index' in history_df.columns: history_df = history_df.drop(columns=['Sheet_Row_Index'])
            
            if not history_df.empty:
                if 'Order_Date' in history_df.columns:
                    history_df['Order_Date'] = pd.to_datetime(history_df['Order_Date'], errors='coerce')
                    history_df = history_df.sort_values(by='Order_Date', ascending=False)
                    history_df['Order_Date'] = history_df['Order_Date'].dt.strftime('%Y-%m-%d').fillna("-")

                st.divider()
                st.markdown(f"### {selected_pid} : {product_name}")
                st.dataframe(
                    history_df,
                    column_config={
                        "Product_ID": st.column_config.TextColumn("รหัสสินค้า"),
                        "PO_Number": st.column_config.TextColumn("เลข PO", width="medium"),
                        "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width="medium"),
                        "Received_Date": st.column_config.TextColumn("ของมา", width="medium"),
                        "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d"),
                        "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d"),
                        "Total_Yuan": st.column_config.NumberColumn("ยอดหยวน", format="%.2f ¥"),
                        "Transport_Type": st.column_config.TextColumn("ขนส่ง"),
                    },
                    use_container_width=True, hide_index=True, height=400
                )
            else: st.warning(f"สินค้านี้ ({selected_pid}) ยังไม่มีประวัติการสั่งซื้อ (PO)")

@st.dialog("📝 จัดการรายการสั่งซื้อ", width="large")
def po_form_dialog(mode="add"):
    if mode == "add": st.subheader("➕ เพิ่มรายการใหม่")
    else: st.subheader("✏️ แก้ไขรายการ")
    d = {}
    sheet_row_index = None

    if mode == "search":
        st.markdown("### 🔍 ค้นหา PO")
        if not df_po.empty: 
            po_map = {f"{row['PO_Number']} (สินค้า: {row['Product_ID']})": row for _, row in df_po.iterrows()}
            selected_key = st.selectbox("เลือกรายการ PO", options=list(po_map.keys()), index=None, placeholder="พิมพ์เพื่อค้นหา PO...")
            if selected_key:
                d = po_map[selected_key].to_dict()
                if 'Sheet_Row_Index' in d: sheet_row_index = int(d['Sheet_Row_Index'])
                else: 
                    match_row = df_po[(df_po['PO_Number']==d['PO_Number']) & (df_po['Product_ID']==d['Product_ID'])]
                    if not match_row.empty: sheet_row_index = match_row.index[0] + 2
        else:
            st.warning("ยังไม่มีข้อมูล PO"); return

    def clear_form_data():
        keys = ["add_po_num", "add_weight", "add_qty_ord", "add_qty_rem", "add_yuan_rate", "add_fees", "add_p_novat", "add_p_1688_no", "add_p_1688_ship", "add_p_shopee", "add_p_tiktok", "add_total_yuan"]
        for k in keys: st.session_state[k] = None
        st.session_state["add_order_date"] = date.today()
        st.session_state["add_recv_date"] = None

    key_prefix = f"search_{sheet_row_index}" if mode == "search" and sheet_row_index else "add"

    if 'Product_ID' in df_master.columns and 'Product_Name' in df_master.columns:
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        default_idx = None
        if mode == "search" and "Product_ID" in d:
             matches = [i for i, opt in enumerate(product_options) if opt.startswith(str(d["Product_ID"]) + " :")]
             if matches: default_idx = matches[0]
        selected_option = st.selectbox("ระบุสินค้า", product_options, index=default_idx, placeholder="🔍 Search...", label_visibility="collapsed", key=f"{key_prefix}_product_select")
    else: selected_option = None
    
    master_img_url = "https://via.placeholder.com/300x300.png?text=No+Image"
    master_pid = ""
    master_name = ""

    if selected_option:
        master_pid = selected_option.split(" : ")[0]
        row_info = df_master[df_master['Product_ID'] == master_pid].iloc[0]
        if 'Product_Name' in row_info: master_name = row_info['Product_Name']
        if 'Image' in row_info and row_info['Image']: master_img_url = row_info['Image']

    with st.container(border=True):
        col_left_img, col_right_form = st.columns([1.2, 3], gap="medium")
        with col_left_img:
            st.markdown(f"**{master_pid}**"); st.image(master_img_url, use_container_width=True)
            if master_name: st.caption(f"{master_name}")
        
        with col_right_form:
            def get_date_val(val):
                if not val or val == "" or val == "nan": return None
                try: return datetime.strptime(str(val), "%Y-%m-%d").date()
                except: return None
            def vn(k): 
                val = d.get(k)
                if mode == "search":
                    try: return float(val) if val is not None and str(val).strip() != "" else 0.0
                    except: return 0.0
                else:
                    try: return float(val) if val and float(val)!=0 else None
                    except: return None

            r1c1, r1c2, r1c3 = st.columns(3)
            po_num = r1c1.text_input("เลข PO *", value=d.get("PO_Number") if mode=="search" else None, placeholder="ระบุเลข PO", key=f"{key_prefix}_po_num")
            order_date = r1c2.date_input("วันที่สั่ง", value=get_date_val(d.get("Order_Date")) if mode=="search" else date.today(), key=f"{key_prefix}_order_date")
            recv_date = r1c3.date_input("ของมา (ประมาณ)", value=get_date_val(d.get("Received_Date")), key=f"{key_prefix}_recv_date")
            weight_txt = st.text_area("📦 น้ำหนักขนส่ง / รายละเอียด *", value=d.get("Transport_Weight") if mode=="search" else None, height=100, placeholder="รายละเอียด...", key=f"{key_prefix}_weight")
            
            r3c1, r3c2, r3c3, r3c4 = st.columns(4)
            qty_ord = r3c1.number_input("สั่งมา *", min_value=0.0, step=0.0, value=vn("Qty_Ordered"), key=f"{key_prefix}_qty_ord") 
            qty_rem = r3c2.number_input("เหลือ *", min_value=0.0, step=0.0, value=vn("Qty_Remaining"), key=f"{key_prefix}_qty_rem")
            yuan_rate = r3c3.number_input("เรทหยวน *", min_value=0.0, step=0.0, format="%.2f", value=vn("Yuan_Rate"), key=f"{key_prefix}_yuan_rate")
            fees = r3c4.number_input("ค่าธรรมเนียม", min_value=0.0, step=0.0, format="%.2f", value=vn("Fees"), key=f"{key_prefix}_fees")
            
            r4c1, r4c2, r4c3 = st.columns(3)
            p_no_vat = r4c1.number_input("ราคาต่อชิ้นไม่รวม VAT", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_Unit_NoVAT"), key=f"{key_prefix}_p_novat")
            p_1688_noship = r4c2.number_input("ราคา 1688 ไม่รวมส่ง", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_1688_NoShip"), key=f"{key_prefix}_p_1688_no")
            p_1688_ship = r4c3.number_input("ราคา 1688 รวมส่ง *", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_1688_WithShip"), key=f"{key_prefix}_p_1688_ship")

            r5c1, r5c2, r5c3 = st.columns(3)
            p_shopee = r5c1.number_input("Shopee", min_value=0.0, step=0.0, format="%.2f", value=vn("Shopee_Price"), key=f"{key_prefix}_p_shopee")
            p_tiktok = r5c2.number_input("TikTok", min_value=0.0, step=0.0, format="%.2f", value=vn("TikTok_Price"), key=f"{key_prefix}_p_tiktok")
            transport = r5c3.selectbox("การขนส่ง", ["ส่งทางรถ 🚛", "ส่งทางเรือ 🚢"], index=1 if d.get("Transport_Type") == "ส่งทางเรือ 🚢" else 0, key=f"{key_prefix}_transport")
            
            st.markdown("---")
            f_col1, f_col2, f_col3 = st.columns([1.5, 0.75, 0.75])
            with f_col1: total_yuan_input = st.number_input("ราคาหยวนทั้งหมด *", min_value=0.0, step=0.0, format="%.2f", value=vn("Total_Yuan"), key=f"{key_prefix}_total_yuan")
            with f_col2: 
                st.write(""); st.write("") 
                if mode == "add": st.button("🧹 ล้าง", on_click=clear_form_data, key="btn_clear_data_bottom", type="secondary")
            with f_col3:
                st.write(""); st.write("")
                if st.button("✅ บันทึก" if mode == "add" else "💾 บันทึกทับ", type="primary", key="btn_submit_po"):
                    if not master_pid or not po_num or (qty_ord or 0) <= 0 or (total_yuan_input or 0) <= 0:
                        st.error("⚠️ ข้อมูลไม่ครบถ้วน (รหัสสินค้า, เลข PO, จำนวน, ยอดหยวน)")
                    else:
                        wait_days = (recv_date - order_date).days if order_date and recv_date else ""
                        new_row = [master_pid, po_num, order_date, recv_date, weight_txt, qty_ord or 0, qty_rem or 0, yuan_rate or 0, p_no_vat or 0, p_1688_noship or 0, p_1688_ship or 0, total_yuan_input or 0, p_shopee or 0, p_tiktok or 0, fees or 0, transport, wait_days]
                        if save_po_to_sheet(new_row, row_index=sheet_row_index): 
                            st.success("✅ บันทึกเรียบร้อย!"); time.sleep(1); st.rerun()

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
# 🛑 ปรับลำดับ Tab: 1. สรุปยอดขาย (Daily Sale), 2. PO, 3. รายงาน Stock
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])

# ==========================================
# TAB 1: Daily Sales Report (Moved here)
# ==========================================
with tab1:
    st.subheader("📅 สรุปยอดขายรายวัน")
    
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
        st.markdown("##### 🔍 ตัวกรองช่วงเวลา")
        c_y, c_m, c_s, c_e = st.columns([1, 1.5, 1.5, 1.5])
        with c_y: st.selectbox("ปี", all_years, key="m_y", on_change=update_m_dates)
        with c_m: st.selectbox("เดือน", thai_months, index=today.month-1, key="m_m", on_change=update_m_dates)
        with c_s: st.date_input("วันที่เริ่มต้น", key="m_d_start")
        with c_e: st.date_input("วันที่สิ้นสุด", key="m_d_end")

    start_date = st.session_state.m_d_start
    end_date = st.session_state.m_d_end
    
    if start_date and end_date:
        if start_date > end_date: st.error("⚠️ วันที่เริ่มต้นต้องมาก่อนวันที่สิ้นสุด")
        else:
            if not df_sale.empty and 'Date_Only' in df_sale.columns and 'Product_ID' in df_sale.columns:
                mask = (df_sale['Date_Only'] >= start_date) & (df_sale['Date_Only'] <= end_date)
                df_sale_filtered = df_sale.loc[mask].copy()
                
                if not df_sale_filtered.empty:
                    thai_abbr = ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
                    df_sale_filtered['Day_Col'] = df_sale_filtered['Order_Time'].apply(lambda x: f"{x.day} {thai_abbr[x.month]}")
                    df_sale_filtered['Day_Sort'] = df_sale_filtered['Order_Time'].dt.strftime('%Y%m%d')
                    
                    pivot_data = df_sale_filtered.groupby(['Product_ID', 'Day_Col', 'Day_Sort'])['Qty_Sold'].sum().reset_index()
                    df_pivot = pivot_data.pivot(index='Product_ID', columns='Day_Col', values='Qty_Sold').fillna(0)
                    
                    sorted_cols = sorted(df_pivot.columns, key=lambda x: pivot_data[pivot_data['Day_Col'] == x]['Day_Sort'].values[0])
                    df_pivot = df_pivot[sorted_cols]
                    df_pivot['Total_Sales_Range'] = df_pivot.sum(axis=1)
                    
                    df_pivot = df_pivot.reset_index()
                    stock_map = {}
                    if not df_master.empty and 'Initial_Stock' in df_master.columns:
                        stock_map = df_master.set_index('Product_ID')['Initial_Stock'].to_dict()
                    all_time_sold = df_sale.groupby('Product_ID')['Qty_Sold'].sum().to_dict()
                    
                    if not df_master.empty and 'Product_Name' in df_master.columns and 'Image' in df_master.columns:
                        final_report = pd.merge(df_pivot, df_master[['Product_ID', 'Product_Name', 'Image']], on='Product_ID', how='left')
                    else:
                        final_report = df_pivot; final_report['Product_Name'] = ""; final_report['Image'] = ""

                    final_report['Current_Stock'] = final_report['Product_ID'].apply(lambda x: stock_map.get(x, 0) - all_time_sold.get(x, 0))
                    
                    # ✅ เพิ่ม Column "Status"
                    final_report['Status'] = final_report['Current_Stock'].apply(lambda x: "🔴 หมด" if x<=0 else ("⚠️ ต่ำ" if x<10 else "🟢 ปกติ"))
                    
                    # ✅ จัดเรียง Column: รหัส | รูป | ชื่อ | คงเหลือ | ยอดรวม | สถานะ | วันที่...
                    fixed_cols = ['Product_ID', 'Image', 'Product_Name', 'Current_Stock', 'Total_Sales_Range', 'Status']
                    day_cols = [c for c in final_report.columns if c not in fixed_cols and c in sorted_cols]
                    
                    available_fixed = [c for c in fixed_cols if c in final_report.columns]
                    final_df = final_report[available_fixed + day_cols]
                    
                    st.divider()
                    st.markdown(f"**📊 แสดงผล: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}** ({len(final_df)} รายการ)")
                    st.caption("💡 คลิกที่รายการสินค้าเพื่อดูประวัติ (PO History)")

                    event = st.dataframe(
                        final_df,
                        column_config={
                            "Product_ID": st.column_config.TextColumn("รหัส", width=80),
                            "Image": st.column_config.ImageColumn("รูป", width=60),
                            "Product_Name": st.column_config.TextColumn("ชื่อสินค้า", width=200),
                            "Current_Stock": st.column_config.NumberColumn("คงเหลือ", format="%d", width=70),
                            "Total_Sales_Range": st.column_config.NumberColumn("ยอดรวม", format="%d", width=80),
                            "Status": st.column_config.TextColumn("สถานะ", width=80), # ✅ Config Column Status
                        },
                        height=600, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row"
                    )
                    
                    if event.selection and event.selection["rows"]:
                        selected_idx = event.selection["rows"][0]
                        selected_pid = final_df.iloc[selected_idx]['Product_ID']
                        show_history_dialog(fixed_product_id=selected_pid)
                else: st.warning("⚠️ ไม่พบยอดขายในช่วงเวลาที่เลือก")
            else: st.error("⚠️ ไม่พบข้อมูลการขาย (Sale Data) หรือ Master Data ไม่สมบูรณ์")

# ==========================================
# TAB 2: Purchase Orders
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
    with col_action:
        b1, b2 = st.columns(2)
        with b1:
            if st.button("➕ เพิ่ม PO ใหม่", type="primary"): st.session_state.active_dialog = "add"; st.rerun()
        with b2:
            if st.button("🔍 ค้นหา & แก้ไข", type="secondary"): st.session_state.active_dialog = "search"; st.rerun()

    if st.session_state.active_dialog == "add": po_form_dialog(mode="add")
    elif st.session_state.active_dialog == "search": po_form_dialog(mode="search")

    if not df_po.empty and 'Product_ID' in df_po.columns and not df_master.empty and 'Product_ID' in df_master.columns:
        df_po_display = pd.merge(df_po, df_master[['Product_ID', 'Image']], on='Product_ID', how='left')
        if "Image" in df_po_display.columns: df_po_display["Image"] = df_po_display["Image"].fillna("").astype(str)
        st.data_editor(
            df_po_display[["Image", "Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Total_Yuan", "Transport_Type"]],
            column_config={"Image": st.column_config.ImageColumn("รูปสินค้า", width=80)},
            height=700, use_container_width=True, hide_index=True, disabled=True 
        )
    else: st.info("ยังไม่มีข้อมูลใบสั่งซื้อ หรือ ข้อมูล Master Product ไม่พร้อม")

# ==========================================
# TAB 3: Stock Report (Moved here)
# ==========================================
with tab3:
    st.subheader("📈 รายงาน Stock")
    if not df_master.empty and 'Product_ID' in df_master.columns:
        df_po_latest = pd.DataFrame()
        if not df_po.empty and 'Product_ID' in df_po.columns:
            df_po_latest = df_po.drop_duplicates(subset=['Product_ID'], keep='last')
        
        df_stock_report = pd.merge(df_master, df_po_latest, on='Product_ID', how='left')
        
        sales_map = {}
        if not df_sale.empty and 'Product_ID' in df_sale.columns:
            sales_summary = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
            sales_map = sales_summary.set_index('Product_ID')['Qty_Sold'].to_dict()
        
        df_stock_report['Qty_Sold'] = df_stock_report['Product_ID'].map(sales_map).fillna(0)
        if 'Initial_Stock' not in df_stock_report.columns: df_stock_report['Initial_Stock'] = 0
        df_stock_report['Current_Stock'] = df_stock_report['Initial_Stock'] - df_stock_report['Qty_Sold']
        df_stock_report['Status'] = df_stock_report['Current_Stock'].apply(lambda x: "🔴 หมดเกลี้ยง" if x<=0 else ("⚠️ ใกล้หมด" if x<10 else "🟢 มีของ"))

        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"""<div class="metric-card border-cyan"><div class="metric-title">สินค้าทั้งหมด</div><div class="metric-value text-cyan">{len(df_stock_report):,}</div></div>""", unsafe_allow_html=True)
        with c2: st.markdown(f"""<div class="metric-card border-gold"><div class="metric-title">ยอดขายรวม (All Time)</div><div class="metric-value text-gold">{int(df_stock_report['Qty_Sold'].sum()):,}</div></div>""", unsafe_allow_html=True)
        with c3: st.markdown(f"""<div class="metric-card border-red"><div class="metric-title">ต้องเติมของ</div><div class="metric-value text-red">{len(df_stock_report[df_stock_report['Current_Stock'] < 10]):,}</div></div>""", unsafe_allow_html=True)
        
        st.divider()
        if 'filter_status' not in st.session_state: st.session_state.filter_status = []
        if 'search_query' not in st.session_state: st.session_state.search_query = ""
        
        col_f1, col_f2, col_b1, col_b2, col_b3 = st.columns([2, 2, 0.4, 0.5, 0.5])
        with col_f1: st.multiselect("กรองสถานะ", ["📦 สินค้าทั้งหมด", "🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"], key="filter_status")
        with col_f2: st.text_input("ค้นหา (ชื่อ/รหัส)", key="search_query")
        with col_b1: 
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            st.button("❌ ล้าง", on_click=lambda: [st.session_state.update({'filter_status':[], 'search_query':''})])
        with col_b2: 
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            if st.button("📜 ประวัติ", type="secondary"): show_history_dialog()
        with col_b3: 
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            st.button("🔄", on_click=lambda: [st.cache_data.clear(), st.rerun()], type="primary")

        show_df = df_stock_report.copy()
        if st.session_state.filter_status and "📦 สินค้าทั้งหมด" not in st.session_state.filter_status:
            show_df = show_df[show_df['Status'].isin(st.session_state.filter_status)]
        if st.session_state.search_query:
            cond = pd.Series([False]*len(show_df), index=show_df.index)
            if 'Product_Name' in show_df.columns: cond = cond | show_df['Product_Name'].str.contains(st.session_state.search_query, case=False, na=False)
            if 'Product_ID' in show_df.columns: cond = cond | show_df['Product_ID'].str.contains(st.session_state.search_query, case=False, na=False)
            show_df = show_df[cond]

        for col in ["Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Price_Unit_NoVAT", "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan", "Shopee_Price", "TikTok_Price", "Fees", "Qty_Sold", "Current_Stock"]:
             if col in show_df.columns: show_df[col] = pd.to_numeric(show_df[col], errors='coerce').fillna(0)

        st.dataframe(
            show_df[[c for c in ["Product_ID", "Image", "Product_Name", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Total_Yuan", "Qty_Sold", "Current_Stock", "Status"] if c in show_df.columns]].style.map(lambda v: f'color: {"#ff4d4d" if float(v)<0 else "white"}' if isinstance(v, (int, float)) else None, subset=['Current_Stock']),
            column_config={
                "Product_ID": st.column_config.TextColumn("รหัสสินค้า", width=100),
                "Image": st.column_config.ImageColumn("รูปสินค้า", width=80),
                "Product_Name": st.column_config.TextColumn("ชื่อสินค้า", width=150), 
                "PO_Number": st.column_config.TextColumn("เลข PO ล่าสุด", width=100),
                "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width=100),
                "Received_Date": st.column_config.TextColumn("ของมา", width=100),
                "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d", width=80),
                "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d", width=80),
                "Total_Yuan": st.column_config.NumberColumn("ยอดหยวน", format="%.2f ¥", width=100),
                "Qty_Sold": st.column_config.NumberColumn("ยอดขาย", format="%d", width=80),
                "Current_Stock": st.column_config.NumberColumn("คงเหลือ", format="%d", width=80),
            },
            height=800, use_container_width=True, hide_index=True
        )
    else: st.warning("ไม่พบข้อมูล Master Product หรือข้อมูลไม่ถูกต้อง (ขาด Product_ID)")