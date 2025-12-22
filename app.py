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
# 3. ฟังก์ชันจัดการข้อมูล (Data Functions) - ครบถ้วน
# ==========================================

def highlight_negative(val):
    if isinstance(val, (int, float)):
        if val < 0: return 'color: #ff4b4b; font-weight: bold;'
    return ''

# 1. ฟังก์ชันอ่านข้อมูล PO (รองรับคอลัมน์ 'จำนวนที่ได้รับ')
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
            'วันที่สั่งซื้อ': 'Order_Date', 'วันที่ได้รับ': 'Received_Date', 
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
            if 'Product_ID' in df.columns: df['Product_ID'] = df['Product_ID'].astype(str)
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล PO ไม่ได้: {e}")
        return pd.DataFrame()

# 2. ฟังก์ชันอ่าน Master Stock (สำคัญมาก ห้ามหาย)
@st.cache_data(ttl=300)
def get_stock_from_sheet():
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_STOCK)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'รหัส': 'Product_ID',
            'ชื่อสินค้า': 'Product_Name', 'รูป': 'Image', 
            'หมวดหมู่': 'Product_Type', 'ประเภท': 'Product_Type',
            'Stock': 'Initial_Stock', 'จำนวน': 'Initial_Stock', 'ยกมา': 'Initial_Stock',
            'Min_Limit': 'Min_Limit'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})

        if not df.empty:
            if 'Initial_Stock' in df.columns:
                df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'], errors='coerce').fillna(0)
            else: df['Initial_Stock'] = 0 
            if 'Product_ID' in df.columns:
                df['Product_ID'] = df['Product_ID'].astype(str)
        return df
    except Exception as e:
        st.error(f"❌ อ่านข้อมูล Master Stock ไม่ได้: {e}")
        return pd.DataFrame()

# 3. ฟังก์ชันอ่านยอดขายจาก Folder
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
                if 'Product_ID' in temp_df.columns:
                    temp_df['Product_ID'] = temp_df['Product_ID'].astype(str)
                if not temp_df.empty: all_dfs.append(temp_df)
            except: continue
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ อ่านไฟล์ Excel Sale ไม่ทัน: {e}")
        return pd.DataFrame()

# --- ฟังก์ชันบันทึกแบบ Split (แก้ Column W) ---
def save_po_edit_split(row_index, current_row_data, new_row_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        formatted_curr = [item.strftime("%Y-%m-%d") if isinstance(item, (date, datetime)) else ("" if item is None else item) for item in current_row_data]
        range_name = f"A{row_index}:W{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        formatted_new = [item.strftime("%Y-%m-%d") if isinstance(item, (date, datetime)) else ("" if item is None else item) for item in new_row_data]
        ws.append_row(formatted_new)
        
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Split ไม่สำเร็จ: {e}")
        return False

# --- ฟังก์ชันบันทึกแบบ Update (แก้ Column W) ---
def save_po_edit_update(row_index, current_row_data):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        formatted_curr = [item.strftime("%Y-%m-%d") if isinstance(item, (date, datetime)) else ("" if item is None else item) for item in current_row_data]
        range_name = f"A{row_index}:W{row_index}" 
        ws.update(range_name, [formatted_curr])
        
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"❌ บันทึก Update ไม่สำเร็จ: {e}")
        return False

# --- ฟังก์ชันบันทึกแบบ Batch ---
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
        col_index = headers.index(target_col_name) + 1 if target_col_name in headers else len(headers) + 1
        if target_col_name not in headers: ws.update_cell(1, col_index, target_col_name)
        
        all_rows = ws.get_all_values()
        if len(all_rows) < 2: return
        pid_idx = next((i for i, h in enumerate(all_rows[0]) if h in ['รหัสสินค้า', 'รหัส', 'ID', 'Product_ID']), -1)
        if pid_idx == -1: return
        
        limit_map = df_edited.set_index('Product_ID')['Min_Limit'].to_dict()
        values_to_update = []
        for row in all_rows[1:]:
            pid = str(row[pid_idx]) if len(row) > pid_idx else ""
            old_val = int(row[col_index-1]) if len(row) >= col_index and str(row[col_index-1]).isdigit() else 10
            values_to_update.append([int(limit_map.get(pid, old_val))])

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
if "active_dialog" not in st.session_state: st.session_state.active_dialog = None 
if 'po_temp_cart' not in st.session_state: st.session_state.po_temp_cart = []

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    # Ensure types for merging
    if not df_master.empty: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty: df_po['Product_ID'] = df_po['Product_ID'].astype(str)
    if not df_sale.empty: df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

recent_sales_map = {}
latest_date_str = "ไม่พบข้อมูล"
if not df_sale.empty and 'Date_Only' in df_sale.columns:
    max_date = df_sale['Date_Only'].max()
    latest_date_str = max_date.strftime("%d/%m/%Y")
    recent_sales_map = df_sale[df_sale['Date_Only'] == max_date].groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()

# ==========================================
# 5. DIALOG FUNCTIONS
# ==========================================

@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    selected_pid = fixed_product_id
    if not selected_pid:
        if df_master.empty: return
        opts = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        sel = st.selectbox("🔍 ค้นหาสินค้า", opts, index=None)
        if sel: selected_pid = sel.split(" : ")[0]
    
    if selected_pid and not df_po.empty:
        hist = df_po[df_po['Product_ID'] == selected_pid].copy()
        if not hist.empty: st.dataframe(hist, use_container_width=True, hide_index=True)
        else: st.warning("ยังไม่มีประวัติการสั่งซื้อ")

@st.dialog("📝 บันทึกรับของ / แก้ไข PO", width="large")
def po_edit_dialog_v2():
    st.caption("📦 เลือกรายการ -> ระบุจำนวนที่ได้รับจริง")
    selected_row, row_index = None, None
    if not df_po.empty:
        po_map = {}
        for idx, row in df_po.iterrows():
            qty_ord = int(row.get('Qty_Ordered', 0))
            recv_date = str(row.get('Received_Date', '')).strip()
            is_recv = (recv_date != '' and recv_date.lower() != 'nat')
            icon = "✅ รับแล้ว" if is_recv else ("✅ ครบ/ปิด" if qty_ord <= 0 else "⏳ รอของ")
            txt = f"[{icon}] {row.get('PO_Number','-')} : {row.get('Product_ID','-')} (สั่ง: {qty_ord})"
            po_map[txt] = row
        
        search_key = st.selectbox("🔍 ค้นหารายการ", sorted(po_map.keys(), key=lambda x: "⏳" not in x), index=None)
        if search_key:
            selected_row = po_map[search_key]
            if 'Sheet_Row_Index' in selected_row: row_index = selected_row['Sheet_Row_Index']

    st.divider()
    if selected_row is not None and row_index is not None:
        def get_val(col, default): return selected_row.get(col, default)
        orig_qty = int(get_val('Qty_Ordered', 1))
        try: d_ord = datetime.strptime(str(get_val('Order_Date', date.today())), "%Y-%m-%d").date()
        except: d_ord = date.today()
        
        # UI
        with st.container(border=True):
            pid = str(get_val('Product_ID', '')).strip()
            name = get_val('Product_Name', '')
            st.subheader(f"สินค้า: {pid} | {name}")
            
            st.markdown("#### 📦 บันทึกการรับของ")
            c1, c2, c3 = st.columns([1.5, 1.5, 2])
            qty_recv = c1.number_input("จำนวนที่ได้รับจริง", min_value=1, value=orig_qty, key="e_qty_recv")
            d_recv = c2.date_input("วันที่ได้รับของ", date.today(), key="e_recv_date")
            
            rem_qty = orig_qty - qty_recv
            def_note = get_val('Note', '')
            if not def_note:
                if rem_qty > 0: def_note = f"รับบางส่วน {qty_recv} (ค้าง {rem_qty})"
                elif rem_qty < 0: def_note = f"เกิน {abs(rem_qty)} ชิ้น"
                else: def_note = "ได้รับครบ"
            note = c3.text_input("หมายเหตุ", value=def_note, key="e_note")
            
            if rem_qty > 0: st.warning(f"⚠️ ค้างส่ง {rem_qty} ชิ้น (ระบบจะแยกรายการ)")
            elif rem_qty < 0: st.info(f"ℹ️ ได้รับเกิน {abs(rem_qty)} ชิ้น")
            else: st.success("✅ รับครบ")

            with st.expander("💰 แก้ไขข้อมูลต้นทุน"):
                ec1, ec2, ec3 = st.columns(3)
                yuan = ec1.number_input("ราคารวม (หยวน)", value=float(get_val('Total_Yuan', 0)), key="e_yuan")
                rate = ec2.number_input("เรทเงิน", value=float(get_val('Yuan_Rate', 5.0)), key="e_rate")
                cbm_orig = float(get_val('CBM', 0))
                cbm_sugg = (cbm_orig / orig_qty) * qty_recv if orig_qty > 0 else cbm_orig
                cbm = ec1.number_input("CBM (ตามยอดรับ)", value=float(cbm_sugg), format="%.4f", key="e_cbm")
                ship_rate = ec2.number_input("เรทขนส่ง", value=float(get_val('Ship_Rate', 5000)), key="e_ship_rate")
                weight = ec3.number_input("น้ำหนัก KG", value=float(get_val('Transport_Weight', 0)), key="e_weight")
                link = st.text_input("Link", value=get_val('Link', ''), key="e_link")
        
        if st.button("💾 บันทึกรับของ", type="primary"):
            y_recv = (float(get_val('Total_Yuan', 0))/orig_qty)*qty_recv if (yuan == float(get_val('Total_Yuan', 0)) and orig_qty > 0) else yuan
            thb_recv = (y_recv * rate) + (cbm * ship_rate)
            unit_cost = thb_recv / qty_recv if qty_recv > 0 else 0
            
            rem_yuan = float(get_val('Total_Yuan', 0)) - y_recv
            rem_cbm = float(get_val('CBM', 0)) - cbm
            if rem_cbm < 0: rem_cbm = 0

            # Data 23 Columns
            base_info = [
                get_val('Product_ID', ''), get_val('PO_Number', ''), get_val('Transport_Type', ''), d_ord.strftime("%Y-%m-%d"),
                get_val('Shopee_Price',0), get_val('Lazada_Price',0), get_val('TikTok_Price',0), link, get_val('WeChat', '')
            ]
            
            row_remain = [
                base_info[0], base_info[1], base_info[2], base_info[3],
                None, 0, rem_qty, 0, 
                0, round(rem_yuan, 2), 0, rate, ship_rate, round(rem_cbm, 4), 0, weight,
                0, base_info[4], base_info[5], base_info[6], f"รอรับส่วนที่เหลือ ({rem_qty})", base_info[7], base_info[8]
            ]
            
            row_recv = [
                base_info[0], base_info[1], base_info[2], base_info[3],
                d_recv.strftime("%Y-%m-%d"), (d_recv - d_ord).days, qty_recv, qty_recv,
                unit_cost, round(y_recv, 2), round(thb_recv, 2), rate, ship_rate, round(cbm, 4), round(cbm*ship_rate, 2), weight,
                round(y_recv/qty_recv, 4) if qty_recv else 0, base_info[4], base_info[5], base_info[6], note, base_info[7], base_info[8]
            ]

            if rem_qty > 0: success = save_po_edit_split(row_index, row_remain, row_recv)
            else:
                if rem_qty < 0: row_recv[6] = orig_qty 
                success = save_po_edit_update(row_index, row_recv)
            
            if success:
                st.success("✅ บันทึกเรียบร้อย"); st.session_state.active_dialog = None; time.sleep(1); st.rerun()

@st.dialog("📝 บันทึกข้อมูลการสั่งซื้อ (Batch PO)", width="large")
def po_batch_dialog():
    st.caption("💡 กรอกข้อมูลสินค้า -> กดเพิ่มลงตระกร้า -> กดบันทึก (สถานะ: รอรับของ)")
    if st.session_state.get("need_reset", False):
        for k in ["bp_qty", "bp_cost", "bp_cbm", "bp_note"]: 
            if k in st.session_state: del st.session_state[k]
        st.session_state["need_reset"] = False

    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        po_num = c1.text_input("เลข PO", key="bp_po")
        trans = c2.selectbox("ขนส่ง", ["ทางรถ🚚", "ทางเรือ🚤", "ทางอากาศ✈️"], key="bp_trans")
        d_ord = c3.date_input("วันที่สั่ง", date.today(), key="bp_date")

    with st.container(border=True):
        prods = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist() if not df_master.empty else []
        sel_prod = st.selectbox("เลือกสินค้า", prods, index=None, key="bp_prod")
        
        img_url = ""
        if sel_prod:
            pid = sel_prod.split(" : ")[0]
            row = df_master[df_master['Product_ID'] == pid]
            if not row.empty: img_url = row.iloc[0].get('Image', '')

        ic, fc = st.columns([1, 3])
        with ic: 
            if img_url: st.image(img_url, width=100)
        with fc:
            r1c1, r1c2, r1c3 = st.columns(3)
            qty = r1c1.number_input("จำนวนสั่ง", min_value=1, value=100, key="bp_qty")
            cost = r1c2.number_input("ต้นทุน (หยวน)", min_value=0.0, step=0.01, key="bp_cost")
            rate = r1c3.number_input("เรทเงิน", value=5.0, step=0.01, key="bp_rate")
            
            r2c1, r2c2, r2c3 = st.columns(3)
            cbm = r2c1.number_input("CBM", min_value=0.0, format="%.4f", key="bp_cbm")
            ship_rate = r2c2.number_input("เรทขนส่ง", value=5000.0, key="bp_ship")
            weight = r2c3.number_input("น้ำหนัก KG", min_value=0.0, key="bp_w")
            
            is_per_piece = st.checkbox("CBM ต่อชิ้น", value=False)
            note = st.text_input("หมายเหตุ", key="bp_note")
            
            with st.expander("เพิ่มเติม"):
                link = st.text_input("Link", key="bp_l"); wechat = st.text_input("WeChat", key="bp_wc")
                p1, p2, p3 = st.columns(3)
                s_p = p1.number_input("Shopee", key="bp_s"); l_p = p2.number_input("Lazada", key="bp_lz"); t_p = p3.number_input("TikTok", key="bp_t")

    if st.button("➕ เพิ่มรายการ", type="primary", disabled=(not po_num or not sel_prod)):
        pid = sel_prod.split(" : ")[0]
        final_cbm = cbm * qty if is_per_piece else cbm
        ship_cost = final_cbm * ship_rate
        
        item = {
            "SKU": pid, "PO": po_num, "Trans": trans, "Ord": str(d_ord),
            "Qty": int(qty), "Yuan": cost, "Rate": rate, "ShipRate": ship_rate,
            "CBM": final_cbm, "ShipCost": ship_cost, "W": weight,
            "Shopee": s_p, "Laz": l_p, "Tik": t_p, "Note": note, "Link": link, "WeChat": wechat
        }
        st.session_state.po_temp_cart.append(item)
        st.toast(f"เพิ่ม {pid} แล้ว"); st.session_state["need_reset"] = True; st.rerun()

    if st.session_state.po_temp_cart:
        st.divider()
        st.dataframe(pd.DataFrame(st.session_state.po_temp_cart)[["SKU", "Qty", "Yuan", "Note"]], hide_index=True)
        if st.button("💾 บันทึก PO ทั้งหมด", type="primary"):
            rows = []
            for i in st.session_state.po_temp_cart:
                # 23 Columns (Index 7 is QtyRecv = 0)
                row = [
                    i["SKU"], i["PO"], i["Trans"], i["Ord"], 
                    "", 0, i["Qty"], 0,
                    0, i["Yuan"], 0, i["Rate"], i["ShipRate"], i["CBM"], i["ShipCost"], i["W"],
                    0, i["Shopee"], i["Laz"], i["Tik"], i["Note"], i["Link"], i["WeChat"]
                ]
                rows.append(row)
            
            if save_po_batch_to_sheet(rows):
                st.success("✅ บันทึก PO สำเร็จ!"); st.session_state.po_temp_cart = []
                st.session_state.active_dialog = None; time.sleep(1); st.rerun()

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])

# --- TAB 1 ---
with tab1:
    st.subheader("📅 สรุปยอดขายรายวัน")
    if "history_pid" in st.query_params:
        show_history_dialog(st.query_params["history_pid"]); st.query_params.clear()

    today = date.today()
    all_years = [today.year - i for i in range(3)]
    thai_months = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน", 
                   "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]

    def update_m_dates():
        y = st.session_state.m_y
        m_index = thai_months.index(st.session_state.m_m) + 1
        _, last_day = calendar.monthrange(y, m_index)
        st.session_state.m_d_start = date(y, m_index, 1)
        st.session_state.m_d_end = date(y, m_index, last_day)

    if "m_d_start" not in st.session_state: st.session_state.m_d_start = date(today.year, today.month, 1)
    if "m_d_end" not in st.session_state: st.session_state.m_d_end = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])

    with st.container(border=True):
        c_y, c_m, c_s, c_e = st.columns([1, 1.5, 1.5, 1.5])
        with c_y: st.selectbox("ปี", all_years, key="m_y", on_change=update_m_dates)
        with c_m: st.selectbox("เดือน", thai_months, index=today.month-1, key="m_m", on_change=update_m_dates)
        with c_s: st.date_input("เริ่ม", key="m_d_start")
        with c_e: st.date_input("สิ้นสุด", key="m_d_end")
        
        st.divider()
        use_focus = st.checkbox("🔎 กรองเฉพาะวันที่มียอดขาย")
        focus_date = None
        if use_focus: focus_date = st.date_input("เลือกวันที่", value=today)

    if not df_sale.empty:
        mask = (df_sale['Date_Only'] >= st.session_state.m_d_start) & (df_sale['Date_Only'] <= st.session_state.m_d_end)
        df_sub = df_sale.loc[mask].copy()
        
        if not df_sub.empty:
            df_sub['D'] = df_sub['Order_Time'].apply(lambda x: f"{x.day}")
            pivot = df_sub.groupby(['Product_ID', 'D'])['Qty_Sold'].sum().unstack(fill_value=0)
            
            if use_focus and focus_date:
                sold_today = df_sale[(df_sale['Date_Only'] == focus_date) & (df_sale['Qty_Sold'] > 0)]['Product_ID'].unique()
                pivot = pivot[pivot.index.isin(sold_today)]
            
            final = pd.merge(df_master, pivot, on='Product_ID', how='left').fillna(0) if not pivot.empty else df_master.copy()
            cols = sorted([c for c in final.columns if str(c).isdigit()], key=int)
            final['Total'] = final[cols].sum(axis=1) if cols else 0
            
            st.markdown("---")
            html = """<div style='overflow-x:auto'><table style='width:100%; border-collapse:collapse; color:white; font-size:12px;'>
            <tr style='background:#1e3c72; border-bottom:2px solid white;'>
            <th>History</th><th>ID</th><th>รูป</th><th>ชื่อสินค้า</th><th>Stock</th><th>รวมขาย</th>"""
            for c in cols: html += f"<th>{c}</th>"
            html += "</tr>"
            
            for _, r in final.iterrows():
                if r.get('Total', 0) == 0 and not use_focus: continue
                stock = int(r.get('Initial_Stock',0)) - recent_sales_map.get(str(r['Product_ID']),0)
                img = f"<img src='{r['Image']}' width='40'>" if str(r.get('Image','')).startswith('http') else ""
                hist_link = f"<a href='?history_pid={r['Product_ID']}' target='_self' style='text-decoration:none;font-size:16px;'>📜</a>"
                
                html += f"<tr style='border-bottom:1px solid #333; text-align:center;'><td>{hist_link}</td><td>{r['Product_ID']}</td><td>{img}</td><td style='text-align:left'>{r['Product_Name']}</td><td>{stock}</td><td>{int(r['Total'])}</td>"
                for c in cols:
                    val = int(r[c])
                    color = "color:#ff4b4b;font-weight:bold" if val < 0 else ""
                    html += f"<td style='{color}'>{val if val!=0 else ''}</td>"
                html += "</tr>"
            html += "</table></div>"
            st.markdown(html, unsafe_allow_html=True)
        else: st.info("ไม่พบยอดขายในช่วงนี้")

# --- TAB 2 ---
with tab2:
    h, b = st.columns([3, 1])
    h.subheader("📋 รายการสั่งซื้อ")
    if b.button("➕ เพิ่ม PO", type="primary"): st.session_state.active_dialog = "po_batch"; st.rerun()
    if b.button("🔍 รับของ / แก้ไข"): st.session_state.active_dialog = "po_search"; st.rerun()

    if not df_po.empty:
        df_show = df_po.copy()
        valid_cols = [c for c in ['Product_ID', 'Product_Name', 'Image', 'Product_Type'] if c in df_master.columns]
        df_show = pd.merge(df_show, df_master[valid_cols], on='Product_ID', how='left')
        
        status_filter = st.radio("สถานะ:", ["ทั้งหมด", "รอของ", "รับแล้ว"], horizontal=True)
        if status_filter == "รอของ":
            df_show = df_show[(df_show['Received_Date'] == "") | (df_show['Received_Date'].isna())]
        elif status_filter == "รับแล้ว":
             df_show = df_show[(df_show['Received_Date'] != "") & (df_show['Received_Date'].notna())]

        st.dataframe(
            df_show[['PO_Number','Image','Product_ID','Product_Name','Qty_Ordered','Qty_Received','Order_Date','Received_Date','Note']],
            column_config={"Image": st.column_config.ImageColumn("รูป", width=50)},
            use_container_width=True, hide_index=True
        )

# --- TAB 3 ---
with tab3:
    st.subheader("📈 Stock Monitor")
    if not df_master.empty:
        stock_df = df_master.copy()
        stock_df['Sold_Today'] = stock_df['Product_ID'].map(recent_sales_map).fillna(0)
        stock_df['Current'] = stock_df['Initial_Stock'] - stock_df['Sold_Today']
        
        edited = st.data_editor(
            stock_df[['Product_ID','Image','Product_Name','Current','Min_Limit']],
            column_config={
                "Image": st.column_config.ImageColumn(width=50),
                "Product_ID": st.column_config.TextColumn(disabled=True),
                "Min_Limit": st.column_config.NumberColumn("จุดเตือน 🔔")
            },
            use_container_width=True, hide_index=True, key="stock_edit"
        )
        if st.button("💾 บันทึกการตั้งค่า Stock"):
            update_master_limits(edited); st.rerun()

# Execution
if st.session_state.active_dialog == "po_batch": po_batch_dialog()
elif st.session_state.active_dialog == "po_search": po_edit_dialog_v2()