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
    
    /* --- แก้ไข CSS หัวตารางตรงนี้ --- */
    [data-testid="stDataFrame"] th { 
        text-align: center !important; 
        background-color: #1e3c72 !important; /* เปลี่ยนเป็นสีน้ำเงินเข้มตามต้นฉบับ */
        color: white !important; 
        vertical-align: middle !important; 
        min-height: 60px; 
        font-size: 14px; 
        border-bottom: 2px solid #ffffff !important; 
    }
    /* -------------------------------- */
    
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

# --- Helper: Highlight Negative Numbers Red ---
def highlight_negative(val):
    """ใส่สีแดงให้กับตัวเลขที่น้อยกว่า 0"""
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
        
        col_map = {
            'รหัสสินค้า': 'Product_ID', 'รหัส': 'Product_ID', 'ID': 'Product_ID',
            'ชื่อสินค้า': 'Product_Name', 'ชื่อ': 'Product_Name', 'Name': 'Product_Name',
            'รูป': 'Image', 'รูปภาพ': 'Image', 'Link รูป': 'Image',
            'Stock': 'Initial_Stock', 'จำนวน': 'Initial_Stock', 'สต็อก': 'Initial_Stock',
            'Min_Limit': 'Min_Limit', 'Min': 'Min_Limit', 'จุดเตือน': 'Min_Limit'
        }
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
        # Ensure integer for stock
        if 'Initial_Stock' in df.columns:
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
        if not df.empty:
            df['Sheet_Row_Index'] = range(2, len(df) + 2)
            # Ensure integers for Qtys
            for col in ['Qty_Ordered', 'Qty_Remaining']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
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
                    # Convert to numeric first, then fillna, then to int
                    temp_df['Qty_Sold'] = pd.to_numeric(temp_df['Qty_Sold'], errors='coerce').fillna(0).astype(int)
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
            time.sleep(1) 
        else:
            col_index = headers.index(target_col_name) + 1
            
        all_rows = ws.get_all_values()
        if len(all_rows) < 2: return
        
        header_row = all_rows[0]
        try:
            pid_idx = -1
            for i, h in enumerate(header_row):
                if h in ['รหัสสินค้า', 'รหัส', 'ID', 'Product_ID']:
                    pid_idx = i
                    break
            if pid_idx == -1: raise Exception("หาคอลัมน์รหัสสินค้าไม่เจอ")
            
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
            time.sleep(1)
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
if "selected_product_history" not in st.session_state: st.session_state.selected_product_history = None
if 'filter_status' not in st.session_state: st.session_state.filter_status = []
if 'search_query' not in st.session_state: st.session_state.search_query = ""

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    
    if not df_master.empty and 'Product_ID' in df_master.columns: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty and 'Product_ID' in df_po.columns: df_po['Product_ID'] = df_po['Product_ID'].astype(str)
    if not df_sale.empty and 'Product_ID' in df_sale.columns: df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)

# ==========================================
# 🛠️ PREPARE DATA: หาข้อมูลยอดขาย "วันล่าสุด" เพื่อใช้ตัดสต็อก
# ==========================================
recent_sales_map = {}
latest_date_str = "ไม่พบข้อมูล"

if not df_sale.empty and 'Date_Only' in df_sale.columns:
    # 1. หาวันที่มากที่สุด (ล่าสุด)
    max_date = df_sale['Date_Only'].max()
    latest_date_str = max_date.strftime("%d/%m/%Y")
    
    # 2. กรองเอาเฉพาะรายการของวันนั้น
    df_latest_sale = df_sale[df_sale['Date_Only'] == max_date]
    
    # 3. รวมยอดขายรายสินค้าของวันล่าสุด
    recent_sales_map = df_latest_sale.groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()

# ==========================================
# 5. DIALOG FUNCTIONS
# ==========================================
@st.dialog("📜 ประวัติการสั่งซื้อสินค้า", width="large")
def show_history_dialog(fixed_product_id=None):
    selected_pid = fixed_product_id
    if not selected_pid:
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty or df_po.empty:
            st.info("ไม่มีข้อมูลสินค้าหรือประวัติการสั่งซื้อ")
            return
        if 'Product_ID' in df_master.columns and 'Product_Name' in df_master.columns:
            product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
            selected_product = st.selectbox("🔍 ค้นหาสินค้า", options=product_options, index=None, placeholder="พิมพ์เพื่อค้นหา...", key="hist_search_box")
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
                
                # ✅ ปรับหัวตารางเป็นภาษาไทยทั้งหมด ตามที่คุณต้องการ
                st.dataframe(
                    history_df.style.map(highlight_negative), 
                    column_config={
                        "Product_ID": st.column_config.TextColumn("รหัสสินค้า"),
                        "PO_Number": st.column_config.TextColumn("เลข PO", width="medium"),
                        "Order_Date": st.column_config.TextColumn("วันที่สั่งซื้อ", width="medium"),
                        "Received_Date": st.column_config.TextColumn("วันที่ได้รับ", width="medium"),
                        "Qty_Ordered": st.column_config.NumberColumn("จำนวนสั่ง", format="%d"),
                        "Qty_Remaining": st.column_config.NumberColumn("คงเหลือ", format="%d"),
                        "Transport_Type": st.column_config.TextColumn("ขนส่งทาง"),
                        "Transport_Weight": st.column_config.TextColumn("รายละเอียด/นน."),
                        "Yuan_Rate": st.column_config.NumberColumn("เรทเงิน", format="%.2f"),
                        
                        # --- รายการที่เพิ่มและปรับปรุงชื่อ ---
                        "Total_Yuan": st.column_config.NumberColumn("ราคาหยวนทั้งหมด", format="%.2f ¥"),
                        "Price_Unit_NoVAT": st.column_config.NumberColumn("ราคาต่อชิ้นไม่รวม VAT", format="%.2f"),
                        "Price_1688_NoShip": st.column_config.NumberColumn("ราคา1688/1 ชิ้น ไม่รวมค่าส่ง", format="%.2f"),
                        "Price_1688_WithShip": st.column_config.NumberColumn("ราคา 1688/1 ชิ้น รวมค่าส่ง", format="%.2f"),
                        "Shopee_Price": st.column_config.NumberColumn("ราคาในช้อปปี้", format="%.2f"),
                        "TikTok_Price": st.column_config.NumberColumn("ราคาใน TIKTOK", format="%.2f"),
                        "Fees": st.column_config.NumberColumn("ค่าธรรมเนียม", format="%.2f"),
                        "Wait_Days": st.column_config.NumberColumn("ระยะเวลารอสินค้า", format="%d วัน"),
                        "Wait_Date": st.column_config.NumberColumn("ระยะเวลารอสินค้า", format="%d วัน"), # เผื่อกรณีชื่อคอลัมน์ต่างกัน
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
    kp = f"d_{mode}"
    selected_suffix = "" 

    if mode == "search":
        st.markdown("### 🔍 ค้นหา PO")
        if not df_po.empty: 
            po_map = {f"{row['PO_Number']} (สินค้า: {row['Product_ID']})": row for _, row in df_po.iterrows()}
            selected_key = st.selectbox(
                "เลือกรายการ PO", options=list(po_map.keys()), index=None, placeholder="พิมพ์เพื่อค้นหา PO...", key=f"{kp}_sel"
            )
            if selected_key:
                d = po_map[selected_key].to_dict()
                selected_suffix = f"_{selected_key}" 
                if 'Sheet_Row_Index' in d: sheet_row_index = int(d['Sheet_Row_Index'])
                else: 
                    match_row = df_po[(df_po['PO_Number']==d['PO_Number']) & (df_po['Product_ID']==d['Product_ID'])]
                    if not match_row.empty: sheet_row_index = match_row.index[0] + 2
        else:
            st.warning("ยังไม่มีข้อมูล PO"); return

    def clear_form_data():
        keys_to_clear = [
            f"{kp}_po{selected_suffix}", f"{kp}_w{selected_suffix}", 
            f"{kp}_qord{selected_suffix}", f"{kp}_qrem{selected_suffix}", 
            f"{kp}_rate{selected_suffix}", f"{kp}_fee{selected_suffix}", 
            f"{kp}_pnov{selected_suffix}", f"{kp}_p1688n{selected_suffix}", 
            f"{kp}_p1688s{selected_suffix}", f"{kp}_pshop{selected_suffix}", 
            f"{kp}_ptik{selected_suffix}", f"{kp}_toty{selected_suffix}"
        ]
        for k in keys_to_clear:
            if k in st.session_state: st.session_state[k] = None

    if 'Product_ID' in df_master.columns and 'Product_Name' in df_master.columns:
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        default_idx = None
        if mode == "search" and "Product_ID" in d:
             matches = [i for i, opt in enumerate(product_options) if opt.startswith(str(d["Product_ID"]) + " :")]
             if matches: default_idx = matches[0]
        selected_option = st.selectbox(
            "ระบุสินค้า", product_options, index=default_idx, placeholder="🔍 Search...", label_visibility="collapsed", key=f"{kp}_prod{selected_suffix}"
        )
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
                try: return datetime.strptime(str(val), "%Y-%m-%d").date()
                except: return None
            def vn(k): 
                val = d.get(k)
                if mode == "search": return float(val) if val is not None and str(val).strip() != "" else 0.0
                else: return float(val) if val and float(val)!=0 else None

            r1c1, r1c2, r1c3 = st.columns(3)
            po_num = r1c1.text_input("เลข PO *", value=d.get("PO_Number") if mode=="search" else None, key=f"{kp}_po{selected_suffix}")
            order_date = r1c2.date_input("วันที่สั่ง", value=get_date_val(d.get("Order_Date")) if mode=="search" else date.today(), key=f"{kp}_odate{selected_suffix}")
            recv_date = r1c3.date_input("ของมา (ประมาณ)", value=get_date_val(d.get("Received_Date")), key=f"{kp}_rdate{selected_suffix}")
            weight_txt = st.text_area("📦 น้ำหนักขนส่ง / รายละเอียด *", value=d.get("Transport_Weight") if mode=="search" else None, height=100, key=f"{kp}_w{selected_suffix}")
            
            r3c1, r3c2, r3c3, r3c4 = st.columns(4)
            # Use step=1.0 for integers if you prefer, but here keeping standard number input
            qty_ord = r3c1.number_input("สั่งมา *", min_value=0, step=1, value=int(vn("Qty_Ordered") or 0), key=f"{kp}_qord{selected_suffix}") 
            qty_rem = r3c2.number_input("เหลือ *", min_value=0, step=1, value=int(vn("Qty_Remaining") or 0), key=f"{kp}_qrem{selected_suffix}")
            yuan_rate = r3c3.number_input("เรทหยวน *", min_value=0.0, step=0.0, format="%.2f", value=vn("Yuan_Rate"), key=f"{kp}_rate{selected_suffix}")
            fees = r3c4.number_input("ค่าธรรมเนียม", min_value=0.0, step=0.0, format="%.2f", value=vn("Fees"), key=f"{kp}_fee{selected_suffix}")
            
            r4c1, r4c2, r4c3 = st.columns(3)
            p_no_vat = r4c1.number_input("ราคาต่อชิ้นไม่รวม VAT", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_Unit_NoVAT"), key=f"{kp}_pnov{selected_suffix}")
            p_1688_noship = r4c2.number_input("ราคา 1688 ไม่รวมส่ง", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_1688_NoShip"), key=f"{kp}_p1688n{selected_suffix}")
            p_1688_ship = r4c3.number_input("ราคา 1688 รวมส่ง *", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_1688_WithShip"), key=f"{kp}_p1688s{selected_suffix}")

            r5c1, r5c2, r5c3 = st.columns(3)
            p_shopee = r5c1.number_input("Shopee", min_value=0.0, step=0.0, format="%.2f", value=vn("Shopee_Price"), key=f"{kp}_pshop{selected_suffix}")
            p_tiktok = r5c2.number_input("TikTok", min_value=0.0, step=0.0, format="%.2f", value=vn("TikTok_Price"), key=f"{kp}_ptik{selected_suffix}")
            transport = r5c3.selectbox("การขนส่ง", ["ส่งทางรถ 🚛", "ส่งทางเรือ 🚢"], index=1 if d.get("Transport_Type") == "ส่งทางเรือ 🚢" else 0, key=f"{kp}_trans{selected_suffix}")
            
            st.markdown("---")
            f_col1, f_col2, f_col3 = st.columns([1.5, 0.75, 0.75])
            with f_col1: total_yuan_input = st.number_input("ราคาหยวนทั้งหมด *", min_value=0.0, step=0.0, format="%.2f", value=vn("Total_Yuan"), key=f"{kp}_toty{selected_suffix}")
            with f_col2: 
                st.write(""); st.write("") 
                if mode == "add": st.button("🧹 ล้าง", on_click=clear_form_data, key=f"{kp}_clr{selected_suffix}", type="secondary")
            with f_col3:
                st.write(""); st.write("")
                if st.button("✅ บันทึก" if mode == "add" else "💾 บันทึกทับ", type="primary", key=f"{kp}_sub{selected_suffix}"):
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
tab1, tab2, tab3 = st.tabs(["📅 สรุปยอดขายรายวัน", "📝 รายการสั่งซื้อ", "📈 รายงาน Stock"])

dialog_action = None 
dialog_data = None

# ==========================================
# TAB 1: Daily Sales Report (ฉบับสมบูรณ์ - HTML Table + Popup)
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

    # --- FILTER SECTION ---
    with st.container(border=True):
        st.markdown("##### 🔍 ตัวกรองช่วงเวลา (Main Range)")
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
            with col_sec_date:
                focus_date = st.date_input("ระบุวันที่ขาย (Focus Date):", value=today, key="filter_focus_date")

    start_date = st.session_state.m_d_start
    end_date = st.session_state.m_d_end
    
    if start_date and end_date:
        if start_date > end_date: 
            st.error("⚠️ วันที่เริ่มต้นต้องมาก่อนวันที่สิ้นสุด")
        else:
            if not df_sale.empty and 'Date_Only' in df_sale.columns:
                
                # 1. Prepare Data
                mask_range = (df_sale['Date_Only'] >= start_date) & (df_sale['Date_Only'] <= end_date)
                df_sale_range = df_sale.loc[mask_range].copy()
                
                if not df_sale_range.empty:
                    thai_abbr = ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
                    df_sale_range['Day_Col'] = df_sale_range['Order_Time'].apply(lambda x: f"{x.day} {thai_abbr[x.month]}")
                    df_sale_range['Day_Sort'] = df_sale_range['Order_Time'].dt.strftime('%Y%m%d')
                    
                    pivot_data = df_sale_range.groupby(['Product_ID', 'Day_Col', 'Day_Sort'])['Qty_Sold'].sum().reset_index()
                    df_pivot = pivot_data.pivot(index='Product_ID', columns='Day_Col', values='Qty_Sold').fillna(0).astype(int)
                    
                    if use_focus_date and focus_date:
                        products_sold_on_focus = df_sale[
                            (df_sale['Date_Only'] == focus_date) & 
                            (df_sale['Qty_Sold'] > 0)
                        ]['Product_ID'].unique()
                        df_pivot = df_pivot[df_pivot.index.isin(products_sold_on_focus)]

                    if df_pivot.empty:
                        msg_suffix = f"ในวันที่ {focus_date.strftime('%d/%m/%Y')}" if use_focus_date else "ในช่วงเวลาที่เลือก"
                        st.warning(f"⚠️ ไม่พบสินค้าที่มียอดขาย {msg_suffix}")
                    else:
                        sorted_cols = sorted(df_pivot.columns, key=lambda x: pivot_data[pivot_data['Day_Col'] == x]['Day_Sort'].values[0] if x in pivot_data['Day_Col'].values else 0)
                        df_pivot = df_pivot[sorted_cols]
                        df_pivot['Total_Sales_Range'] = df_pivot.sum(axis=1).astype(int)
                        df_pivot = df_pivot.reset_index()
                        
                        stock_map = {}
                        if not df_master.empty and 'Initial_Stock' in df_master.columns:
                            stock_map = df_master.set_index('Product_ID')['Initial_Stock'].to_dict()
                        
                        if not df_master.empty:
                            final_report = pd.merge(df_pivot, df_master[['Product_ID', 'Product_Name', 'Image']], on='Product_ID', how='left')
                        else:
                            final_report = df_pivot; final_report['Product_Name'] = ""; final_report['Image'] = ""

                        final_report['Current_Stock'] = final_report['Product_ID'].apply(lambda x: stock_map.get(x, 0) - recent_sales_map.get(x, 0)).astype(int)
                        final_report['Status'] = final_report['Current_Stock'].apply(lambda x: "🔴 หมด" if x<=0 else ("⚠️ ต่ำ" if x<10 else "🟢 ปกติ"))
                        
                        fixed_cols = ['Product_ID', 'Image', 'Product_Name', 'Current_Stock', 'Total_Sales_Range', 'Status']
                        day_cols = [c for c in final_report.columns if c not in fixed_cols and c in sorted_cols]
                        
                        # สร้าง DataFrame สุดท้าย
                        final_df = final_report[fixed_cols + day_cols].copy()
                        
                        # ======================================================
                        # 🎨 เริ่มสร้างตารางแบบ HTML Hybrid (Custom Table)
                        # ======================================================
                        st.divider()
                        st.markdown(f"**📊 รายการสินค้า ({len(final_df)} รายการ)**")

                        # 1. กำหนดสัดส่วนคอลัมน์ [ปุ่ม, รหัส, รูป, ชื่อ, คงเหลือ, ยอดขาย, สถานะ, ...วันต่างๆ]
                        # ปรับตัวเลขที่นี่เพื่อขยาย/ลดความกว้างช่อง
                        col_ratios = [0.6, 1.2, 0.8, 3.5, 1, 1.2, 1.2] + [0.8] * len(day_cols)
                        
                        # 2. CSS สำหรับตาราง (หัวสีน้ำเงิน #1e3c72)
                        st.markdown("""
                        <style>
                            .tbl-header { 
                                background-color: #1e3c72; 
                                color: white; 
                                padding: 12px 5px; 
                                text-align: center; 
                                font-weight: bold; 
                                border-right: 1px solid #ffffff30;
                                font-size: 14px;
                                height: 100%;
                                display: flex; align-items: center; justify-content: center;
                                margin-bottom: 5px;
                            }
                            .tbl-cell {
                                padding: 8px 5px;
                                text-align: center;
                                font-size: 14px;
                                display: flex; align-items: center; justify-content: center;
                                height: 50px; 
                                width: 100%;
                            }
                            /* ปรับปุ่มให้ดูสวยงาม */
                            div[data-testid="stButton"] button {
                                border: 1px solid #444;
                                background-color: #333;
                                color: white;
                                padding: 0px;
                                height: 35px;
                                width: 100%;
                                margin: 5px auto;
                            }
                            div[data-testid="stButton"] button:hover {
                                border-color: #00d2ff;
                                color: #00d2ff;
                                background-color: #444;
                            }
                            div[data-testid="stButton"] button:active {
                                background-color: #1e3c72;
                                color: white;
                            }
                        </style>
                        """, unsafe_allow_html=True)

                        # 3. วาดหัวตาราง (Header Row)
                        cols = st.columns(col_ratios)
                        headers = ["ประวัติ", "รหัส", "รูป", "ชื่อสินค้า", "คงเหลือ", "ยอดขายรวม", "สถานะ"] + day_cols
                        
                        for i, h in enumerate(headers):
                            # ใส่ Border Radius ที่หัวมุมซ้ายและขวา
                            radius_style = "border-top-left-radius: 8px; border-bottom-left-radius: 8px;" if i==0 else ("border-top-right-radius: 8px; border-bottom-right-radius: 8px;" if i==len(headers)-1 else "")
                            cols[i].markdown(f'<div class="tbl-header" style="{radius_style}">{h}</div>', unsafe_allow_html=True)

                        # 4. ฟังก์ชันช่วยสร้าง HTML Cell
                        def make_html(val, bg, is_img=False, align="center"):
                            color = "#ffffff"
                            weight = "normal"
                            display_val = val
                            
                            # Logic สีแดงถ้าติดลบ
                            if isinstance(val, (int, float)):
                                if val < 0: 
                                    color = "#ff4b4b"
                                    weight = "bold"
                                display_val = f"{val:,}" # ใส่ comma
                            
                            # Logic รูปภาพ
                            if is_img:
                                if val and str(val).lower() != 'nan': 
                                    return f'<div class="tbl-cell" style="background-color:{bg};"><img src="{val}" style="max-height:40px; border-radius:4px;"></div>'
                                else: 
                                    return f'<div class="tbl-cell" style="background-color:{bg};">-</div>'
                            
                            return f'<div class="tbl-cell" style="background-color:{bg}; color:{color}; font-weight:{weight}; justify-content:{align};">{display_val}</div>'

                        # 5. วนลูปสร้างข้อมูล (Data Rows)
                        for idx, row in final_df.iterrows():
                            # คำนวณสีพื้นหลังสลับ (Zebra Striping)
                            bg_color = "#2e2e2e" if idx % 2 == 0 else "#1c1c1c"
                            
                            c = st.columns(col_ratios)
                            
                            # [Col 0] ปุ่มประวัติ (Interactive Streamlit Button)
                            with c[0]:
                                # ใช้ markdown สร้างพื้นหลังให้เต็มช่องปุ่ม
                                st.markdown(f"""
                                    <div style="background-color:{bg_color}; height:50px; position:absolute; top:0; left:0; width:120%; z-index:-1; margin-left:-5px;"></div>
                                """, unsafe_allow_html=True)
                                if st.button("📜", key=f"btn_hist_{row['Product_ID']}", help=f"ดูประวัติ {row['Product_ID']}"):
                                    show_history_dialog(fixed_product_id=row['Product_ID'])
                            
                            # [Col 1] รหัส
                            c[1].markdown(make_html(row['Product_ID'], bg_color), unsafe_allow_html=True)
                            
                            # [Col 2] รูป
                            c[2].markdown(make_html(row['Image'], bg_color, is_img=True), unsafe_allow_html=True)
                            
                            # [Col 3] ชื่อสินค้า (จัดชิดซ้ายแต่อยู่กลางบรรทัด - flex-start)
                            c[3].markdown(make_html(row['Product_Name'], bg_color, align="center"), unsafe_allow_html=True)
                            
                            # [Col 4] คงเหลือ
                            c[4].markdown(make_html(row['Current_Stock'], bg_color), unsafe_allow_html=True)
                            
                            # [Col 5] ยอดขายรวม
                            c[5].markdown(make_html(row['Total_Sales_Range'], bg_color), unsafe_allow_html=True)
                            
                            # [Col 6] สถานะ
                            c[6].markdown(make_html(row['Status'], bg_color), unsafe_allow_html=True)
                            
                            # [Col 7+] วันที่ต่างๆ (Dynamic Columns)
                            for i, col_name in enumerate(day_cols):
                                val = row[col_name]
                                html_content = make_html(val, bg_color)
                                # ทำให้เลข 0 สีจางลงเพื่อให้อ่านง่าย
                                if val == 0: html_content = html_content.replace('color:#ffffff;', 'color:#555;')
                                c[7+i].markdown(html_content, unsafe_allow_html=True)
                            
                            # เส้นคั่นบางๆ ระหว่างแถว (Optional)
                            st.markdown(f"<div style='height:1px; background-color:#333; margin-top:-1px; position:relative; z-index:1;'></div>", unsafe_allow_html=True)

                else: st.warning("⚠️ ไม่พบยอดขายใน **ช่วงเวลาหลัก (Main Range)** ที่เลือก")
            else: st.error("⚠️ ไม่พบข้อมูลการขาย")

# ==========================================
# TAB 2: Purchase Orders
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
    with col_action:
        b1, b2 = st.columns(2)
        with b1:
            if st.button("➕ เพิ่ม PO ใหม่", type="primary", key="btn_add_po_tab2"): 
                dialog_action = "po_add"
        with b2:
            if st.button("🔍 ค้นหา & แก้ไข", type="secondary", key="btn_search_po_tab2"): 
                dialog_action = "po_search"

    if not df_po.empty and 'Product_ID' in df_po.columns and not df_master.empty:
        df_po_display = pd.merge(df_po, df_master[['Product_ID', 'Image']], on='Product_ID', how='left')
        if "Image" in df_po_display.columns: df_po_display["Image"] = df_po_display["Image"].fillna("").astype(str)
        
        # ✅ Force Integers for Display
        st.dataframe(
            df_po_display.style.map(highlight_negative),
            column_config={
                "Image": st.column_config.ImageColumn("รูปสินค้า", width=80),
                "PO_Number": st.column_config.TextColumn("เลข PO"),
                "Product_ID": st.column_config.TextColumn("รหัส"),
                "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d"),
                "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d"),
                "Total_Yuan": st.column_config.NumberColumn("ยอดหยวน", format="%.2f"),
                "Order_Date": st.column_config.TextColumn("วันที่สั่ง"),
                "Received_Date": st.column_config.TextColumn("วันที่รับ"),
            },
            column_order=["Image", "Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Total_Yuan", "Transport_Type"],
            height=700, use_container_width=True, hide_index=True 
        )
    else: st.info("ยังไม่มีข้อมูลใบสั่งซื้อ")

# ==========================================
# TAB 3: Stock Report (Interactive Mode + Filters)
# ==========================================
with tab3:
    st.subheader("📈 รายงาน Stock & ตั้งค่าการเตือน")
    
    if not df_master.empty and 'Product_ID' in df_master.columns:
        # --- 1. เตรียมข้อมูล ---
        df_po_latest = pd.DataFrame()
        if not df_po.empty and 'Product_ID' in df_po.columns:
            df_po_latest = df_po.drop_duplicates(subset=['Product_ID'], keep='last')
        
        df_stock_report = pd.merge(df_master, df_po_latest, on='Product_ID', how='left')
        
        # ยอดขายรวมทั้งหมด (Total All Time)
        total_sales_map = {}
        if not df_sale.empty and 'Product_ID' in df_sale.columns:
            total_sales_map = df_sale.groupby('Product_ID')['Qty_Sold'].sum().fillna(0).astype(int).to_dict()
        
        # ✅ ใช้ recent_sales_map (ยอดขายวันล่าสุด) เป็นตัวตัดสต็อก
        df_stock_report['Recent_Sold'] = df_stock_report['Product_ID'].map(recent_sales_map).fillna(0).astype(int)
        df_stock_report['Total_Sold_All'] = df_stock_report['Product_ID'].map(total_sales_map).fillna(0).astype(int)

        if 'Initial_Stock' not in df_stock_report.columns: df_stock_report['Initial_Stock'] = 0
        
        # ✅ Ensure all quantities are integers
        for col in ['Qty_Ordered', 'Initial_Stock']:
            if col in df_stock_report.columns:
                df_stock_report[col] = pd.to_numeric(df_stock_report[col], errors='coerce').fillna(0).astype(int)
        
        # ✅ คำนวณคงเหลือ = Master - ขายล่าสุด
        df_stock_report['Current_Stock'] = df_stock_report['Initial_Stock'] - df_stock_report['Recent_Sold']

        if 'Min_Limit' not in df_stock_report.columns: df_stock_report['Min_Limit'] = 10
        else: df_stock_report['Min_Limit'] = pd.to_numeric(df_stock_report['Min_Limit'], errors='coerce').fillna(10).astype(int)

        # --- 2. คำนวณสถานะ ---
        def calc_status(row):
            if row['Current_Stock'] <= 0: return "🔴 หมดเกลี้ยง"
            elif row['Current_Stock'] < row['Min_Limit']: return "⚠️ ใกล้หมด"
            return "🟢 มีของ"
            
        df_stock_report['Status'] = df_stock_report.apply(calc_status, axis=1)

        # --- 3. ส่วนควบคุม (Filters) ---
        with st.container(border=True):
            col_filter, col_search, col_reset = st.columns([2, 2, 0.5])
            
            with col_filter:
                status_options = ["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"]
                selected_status = st.multiselect("ตัวกรองสถานะ", options=status_options, default=[], placeholder="📦 แสดงทั้งหมด")
            
            with col_search:
                search_text = st.text_input("🔍 ค้นหา (ชื่อสินค้า / รหัส)", value="", placeholder="พิมพ์ชื่อหรือรหัส...", key="stock_search")
            
            with col_reset:
                st.write(""); st.write("")
                if st.button("❌ ล้าง", use_container_width=True, key="reset_stock"): st.rerun()

        # --- 4. กรองข้อมูล ---
        edit_df = df_stock_report.copy()
        
        if selected_status:
            edit_df = edit_df[edit_df['Status'].isin(selected_status)]
            
        if search_text:
            search_text = search_text.lower()
            mask = (edit_df['Product_Name'].astype(str).str.lower().str.contains(search_text) | 
                    edit_df['Product_ID'].astype(str).str.lower().str.contains(search_text))
            edit_df = edit_df[mask]

        # --- 5. Tips & Save ---
        col_ctrl1, col_ctrl2 = st.columns([3, 1])
        with col_ctrl1:
            st.info(f"💡 การคำนวณ: คงเหลือ = Master Stock - ขายล่าสุด ({latest_date_str}) | ขายรวมทั้งหมดแสดงเพื่อดูยอดขายสะสมเท่านั้น")
        with col_ctrl2:
             if st.button("💾 บันทึกค่าจุดเตือน", type="primary", use_container_width=True):
                 if "edited_stock_data" in st.session_state:
                     update_master_limits(st.session_state.edited_stock_data)
                     st.rerun()

        # --- 6. แสดงตาราง ---
        final_cols = ["Product_ID", "Image", "Product_Name", "Current_Stock", "Recent_Sold", "Total_Sold_All", "Qty_Ordered", "PO_Number", "Status", "Min_Limit"]
        
        for c in ["PO_Number"]:
            if c not in edit_df.columns: edit_df[c] = ""

        # ✅ Force integers in st.data_editor display as well
        edited_df = st.data_editor(
            edit_df[final_cols],
            column_config={
                "Product_ID": st.column_config.TextColumn("รหัส", disabled=True, width=80),
                "Image": st.column_config.ImageColumn("รูป", width=60),
                "Product_Name": st.column_config.TextColumn("ชื่อสินค้า", disabled=True, width=200),
                "Current_Stock": st.column_config.NumberColumn("คงเหลือ", disabled=True, format="%d", width=70, help=f"Stock - ขายล่าสุด ({latest_date_str})"),
                "Recent_Sold": st.column_config.NumberColumn(f"ขาย ({latest_date_str})", disabled=True, format="%d", width=100),
                "Total_Sold_All": st.column_config.NumberColumn("ขายรวม (ทั้งหมด)", disabled=True, format="%d", width=100),
                "Qty_Ordered": st.column_config.NumberColumn("สั่งมา (POล่าสุด)", disabled=True, format="%d", width=100),
                "PO_Number": st.column_config.TextColumn("เลข PO", disabled=True, width=100),
                "Status": st.column_config.TextColumn("สถานะ", disabled=True, width=100),
                "Min_Limit": st.column_config.NumberColumn("🔔 จุดเตือน*(แก้ไขได้)", min_value=0, step=1, format="%d", width=130),
            },
            height=1500, use_container_width=True, hide_index=True, key="edited_stock_data"
        )
        st.markdown(f"**แสดงผล:** {len(edited_df)} รายการ (จากทั้งหมด {len(df_stock_report)}) | **สินค้าต้องเติม (⚠️+🔴):** {len(df_stock_report[df_stock_report['Status']!='🟢 มีของ'])}")
        
    else:
        st.warning("ไม่พบข้อมูล Master Product")

# ==========================================
# 🛑 EXECUTE DIALOGS
# ==========================================
if dialog_action == "po_add":
    po_form_dialog(mode="add")
elif dialog_action == "po_search":
    po_form_dialog(mode="search")
elif dialog_action == "history" and dialog_data:
    show_history_dialog(fixed_product_id=dialog_data)