import streamlit as st
import pandas as pd
import io
import json
import time
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
    [data-testid="stDataFrame"] th { text-align: center !important; background-color: #0047AB !important; color: white !important; vertical-align: middle !important; min-height: 60px; font-size: 14px; border-bottom: 2px solid #ffffff !important; }
    [data-testid="stDataFrame"] th:first-child { border-top-left-radius: 8px; }
    [data-testid="stDataFrame"] th:last-child { border-top-right-radius: 8px; }
    [data-testid="stDataFrame"] td { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 150px; }
    .stButton button { width: 100%; }
    button[data-testid="stNumberInputStepDown"], button[data-testid="stNumberInputStepUp"] { display: none !important; }
    div[data-testid="stNumberInput"] input { text-align: left; }
    .day-header { font-size: 12px !important; padding: 8px 4px !important; }
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
        df = pd.DataFrame(ws.get_all_records())
        col_map = {'รูปภาพ':'Image', 'รหัสสินค้า':'Product_ID', 'ชื่อสินค้า':'Product_Name', 'สินค้าคงคลัง':'Initial_Stock'}
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        if 'Initial_Stock' in df.columns:
            df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.warning(f"⚠️ โหลด Master Sheet ไม่ทัน: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_po_data():
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        try:
            ws = sh.worksheet(TAB_NAME_PO)
            data = ws.get_all_records()
            expected_cols = ["Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", 
                             "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Price_Unit_NoVAT", 
                             "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan", 
                             "Shopee_Price", "TikTok_Price", "Fees", "Transport_Type", "Wait_Date"]
            if not data: return pd.DataFrame(columns=expected_cols)
            df = pd.DataFrame(data)
            for col in expected_cols:
                if col not in df.columns: df[col] = ""
            df['Sheet_Row_Index'] = range(2, len(df) + 2) 
            return df
        except gspread.WorksheetNotFound:
            return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ โหลด PO Data ไม่ทัน: {e}")
        return pd.DataFrame()

def save_po_to_sheet(data_row, row_index=None):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_PO)
        
        formatted_row = []
        for item in data_row:
            if isinstance(item, (date, datetime)):
                formatted_row.append(item.strftime("%Y-%m-%d"))
            elif item is None:
                formatted_row.append("")
            else:
                formatted_row.append(item)
                
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

@st.cache_data(ttl=300)
def get_sale_from_folder():
    try:
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q=f"'{FOLDER_ID_DATA_SALE}' in parents and trashed=false",
            orderBy='modifiedTime desc', pageSize=1, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items: return pd.DataFrame()
        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_excel(fh)
        col_map = {'รหัสสินค้า':'Product_ID', 'จำนวน':'Qty_Sold', 'ร้านค้า':'Shop', 'เวลาสั่งซื้อ':'Order_Time'}
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        if 'Qty_Sold' in df.columns: df['Qty_Sold'] = pd.to_numeric(df['Qty_Sold'], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.warning(f"⚠️ อ่านไฟล์ Excel Sale ไม่ทัน: {e}")
        return pd.DataFrame()

# ==========================================
# 4. Main App Structure & Data Loading
# ==========================================
st.title("📊 JST Hybrid Management System")

# Initialize Session State
if "active_dialog" not in st.session_state:
    st.session_state.active_dialog = None
if "monthly_report_pid" not in st.session_state:
    st.session_state.monthly_report_pid = None

with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()
    if not df_master.empty: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty: df_po['Product_ID'] = df_po['Product_ID'].astype(str)

# ==========================================
# 5. DIALOG FUNCTIONS
# ==========================================

@st.dialog("📜 ประวัติการสั่งซื้อ (PO History)", width="large")
def show_history_dialog():
    # ใช้ selected_pid จาก session state หรือให้เลือก
    selected_pid = st.session_state.get("monthly_report_pid")
    
    if selected_pid:
        # แสดงประวัติของสินค้าที่เลือก
        history_df = df_po[df_po['Product_ID'] == selected_pid].copy()
        if 'Sheet_Row_Index' in history_df.columns: 
            history_df = history_df.drop(columns=['Sheet_Row_Index'])
        
        if not history_df.empty:
            if 'Order_Date' in history_df.columns:
                history_df['Order_Date'] = pd.to_datetime(history_df['Order_Date'], errors='coerce')
                history_df = history_df.sort_values(by='Order_Date', ascending=False)
                history_df['Order_Date'] = history_df['Order_Date'].dt.strftime('%Y-%m-%d').fillna("-")
            
            st.markdown(f"**ประวัติการสั่งซื้อสำหรับ: {selected_pid}**")
            st.dataframe(
                history_df,
                column_config={
                    "Product_ID": st.column_config.TextColumn("รหัสสินค้า"),
                    "PO_Number": st.column_config.TextColumn("เลข PO", width="medium"),
                    "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width="medium"),
                    "Wait_Date": st.column_config.NumberColumn("จำนวนวันที่รอสินค้า", format="%d วัน", width="small"),
                    "Received_Date": st.column_config.TextColumn("ของมา", width="medium"),
                    "Transport_Weight": st.column_config.TextColumn("น้ำหนักขนส่ง", width="medium"),
                    "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d"),
                    "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d"),
                    "Yuan_Rate": st.column_config.NumberColumn("เรทหยวน", format="%.2f"),
                    "Price_Unit_NoVAT": st.column_config.NumberColumn("ราคาต่อชิ้น\nไม่รวม VAT", format="%.2f"),
                    "Price_1688_NoShip": st.column_config.NumberColumn("ราคา 1688/1 ชิ้น\nไม่รวมค่าส่ง", format="%.2f"),
                    "Price_1688_WithShip": st.column_config.NumberColumn("ราคา 1688/1 ชิ้น\nรวมค่าส่ง", format="%.2f"),
                    "Total_Yuan": st.column_config.NumberColumn("ราคาหยวนทั้งหมด", format="%.2f ¥"),
                    "Shopee_Price": st.column_config.NumberColumn("ราคาใน\nช้อปปี้", format="%.2f"),
                    "TikTok_Price": st.column_config.NumberColumn("ราคาใน\nTIKTOK", format="%.2f"),
                    "Fees": st.column_config.NumberColumn("ค่า\nธรรมเนียม", format="%.2f"),
                    "Transport_Type": st.column_config.TextColumn("การขนส่ง"),
                },
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.warning(f"สินค้า {selected_pid} ยังไม่มีประวัติการสั่งซื้อ (PO)")
    else:
        # ถ้าไม่มี selected_pid ให้เลือกสินค้า
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        if df_master.empty or df_po.empty:
            st.info("ไม่มีข้อมูลสินค้าหรือประวัติการสั่งซื้อ")
            return
        
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox("🔍 ค้นหาสินค้า (ชื่อ/รหัส)", options=product_options, index=None, placeholder="พิมพ์เพื่อค้นหา...")
        
        if selected_product:
            selected_pid = selected_product.split(" : ")[0]
            history_df = df_po[df_po['Product_ID'] == selected_pid].copy()
            if 'Sheet_Row_Index' in history_df.columns: 
                history_df = history_df.drop(columns=['Sheet_Row_Index'])
            
            if not history_df.empty:
                if 'Order_Date' in history_df.columns:
                    history_df['Order_Date'] = pd.to_datetime(history_df['Order_Date'], errors='coerce')
                    history_df = history_df.sort_values(by='Order_Date', ascending=False)
                    history_df['Order_Date'] = history_df['Order_Date'].dt.strftime('%Y-%m-%d').fillna("-")

                st.divider()
                st.markdown(f"**รายการสั่งซื้อของ:** `{selected_product}` ({len(history_df)} รายการ)")
                st.dataframe(
                    history_df,
                    column_config={
                        "Product_ID": st.column_config.TextColumn("รหัสสินค้า"),
                        "PO_Number": st.column_config.TextColumn("เลข PO", width="medium"),
                        "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width="medium"),
                        "Wait_Date": st.column_config.NumberColumn("จำนวนวันที่รอสินค้า", format="%d วัน", width="small"),
                        "Received_Date": st.column_config.TextColumn("ของมา", width="medium"),
                        "Transport_Weight": st.column_config.TextColumn("น้ำหนักขนส่ง", width="medium"),
                        "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d"),
                        "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d"),
                        "Yuan_Rate": st.column_config.NumberColumn("เรทหยวน", format="%.2f"),
                        "Price_Unit_NoVAT": st.column_config.NumberColumn("ราคาต่อชิ้น\nไม่รวม VAT", format="%.2f"),
                        "Price_1688_NoShip": st.column_config.NumberColumn("ราคา 1688/1 ชิ้น\nไม่รวมค่าส่ง", format="%.2f"),
                        "Price_1688_WithShip": st.column_config.NumberColumn("ราคา 1688/1 ชิ้น\nรวมค่าส่ง", format="%.2f"),
                        "Total_Yuan": st.column_config.NumberColumn("ราคาหยวนทั้งหมด", format="%.2f ¥"),
                        "Shopee_Price": st.column_config.NumberColumn("ราคาใน\nช้อปปี้", format="%.2f"),
                        "TikTok_Price": st.column_config.NumberColumn("ราคาใน\nTIKTOK", format="%.2f"),
                        "Fees": st.column_config.NumberColumn("ค่า\nธรรมเนียม", format="%.2f"),
                        "Transport_Type": st.column_config.TextColumn("การขนส่ง"),
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.warning("สินค้านี้ยังไม่มีประวัติการสั่งซื้อ (PO)")

@st.dialog("📝 จัดการรายการสั่งซื้อ", width="large")
def po_form_dialog(mode="add"):
    # Header
    if mode == "add": st.subheader("➕ เพิ่มรายการใหม่")
    else: st.subheader("✏️ แก้ไขรายการ")

    d = {}
    sheet_row_index = None

    # --- ส่วนค้นหา (โหมด Search) ---
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
            st.warning("ยังไม่มีข้อมูล PO")
            return

    # --- ฟังก์ชันล้างข้อมูล (Reset Callback - ใช้เฉพาะโหมด Add) ---
    def clear_form_data():
        keys_to_reset = {
            "add_po_num": "",
            "add_order_date": date.today(),
            "add_recv_date": None,
            "add_weight": "",
            "add_qty_ord": None,
            "add_qty_rem": None,
            "add_yuan_rate": None,
            "add_fees": None,
            "add_p_novat": None,
            "add_p_1688_no": None,
            "add_p_1688_ship": None,
            "add_p_shopee": None,
            "add_p_tiktok": None,
            "add_total_yuan": None
        }
        for k, v in keys_to_reset.items():
            st.session_state[k] = v

    # --- กำหนด Key Prefix ---
    if mode == "search" and sheet_row_index:
        key_prefix = f"search_{sheet_row_index}"
    else:
        key_prefix = "add"

    # --- Prepare Data ---
    st.markdown("##### 1. ค้นหารหัสสินค้า")
    product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
    default_idx = None
    if mode == "search" and "Product_ID" in d:
         matches = [i for i, opt in enumerate(product_options) if opt.startswith(str(d["Product_ID"]) + " :")]
         if matches: default_idx = matches[0]

    selected_option = st.selectbox("ระบุสินค้า", product_options, index=default_idx, placeholder="🔍 Search...", label_visibility="collapsed", key=f"{key_prefix}_product_select")
    
    master_img_url = "https://via.placeholder.com/300x300.png?text=No+Image"
    master_pid = ""
    master_name = ""

    if selected_option:
        master_pid = selected_option.split(" : ")[0]
        row_info = df_master[df_master['Product_ID'] == master_pid].iloc[0]
        master_name = row_info['Product_Name']
        if row_info['Image']: master_img_url = row_info['Image']

    with st.container(border=True):
        col_left_img, col_right_form = st.columns([1.2, 3], gap="medium")
        with col_left_img:
            st.markdown(f"**{master_pid}**") 
            st.image(master_img_url, use_container_width=True)
            if master_name: st.caption(f"{master_name}")
        
        with col_right_form:
            st.markdown("###### 📄 ข้อมูลทั่วไป")
            def get_date_val(val):
                if not val or val == "" or val == "nan": return None
                try: return datetime.strptime(str(val), "%Y-%m-%d").date()
                except: return None
            
            # Helper: ดึงค่า Text
            def v(k): return d.get(k) if mode == "search" else None

            # Helper: ดึงค่า Numeric (แยก Logic Add/Search)
            def vn(k): 
                val = d.get(k)
                if mode == "search":
                    try: return float(val) if val is not None and str(val).strip() != "" else 0.0
                    except: return 0.0
                else:
                    try: return float(val) if val and float(val)!=0 else None
                    except: return None

            r1c1, r1c2, r1c3 = st.columns(3)
            po_num = r1c1.text_input("เลข PO *", value=v("PO_Number"), placeholder="ระบุเลข PO", key=f"{key_prefix}_po_num")
            
            def_order_date = get_date_val(d.get("Order_Date")) if mode=="search" else date.today()
            order_date = r1c2.date_input("วันที่สั่ง", value=def_order_date, key=f"{key_prefix}_order_date")
            recv_date = r1c3.date_input("ของมา (ประมาณ)", value=get_date_val(d.get("Received_Date")), key=f"{key_prefix}_recv_date")
            
            weight_txt = st.text_area("📦 น้ำหนักขนส่ง / รายละเอียด *", value=v("Transport_Weight"), height=100, placeholder="รายละเอียด...", key=f"{key_prefix}_weight")
            
            st.markdown("###### 💰 ปริมาณ & ราคาต้นทุน")
            r3c1, r3c2, r3c3, r3c4 = st.columns(4)
            qty_ord = r3c1.number_input("สั่งมา *", min_value=0.0, step=0.0, value=vn("Qty_Ordered"), key=f"{key_prefix}_qty_ord") 
            qty_rem = r3c2.number_input("เหลือ *", min_value=0.0, step=0.0, value=vn("Qty_Remaining"), key=f"{key_prefix}_qty_rem")
            yuan_rate = r3c3.number_input("เรทหยวน *", min_value=0.0, step=0.0, format="%.2f", value=vn("Yuan_Rate"), key=f"{key_prefix}_yuan_rate")
            fees = r3c4.number_input("ค่าธรรมเนียม", min_value=0.0, step=0.0, format="%.2f", value=vn("Fees"), key=f"{key_prefix}_fees")
            
            r4c1, r4c2, r4c3 = st.columns(3)
            p_no_vat = r4c1.number_input("ราคาต่อชิ้นไม่รวม VAT", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_Unit_NoVAT"), key=f"{key_prefix}_p_novat")
            p_1688_noship = r4c2.number_input("ราคา 1688 ไม่รวมส่ง", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_1688_NoShip"), key=f"{key_prefix}_p_1688_no")
            p_1688_ship = r4c3.number_input("ราคา 1688 รวมส่ง *", min_value=0.0, step=0.0, format="%.2f", value=vn("Price_1688_WithShip"), key=f"{key_prefix}_p_1688_ship")

            st.markdown("###### 🏷️ ราคาขาย & สรุป")
            r5c1, r5c2, r5c3 = st.columns(3)
            p_shopee = r5c1.number_input("Shopee", min_value=0.0, step=0.0, format="%.2f", value=vn("Shopee_Price"), key=f"{key_prefix}_p_shopee")
            p_tiktok = r5c2.number_input("TikTok", min_value=0.0, step=0.0, format="%.2f", value=vn("TikTok_Price"), key=f"{key_prefix}_p_tiktok")
            
            def_transport_idx = 1 if d.get("Transport_Type") == "ส่งทางเรือ 🚢" else 0
            transport = r5c3.selectbox("การขนส่ง", ["ส่งทางรถ 🚛", "ส่งทางเรือ 🚢"], index=def_transport_idx, key=f"{key_prefix}_transport")
            
            st.markdown("---")
            # Layout ปุ่มด้านล่าง
            f_col1, f_col2, f_col3 = st.columns([1.5, 0.75, 0.75])
            with f_col1:
                total_yuan_input = st.number_input("ราคาหยวนทั้งหมด *", min_value=0.0, step=0.0, format="%.2f", value=vn("Total_Yuan"), key=f"{key_prefix}_total_yuan")

            # ปุ่ม Clear (เฉพาะโหมด Add)
            with f_col2:
                st.write(""); st.write("") # ดันปุ่มลงมา
                if mode == "add":
                     st.button("🧹 ล้าง", on_click=clear_form_data, key="btn_clear_data_bottom", type="secondary")

            # ปุ่ม Save
            with f_col3:
                st.write(""); st.write("") # ดันปุ่มลงมา
                btn_label = "✅ บันทึก" if mode == "add" else "💾 บันทึกทับ"
                if st.button(btn_label, type="primary", key="btn_submit_po"):
                    # Validation Logic
                    errors = []
                    if not master_pid: errors.append("ยังไม่ได้เลือกสินค้า")
                    if not po_num: errors.append("ยังไม่ได้ระบุเลข PO")
                    if (qty_ord or 0) <= 0: errors.append("จำนวนสั่งซื้อต้องมากกว่า 0")
                    if (p_1688_ship or 0) <= 0: errors.append("ราคาต้นทุนรวมส่งต้องมากกว่า 0")
                    if (total_yuan_input or 0) <= 0: errors.append("ยอดรวมหยวนต้องมากกว่า 0")
                    
                    if errors: 
                        st.error(f"⚠️ บันทึกไม่ได้: {', '.join(errors)}")
                    else:
                        wait_days = ""
                        if order_date and recv_date: wait_days = (recv_date - order_date).days
                        
                        new_row = [
                            master_pid, po_num, order_date, recv_date, weight_txt, 
                            qty_ord or 0, qty_rem or 0, yuan_rate or 0, p_no_vat or 0, 
                            p_1688_noship or 0, p_1688_ship or 0, total_yuan_input or 0, 
                            p_shopee or 0, p_tiktok or 0, fees or 0, transport, wait_days
                        ]
                        if save_po_to_sheet(new_row, row_index=sheet_row_index): 
                            st.success("✅ บันทึกเรียบร้อย!")

# ==========================================
# 6. TABS & UI LOGIC
# ==========================================
tab1, tab2, tab3 = st.tabs(["📈 รายงาน Stock", "📝 รายการสั่งซื้อ", "📅 รายงาน Stock รายเดือน"])

# ==========================================
# TAB 1: Stock Report
# ==========================================
with tab1:
    if not df_master.empty:
        df_po_latest = pd.DataFrame()
        if not df_po.empty:
            df_po_latest = df_po.drop_duplicates(subset=['Product_ID'], keep='last')
        
        df_stock_report = pd.merge(df_master, df_po_latest, on='Product_ID', how='left')
        
        sales_map = {}
        if not df_sale.empty:
            df_sale['Product_ID'] = df_sale['Product_ID'].astype(str)
            sales_summary = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
            sales_map = sales_summary.set_index('Product_ID')['Qty_Sold'].to_dict()
        
        df_stock_report['Qty_Sold'] = df_stock_report['Product_ID'].map(sales_map).fillna(0)
        df_stock_report['Current_Stock'] = df_stock_report['Initial_Stock'] - df_stock_report['Qty_Sold']
        df_stock_report['Status'] = df_stock_report['Current_Stock'].apply(lambda x: "🔴 หมดเกลี้ยง" if x<=0 else ("⚠️ ใกล้หมด" if x<10 else "🟢 มีของ"))

        # Metrics
        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"""<div class="metric-card border-cyan"><div class="metric-title">สินค้าทั้งหมด</div><div class="metric-value text-cyan">{len(df_stock_report):,}</div></div>""", unsafe_allow_html=True)
        with c2: st.markdown(f"""<div class="metric-card border-gold"><div class="metric-title">ยอดขายรวม</div><div class="metric-value text-gold">{int(df_stock_report['Qty_Sold'].sum()):,}</div></div>""", unsafe_allow_html=True)
        with c3: st.markdown(f"""<div class="metric-card border-red"><div class="metric-title">ต้องเติมของ</div><div class="metric-value text-red">{len(df_stock_report[df_stock_report['Current_Stock'] < 10]):,}</div></div>""", unsafe_allow_html=True)
        
        st.divider()
        
        # Filters
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
            if st.button("📜 ดูประวัติ", type="secondary"): show_history_dialog()
        with col_b3: 
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            st.button("🔄", on_click=lambda: [st.cache_data.clear(), st.rerun()], type="primary")

        # Table
        show_df = df_stock_report.copy()
        if st.session_state.filter_status and "📦 สินค้าทั้งหมด" not in st.session_state.filter_status:
            show_df = show_df[show_df['Status'].isin(st.session_state.filter_status)]
        if st.session_state.search_query:
            show_df = show_df[show_df['Product_Name'].str.contains(st.session_state.search_query, case=False, na=False) | show_df['Product_ID'].str.contains(st.session_state.search_query, case=False, na=False)]

        for col in ["Image", "Product_ID", "Product_Name", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Transport_Type", "Status"]:
            if col in show_df.columns: show_df[col] = show_df[col].fillna("").astype(str)
        for col in ["Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Price_Unit_NoVAT", "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan", "Shopee_Price", "TikTok_Price", "Fees", "Qty_Sold", "Current_Stock"]:
            if col in show_df.columns: show_df[col] = pd.to_numeric(show_df[col], errors='coerce').fillna(0)

        st.dataframe(
            show_df[[c for c in ["Product_ID", "Image", "Product_Name", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Price_Unit_NoVAT", "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan", "Shopee_Price", "TikTok_Price", "Fees", "Transport_Type", "Qty_Sold", "Current_Stock", "Status"] if c in show_df.columns]].style.map(lambda v: f'color: {"#ff4d4d" if float(v)<0 else "white"}' if isinstance(v, (int, float)) else None, subset=['Current_Stock', 'Qty_Remaining']),
            column_config={
                "Product_ID": st.column_config.TextColumn("รหัสสินค้า", width=100),
                "Image": st.column_config.ImageColumn("รูปสินค้า", width=80),
                "Product_Name": st.column_config.TextColumn("ชื่อสินค้า", width=150), 
                "PO_Number": st.column_config.TextColumn("เลข PO", width=100),
                "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width=100),
                "Received_Date": st.column_config.TextColumn("ของมา", width=100),
                "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d", width=100),
                "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d", width=100),
                "Total_Yuan": st.column_config.NumberColumn("ราคาหยวน", format="%.2f ¥", width=100),
                "Qty_Sold": st.column_config.NumberColumn("ยอดขาย", format="%d", width=100),
                "Current_Stock": st.column_config.NumberColumn("คงเหลือ", format="%d", width=100),
            },
            height=2300, use_container_width=True, hide_index=True
        )
    else: st.warning("ไม่พบข้อมูล Master Product")

# ==========================================
# TAB 2: Purchase Orders
# ==========================================
with tab2:
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 สรุปรายการสั่งซื้อสินค้า")
    with col_action:
        b1, b2 = st.columns(2)
        with b1:
            if st.button("➕ เพิ่ม PO ใหม่", type="primary"): 
                st.session_state.active_dialog = "add"
                st.rerun()
        with b2:
            if st.button("🔍 ค้นหา & แก้ไข", type="secondary"): 
                st.session_state.active_dialog = "search"
                st.rerun()

    # Logic to trigger dialogs based on session state
    if st.session_state.active_dialog == "add":
        po_form_dialog(mode="add")
    elif st.session_state.active_dialog == "search":
        po_form_dialog(mode="search")

    if not df_po.empty:
        df_po_display = pd.merge(df_po, df_master[['Product_ID', 'Image']], on='Product_ID', how='left')
        if "Image" in df_po_display.columns: df_po_display["Image"] = df_po_display["Image"].fillna("").astype(str)
        st.data_editor(
            df_po_display[["Image", "Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Total_Yuan", "Transport_Type"]],
            column_config={"Image": st.column_config.ImageColumn("รูปสินค้า", width=80)},
            height=700, use_container_width=True, hide_index=True, disabled=True 
        )
    else: st.info("ยังไม่มีข้อมูลใบสั่งซื้อ")

# ==========================================
# TAB 3: Monthly Stock Report
# ==========================================
with tab3:
    st.subheader("📅 รายงาน Stock รายเดือน")
    
    # 1. Filter Section
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # ปี
        if not df_sale.empty:
            df_sale['Order_Time'] = pd.to_datetime(df_sale['Order_Time'], errors='coerce')
            available_years = sorted(df_sale['Order_Time'].dt.year.dropna().unique().astype(int))
            selected_year = st.selectbox("เลือกปี", available_years, index=len(available_years)-1 if available_years else None)
        else:
            selected_year = datetime.now().year
            st.selectbox("เลือกปี", [selected_year])
    
    with col2:
        # เดือน
        months = list(range(1, 13))
        month_names = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน",
                      "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]
        selected_month = st.selectbox("เลือกเดือน", months, format_func=lambda x: month_names[x-1])
    
    with col3:
        # ช่วงวันที่
        st.write("เลือกช่วงวันที่")
        start_date = st.date_input("วันที่เริ่มต้น", 
                                  value=date(selected_year, selected_month, 1),
                                  min_value=date(selected_year, selected_month, 1),
                                  max_value=date(selected_year, selected_month, 1) + timedelta(days=31),
                                  key="start_date")
        
        # คำนวณวันสุดท้ายของเดือน
        if selected_month == 12:
            last_day = date(selected_year, 12, 31)
        else:
            last_day = date(selected_year, selected_month + 1, 1) - timedelta(days=1)
        
        end_date = st.date_input("วันที่สิ้นสุด", 
                                value=last_day,
                                min_value=start_date,
                                max_value=last_day,
                                key="end_date")
    
    # ปุ่มสำหรับรีเฟรชข้อมูล
    if st.button("🔄 แสดงรายงาน", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    
    # 2. Prepare Data สำหรับรายงานรายเดือน
    if not df_sale.empty and not df_master.empty:
        # แปลงวันที่ใน df_sale
        df_sale['Order_Date'] = pd.to_datetime(df_sale['Order_Time']).dt.date
        
        # กรองข้อมูลตามช่วงวันที่ที่เลือก
        mask = (df_sale['Order_Date'] >= start_date) & (df_sale['Order_Date'] <= end_date)
        df_sale_filtered = df_sale[mask].copy()
        
        if not df_sale_filtered.empty:
            # สร้างคอลัมน์วันที่แต่ละวันในรูปแบบภาษาไทย
            date_range = pd.date_range(start=start_date, end=end_date)
            
            # สร้าง pivot table สำหรับยอดขายรายวัน
            df_sale_filtered['Order_Date'] = pd.to_datetime(df_sale_filtered['Order_Date'])
            df_sale_filtered['Date_Str'] = df_sale_filtered['Order_Date'].dt.strftime('%Y-%m-%d')
            
            # สร้างตารางสรุป
            # 1. ยอดขายรวมในช่วงวันที่
            sales_summary = df_sale_filtered.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
            sales_summary.columns = ['Product_ID', 'ยอดขายรวม']
            
            # 2. ยอดขายรายวัน
            pivot_df = pd.pivot_table(df_sale_filtered, 
                                     values='Qty_Sold', 
                                     index='Product_ID', 
                                     columns='Order_Date', 
                                     aggfunc='sum', 
                                     fill_value=0)
            
            # รวมข้อมูลทั้งหมด
            df_report = pd.merge(df_master[['Product_ID', 'Image', 'Product_Name']], 
                                sales_summary, 
                                on='Product_ID', 
                                how='inner')  # ใช้ inner join เพื่อแสดงเฉพาะสินค้าที่มียอดขาย
            
            # เพิ่มข้อมูลคงเหลือ
            # คำนวณยอดขายทั้งหมดจนถึงปัจจุบัน
            df_sale_total = df_sale.copy()
            df_sale_total['Order_Date'] = pd.to_datetime(df_sale_total['Order_Time']).dt.date
            total_sales = df_sale_total.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
            total_sales.columns = ['Product_ID', 'Total_Sold']
            
            df_report = pd.merge(df_report, total_sales, on='Product_ID', how='left')
            df_report = pd.merge(df_report, df_master[['Product_ID', 'Initial_Stock']], on='Product_ID', how='left')
            df_report['คงเหลือ'] = df_report['Initial_Stock'] - df_report['Total_Sold']
            
            # เพิ่มข้อมูลยอดขายรายวัน
            for day in date_range:
                day_str = day.strftime('%Y-%m-%d')
                if day in pivot_df.columns:
                    df_report[day] = df_report['Product_ID'].map(pivot_df[day].to_dict())
                else:
                    df_report[day] = 0
            
            # จัดเรียงคอลัมน์
            base_cols = ['Product_ID', 'Image', 'Product_Name', 'คงเหลือ', 'ยอดขายรวม']
            date_cols = [day for day in date_range]
            
            # เรียงคอลัมน์ตามลำดับวันที่
            ordered_cols = base_cols + date_cols
            df_report = df_report[ordered_cols]
            
            # แปลงชื่อคอลัมน์วันที่เป็นภาษาไทย
            month_abbr = {
                1: "ม.ค.", 2: "ก.พ.", 3: "มี.ค.", 4: "เม.ย.", 5: "พ.ค.", 6: "มิ.ย.",
                7: "ก.ค.", 8: "ส.ค.", 9: "ก.ย.", 10: "ต.ค.", 11: "พ.ย.", 12: "ธ.ค."
            }
            
            day_abbr = ["อา", "จ", "อ", "พ", "พฤ", "ศ", "ส"]
            
            # สร้างการแมปชื่อคอลัมน์ใหม่
            column_config = {
                'Product_ID': st.column_config.TextColumn("รหัสสินค้า", width=100),
                'Image': st.column_config.ImageColumn("รูปสินค้า", width=80),
                'Product_Name': st.column_config.TextColumn("ชื่อสินค้า", width=200),
                'คงเหลือ': st.column_config.NumberColumn("คงเหลือ", format="%d", width=80),
                'ยอดขายรวม': st.column_config.NumberColumn("ยอดขายรวม", format="%d", width=80),
            }
            
            # เพิ่มคอลัมน์สำหรับปุ่มประวัติ
            df_report.insert(0, 'ประวัติ', False)
            
            # เพิ่มคอลัมน์วันที่ใน column_config
            for day in date_cols:
                day_of_week = day_abbr[day.weekday()]
                month_name = month_abbr[day.month]
                col_title = f"{day.day} {month_name}"
                column_config[day] = st.column_config.NumberColumn(
                    col_title,
                    format="%d",
                    width=60,
                    help=f"{day.strftime('%Y-%m-%d')} ({day_of_week})"
                )
            
            # เพิ่ม column config สำหรับปุ่มประวัติ
            column_config['ประวัติ'] = st.column_config.CheckboxColumn(
                "ประวัติ",
                help="คลิกเพื่อดูประวัติการสั่งซื้อ",
                width=60
            )
            
            # แสดงตาราง
            st.markdown(f"**รายงาน Stock วันที่ {start_date.strftime('%d/%m/%Y')} ถึง {end_date.strftime('%d/%m/%Y')}**")
            
            # ใช้ data_editor เพื่อให้มีปุ่ม interact
            edited_df = st.data_editor(
                df_report,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                height=600,
                disabled=['Product_ID', 'Image', 'Product_Name', 'คงเหลือ', 'ยอดขายรวม'] + date_cols
            )
            
            # ตรวจสอบการคลิกที่ checkbox ประวัติ
            for idx, row in edited_df.iterrows():
                if row['ประวัติ']:
                    st.session_state.monthly_report_pid = row['Product_ID']
                    show_history_dialog()
                    # รีเซ็ต checkbox หลังแสดง dialog
                    edited_df.at[idx, 'ประวัติ'] = False
                    st.rerun()
            
            # สรุปสถิติ
            st.divider()
            st.subheader("📊 สรุปสถิติ")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("จำนวนสินค้าที่ขายได้", len(df_report))
            with col2:
                st.metric("ยอดขายรวม", int(df_report['ยอดขายรวม'].sum()))
            with col3:
                st.metric("สินค้าขายดีที่สุด", 
                         df_report.loc[df_report['ยอดขายรวม'].idxmax(), 'Product_ID'] if len(df_report) > 0 else "-")
            with col4:
                avg_sales = df_report['ยอดขายรวม'].mean() if len(df_report) > 0 else 0
                st.metric("ยอดขายเฉลี่ยต่อสินค้า", f"{avg_sales:.1f}")
        
        else:
            st.warning(f"ไม่พบข้อมูลการขายในช่วงวันที่ {start_date.strftime('%d/%m/%Y')} ถึง {end_date.strftime('%d/%m/%Y')}")
    else:
        st.warning("ไม่พบข้อมูลการขายหรือข้อมูลสินค้า")