import streamlit as st
import pandas as pd
import io
import json
import time
from datetime import date, datetime
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
    /* ลดพื้นที่ว่างด้านบน Header */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
    }

    /* Card Container */
    .metric-card {
        background-color: #1a1a1a;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .metric-title {
        color: #b0b0b0;
        font-size: 14px;
        font-weight: 500;
        margin-bottom: 5px;
    }
    .metric-value {
        color: #ffffff;
        font-size: 28px;
        font-weight: bold;
    }
    
    /* Border Colors */
    .border-cyan { border-left: 4px solid #00e5ff; }
    .border-gold { border-left: 4px solid #ffd700; }
    .border-red  { border-left: 4px solid #ff4d4d; }
    
    /* Text Colors for Values */
    .text-cyan { color: #00e5ff !important; }
    .text-gold { color: #ffd700 !important; }
    .text-red  { color: #ff4d4d !important; }
    
    /* Table Headers: จัดกึ่งกลาง + รองรับการขึ้นบรรทัดใหม่ */
    [data-testid="stDataFrame"] th { 
        text-align: center !important;
        white-space: pre-wrap !important; 
        vertical-align: middle !important;
        min-height: 60px;
        font-size: 13px;
    }

    /* Table Cells: บังคับตัดคำเป็น ... ถ้าล้น */
    [data-testid="stDataFrame"] td {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 150px;
    }
    
    /* Button Full Width */
    .stButton button { width: 100%; }
    
    /* ซ่อนปุ่ม +/- ของ Number Input */
    button[data-testid="stNumberInputStepDown"],
    button[data-testid="stNumberInputStepUp"] {
        display: none !important;
    }
    div[data-testid="stNumberInput"] input {
        text-align: left;
    }
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
            columns = ["Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", 
                       "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Price_Unit_NoVAT", 
                       "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan", 
                       "Shopee_Price", "TikTok_Price", "Fees", "Transport_Type"]
            if not data:
                 return pd.DataFrame(columns=columns)
            df = pd.DataFrame(data)
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
            else:
                formatted_row.append(item)
                
        if row_index:
            range_name = f"A{row_index}:P{row_index}" 
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
        if 'Qty_Sold' in df.columns:
            df['Qty_Sold'] = pd.to_numeric(df['Qty_Sold'], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.warning(f"⚠️ อ่านไฟล์ Excel Sale ไม่ทัน: {e}")
        return pd.DataFrame()

# ==========================================
# 4. Main App Structure
# ==========================================
st.title("📊 JST Hybrid Management System")

# โหลดข้อมูล
with st.spinner('กำลังโหลดข้อมูล...'):
    df_master = get_stock_from_sheet()
    df_po = get_po_data()
    df_sale = get_sale_from_folder()

    if not df_master.empty: df_master['Product_ID'] = df_master['Product_ID'].astype(str)
    if not df_po.empty: df_po['Product_ID'] = df_po['Product_ID'].astype(str)
    
tab1, tab2 = st.tabs(["📈 รายงาน Stock", "📝 รายการสั่งซื้อ"])

# ==========================================
# TAB 1: Stock Report (MASTER BASED)
# ==========================================
with tab1:
    # --- Function: History Dialog ---
    @st.dialog("📜 ประวัติการสั่งซื้อ (PO History)", width="large")
    def show_history_dialog():
        st.caption("ค้นหาและเลือกสินค้าเพื่อดูประวัติการสั่งซื้อทั้งหมด")
        
        if df_master.empty or df_po.empty:
            st.info("ไม่มีข้อมูลสินค้าหรือประวัติการสั่งซื้อ")
            return

        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        selected_product = st.selectbox(
            "🔍 ค้นหาสินค้า (ชื่อ/รหัส)", 
            options=product_options,
            index=None,
            placeholder="พิมพ์เพื่อค้นหา..."
        )

        if selected_product:
            selected_pid = selected_product.split(" : ")[0]
            history_df = df_po[df_po['Product_ID'] == selected_pid].copy()
            
            if not history_df.empty:
                if 'Order_Date' in history_df.columns:
                    history_df['Order_Date'] = pd.to_datetime(history_df['Order_Date'], errors='coerce')
                    history_df = history_df.sort_values(by='Order_Date', ascending=False)
                    history_df['Order_Date'] = history_df['Order_Date'].dt.strftime('%Y-%m-%d').fillna("-")

                st.divider()
                st.markdown(f"**รายการสั่งซื้อของ:** `{selected_product}` ({len(history_df)} รายการ)")
                
                # [แก้ไข] เปลี่ยนหัวตารางใน Dialog ประวัติให้เป็นภาษาไทย
                st.dataframe(
                    history_df,
                    column_config={
                        "PO_Number": st.column_config.TextColumn("เลข PO", width="medium"),
                        "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width="medium"),
                        "Received_Date": st.column_config.TextColumn("ของมา", width="medium"),
                        "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d"),
                        "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d"),
                        "Price_1688_WithShip": st.column_config.NumberColumn("ราคา 1688/1 ชิ้น\nรวมค่าส่ง", format="%.2f"),
                        "Transport_Type": st.column_config.TextColumn("การขนส่ง"),
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.warning("สินค้านี้ยังไม่มีประวัติการสั่งซื้อ (PO)")

    # --- Main Logic ---
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
        
        def get_status(val):
            if val <= 0: return "🔴 หมดเกลี้ยง"
            elif val < 10: return "⚠️ ใกล้หมด"
            else: return "🟢 มีของ"
        df_stock_report['Status'] = df_stock_report['Current_Stock'].apply(get_status)

        # Metrics
        total_items = len(df_stock_report)
        total_sold_all = df_stock_report['Qty_Sold'].sum()
        critical_stock = len(df_stock_report[df_stock_report['Current_Stock'] < 10])

        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"""<div class="metric-card border-cyan"><div class="metric-title">สินค้าทั้งหมด (Master)</div><div class="metric-value text-cyan">{total_items:,}</div></div>""", unsafe_allow_html=True)
        with c2: st.markdown(f"""<div class="metric-card border-gold"><div class="metric-title">ยอดขายรวม (ชิ้น)</div><div class="metric-value text-gold">{int(total_sold_all):,}</div></div>""", unsafe_allow_html=True)
        with c3: st.markdown(f"""<div class="metric-card border-red"><div class="metric-title">ต้องเติมของ (รายการ)</div><div class="metric-value text-red">{critical_stock:,}</div></div>""", unsafe_allow_html=True)
        
        st.divider()
        
        # Filters & Actions
        if 'filter_status' not in st.session_state: st.session_state.filter_status = []
        if 'search_query' not in st.session_state: st.session_state.search_query = ""

        def clear_filters():
            st.session_state.filter_status = []
            st.session_state.search_query = ""
        def manual_update():
            st.cache_data.clear()
            st.rerun()

        col_f1, col_f2, col_b1, col_b2, col_b3 = st.columns([2, 2, 0.4, 0.5, 0.5])
        with col_f1:
            selected_status = st.multiselect("กรองสถานะ", ["📦 สินค้าทั้งหมด", "🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"], key="filter_status")
        with col_f2:
            search_text = st.text_input("ค้นหา (ชื่อ/รหัส)", key="search_query", placeholder="เช่น ชั้นวางของ, SP001...")
        
        with col_b1:
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            st.button("❌ ล้าง", on_click=clear_filters)
        with col_b2:
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            if st.button("📜 ดูประวัติ", type="secondary"): show_history_dialog()
        with col_b3:
            st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
            st.button("🔄 อัพเดต", on_click=manual_update, type="primary")

        # Table Logic
        show_df = df_stock_report.copy()
        
        if selected_status and "📦 สินค้าทั้งหมด" not in selected_status:
            show_df = show_df[show_df['Status'].isin(selected_status)]

        if search_text:
            mask = (
                show_df['Product_Name'].str.contains(search_text, case=False, na=False) |
                show_df['Product_ID'].str.contains(search_text, case=False, na=False)
            )
            show_df = show_df[mask]

        # Cleaning
        str_cols = ["Image", "Product_ID", "Product_Name", "PO_Number", "Order_Date", 
                    "Received_Date", "Transport_Weight", "Transport_Type", "Status"]
        for col in str_cols:
            if col in show_df.columns: show_df[col] = show_df[col].fillna("").astype(str)

        num_cols = ["Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Price_Unit_NoVAT", 
                    "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan", 
                    "Shopee_Price", "TikTok_Price", "Fees", "Qty_Sold", "Current_Stock"]
        for col in num_cols:
            if col in show_df.columns: show_df[col] = pd.to_numeric(show_df[col], errors='coerce').fillna(0)

        # Display
        COL_WIDTH = 100 
        def color_negative_red(val):
            try: color = '#ff4d4d' if float(val) < 0 else 'white'
            except: color = 'white'
            return f'color: {color}'

        display_columns = [
            "Image", "Product_ID", "Product_Name", "PO_Number", "Order_Date", "Received_Date", 
            "Transport_Weight", "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", 
            "Price_Unit_NoVAT", "Price_1688_NoShip", "Price_1688_WithShip", "Total_Yuan",
            "Shopee_Price", "TikTok_Price", "Fees", "Transport_Type", "Qty_Sold", "Current_Stock", "Status"
        ]
        final_cols = [c for c in display_columns if c in show_df.columns]
        
        st.dataframe(
            show_df[final_cols].style.map(color_negative_red, subset=['Current_Stock', 'Qty_Remaining']),
            column_config={
                "Image": st.column_config.ImageColumn("รูปสินค้า", width=80),
                "Product_ID": st.column_config.TextColumn("รหัสสินค้า", width=COL_WIDTH),
                "Product_Name": st.column_config.TextColumn("ชื่อสินค้า", width=150), 
                "PO_Number": st.column_config.TextColumn("เลข PO", width=COL_WIDTH),
                "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width=COL_WIDTH),
                "Received_Date": st.column_config.TextColumn("ของมา", width=COL_WIDTH),
                "Transport_Weight": st.column_config.TextColumn("น้ำหนัก\nขนส่ง", width=COL_WIDTH),
                "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d", width=COL_WIDTH),
                "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d", width=COL_WIDTH),
                "Yuan_Rate": st.column_config.NumberColumn("เรท\nหยวน", format="%.2f", width=COL_WIDTH),
                "Price_Unit_NoVAT": st.column_config.NumberColumn(label="ราคาต่อชิ้น\nไม่รวม VAT", format="%.2f", width=COL_WIDTH),
                "Price_1688_NoShip": st.column_config.NumberColumn(label="ราคา1688/1 ชิ้น\nไม่รวมค่าส่ง", format="%.2f", width=COL_WIDTH),
                "Price_1688_WithShip": st.column_config.NumberColumn(label="ราคา 1688/1 ชิ้น\nรวมค่าส่ง", format="%.2f", width=COL_WIDTH),
                "Total_Yuan": st.column_config.NumberColumn(label="ราคาหยวน\nทั้งหมด", format="%.2f ¥", width=COL_WIDTH),
                "Shopee_Price": st.column_config.NumberColumn(label="ราคาใน\nช้อปปี้", format="%.2f", width=COL_WIDTH),
                "TikTok_Price": st.column_config.NumberColumn(label="ราคาใน\nTIKTOK", format="%.2f", width=COL_WIDTH),
                "Fees": st.column_config.NumberColumn(label="ค่า\nธรรมเนียม", format="%.2f", width=COL_WIDTH),
                "Transport_Type": st.column_config.TextColumn("การขนส่ง", width=COL_WIDTH),
                "Qty_Sold": st.column_config.NumberColumn("ยอดขาย", format="%d", width=COL_WIDTH),
                "Current_Stock": st.column_config.NumberColumn("คงเหลือ", format="%d", width=COL_WIDTH),
                "Status": st.column_config.TextColumn("Status", width=COL_WIDTH),
            },
            height=800,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning("ไม่พบข้อมูล Master Product")

# ==========================================
# TAB 2: Purchase Orders
# ==========================================
with tab2:
    @st.dialog("📝 จัดการรายการสั่งซื้อ", width="large")
    def po_form_dialog(mode="add"):
        d = {}
        sheet_row_index = None
        
        if mode == "search":
            st.markdown("### 🔍 ค้นหา PO ที่ต้องการแก้ไข")
            if not df_po.empty: 
                po_choices = df_po.apply(lambda x: f"{x['PO_Number']} ({x['Product_ID']})", axis=1).tolist()
                selected_po_str = st.selectbox("เลือกเลข PO", po_choices, index=None, placeholder="พิมพ์เพื่อค้นหา PO...")
                
                if selected_po_str:
                    sel_po = selected_po_str.split(" (")[0]
                    sel_pid = selected_po_str.split(" (")[1].replace(")", "")
                    found_row = df_po[(df_po['PO_Number'] == sel_po) & (df_po['Product_ID'] == sel_pid)]
                    
                    if not found_row.empty:
                        d = found_row.iloc[0].to_dict()
                        sheet_row_index = int(d['Sheet_Row_Index'])
                        st.success(f"พบข้อมูล PO: {sel_po}")
                        st.divider()
                    else:
                        st.error("ไม่พบข้อมูล")
                        return
                else:
                    st.info("กรุณาเลือก PO ที่ต้องการแก้ไข")
                    return
            else:
                st.warning("ยังไม่มีข้อมูล PO ในระบบ")
                return

        form_title = "เพิ่มรายการใหม่" if mode == "add" else f"แก้ไขรายการ: {d.get('PO_Number')}"
        st.markdown(f"#### {form_title}")

        st.markdown("##### 1. ค้นหารหัสสินค้า")
        product_options = df_master.apply(lambda x: f"{x['Product_ID']} : {x['Product_Name']}", axis=1).tolist()
        
        default_idx = None
        if mode == "search" and "Product_ID" in d:
             matches = [i for i, opt in enumerate(product_options) if opt.startswith(str(d["Product_ID"]))]
             if matches: default_idx = matches[0]

        selected_option = st.selectbox("ระบุสินค้า", product_options, index=default_idx, placeholder="🔍 Search...", label_visibility="collapsed")
        
        master_img_url = "https://via.placeholder.com/300x300.png?text=No+Image"
        master_pid = ""
        master_name = ""

        if selected_option:
            master_pid = selected_option.split(" : ")[0]
            row_info = df_master[df_master['Product_ID'] == master_pid].iloc[0]
            master_name = row_info['Product_Name']
            if row_info['Image']: master_img_url = row_info['Image']
        st.write("") 

        with st.container(border=True):
            col_left_img, col_right_form = st.columns([1.2, 3], gap="medium")
            with col_left_img:
                st.markdown(f"**{master_pid}**") 
                st.image(master_img_url, use_container_width=True)
                if master_name: st.caption(f"{master_name}")
            
            with col_right_form:
                with st.form("po_form", border=False):
                    st.markdown("###### 📄 ข้อมูลทั่วไป")
                    def get_date_val(val):
                        if not val or val == "" or val == "nan": return None
                        try: return datetime.strptime(str(val), "%Y-%m-%d").date()
                        except:
                            try: return datetime.strptime(str(val), "%d/%m/%Y").date()
                            except: return None
                    
                    r1c1, r1c2, r1c3 = st.columns(3)
                    po_num = r1c1.text_input("เลข PO *", value=d.get("PO_Number", ""), placeholder="ระบุเลข PO")
                    
                    def_order_date = get_date_val(d.get("Order_Date")) or date.today()
                    def_recv_date = get_date_val(d.get("Received_Date"))
                    
                    order_date = r1c2.date_input("วันที่สั่ง", value=def_order_date)
                    recv_date = r1c3.date_input("ของมา (ประมาณ)", value=def_recv_date)
                    
                    weight_txt = st.text_area("📦 น้ำหนักขนส่ง / รายละเอียด *", value=d.get("Transport_Weight", ""), height=100, placeholder="เช่น โกดังใหม่ 3 ลัง 54.99 kg...")
                    
                    st.markdown("###### 💰 ปริมาณ & ราคาต้นทุน")
                    r3c1, r3c2, r3c3, r3c4 = st.columns(4)
                    
                    def val_num(key, default=None):
                        v = d.get(key)
                        try: return float(v) if v and str(v) != "nan" and float(v) != 0 else default
                        except: return default

                    qty_ord = r3c1.number_input("สั่งมา *", min_value=0.0, step=0.0, value=val_num("Qty_Ordered"), placeholder="0") 
                    qty_rem = r3c2.number_input("เหลือ *", min_value=0.0, step=0.0, value=val_num("Qty_Remaining"), placeholder="0")
                    yuan_rate = r3c3.number_input("เรทหยวน *", min_value=0.0, step=0.0, format="%.2f", value=val_num("Yuan_Rate"), placeholder="0.00")
                    fees = r3c4.number_input("ค่าธรรมเนียม", min_value=0.0, step=0.0, format="%.2f", value=val_num("Fees"), placeholder="0.00")
                    
                    r4c1, r4c2, r4c3 = st.columns(3)
                    p_no_vat = r4c1.number_input("ราคาต่อชิ้นไม่รวม VAT", min_value=0.0, step=0.0, format="%.2f", value=val_num("Price_Unit_NoVAT"), placeholder="0.00")
                    p_1688_noship = r4c2.number_input("ราคา 1688 ไม่รวมส่ง", min_value=0.0, step=0.0, format="%.2f", value=val_num("Price_1688_NoShip"), placeholder="0.00")
                    p_1688_ship = r4c3.number_input("ราคา 1688 รวมส่ง *", min_value=0.0, step=0.0, format="%.2f", value=val_num("Price_1688_WithShip"), placeholder="0.00")

                    st.markdown("###### 🏷️ ราคาขาย & สรุป")
                    r5c1, r5c2, r5c3 = st.columns(3)
                    p_shopee = r5c1.number_input("Shopee", min_value=0.0, step=0.0, format="%.2f", value=val_num("Shopee_Price"), placeholder="0.00")
                    p_tiktok = r5c2.number_input("TikTok", min_value=0.0, step=0.0, format="%.2f", value=val_num("TikTok_Price"), placeholder="0.00")
                    
                    def_transport_idx = 0
                    if "Transport_Type" in d and d.get("Transport_Type") == "ส่งทางเรือ 🚢": def_transport_idx = 1
                    transport = r5c3.selectbox("การขนส่ง", ["ส่งทางรถ 🚛", "ส่งทางเรือ 🚢"], index=def_transport_idx)
                    
                    calc_guide = (qty_ord or 0) * (p_1688_ship or 0)
                    
                    st.markdown("---")
                    f_col1, f_col2 = st.columns([2, 1])
                    with f_col1:
                        st.caption(f"💡 ระบบคำนวณแนะนำ: {calc_guide:,.2f}")
                        initial_total = val_num("Total_Yuan")
                        if initial_total is None: initial_total = calc_guide if calc_guide > 0 else None
                        total_yuan_input = st.number_input("ราคาหยวนทั้งหมด *", min_value=0.0, step=0.0, format="%.2f", value=initial_total, placeholder="0.00")

                    with f_col2:
                        st.write(""); st.write("")
                        btn_label = "✅ ยืนยันเพิ่ม" if mode == "add" else "💾 บันทึกทับ"
                        submitted = st.form_submit_button(btn_label, type="primary", use_container_width=True)

                    if submitted:
                        errors = []
                        if not master_pid: errors.append("ยังไม่ได้เลือกสินค้า")
                        if not po_num: errors.append("ยังไม่ได้ระบุเลข PO")
                        if (qty_ord or 0) <= 0: errors.append("จำนวนสั่งซื้อต้องมากกว่า 0")
                        if (p_1688_ship or 0) <= 0: errors.append("ราคาต้นทุนรวมส่งต้องมากกว่า 0")
                        if (total_yuan_input or 0) <= 0: errors.append("ยอดรวมหยวนต้องมากกว่า 0")
                        
                        if errors: st.error(f"⚠️ บันทึกไม่ได้: {', '.join(errors)}")
                        else:
                            new_row = [
                                master_pid, po_num, order_date, recv_date, weight_txt, 
                                qty_ord or 0, qty_rem or 0, yuan_rate or 0, p_no_vat or 0, 
                                p_1688_noship or 0, p_1688_ship or 0, total_yuan_input or 0, 
                                p_shopee or 0, p_tiktok or 0, fees or 0, transport
                            ]
                            if save_po_to_sheet(new_row, row_index=sheet_row_index): 
                                st.success("บันทึกเรียบร้อย!")
                                st.rerun()

    # --- UI Logic ---
    col_head, col_action = st.columns([4, 2])
    with col_head: st.subheader("📋 รายการสั่งซื้อสินค้า (PO Log)")
    with col_action:
        b1, b2 = st.columns(2)
        with b1:
            if st.button("➕ เพิ่ม PO ใหม่", type="primary"): po_form_dialog(mode="add")
        with b2:
            if st.button("🔍 ค้นหา & แก้ไข PO", type="secondary"): po_form_dialog(mode="search")

    if not df_po.empty:
        df_po_display = pd.merge(df_po, df_master[['Product_ID', 'Image']], on='Product_ID', how='left')
        if "Image" in df_po_display.columns: df_po_display["Image"] = df_po_display["Image"].fillna("").astype(str)
        
        str_cols_po = ["Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", "Transport_Type"]
        for col in str_cols_po:
             if col in df_po_display.columns: df_po_display[col] = df_po_display[col].fillna("").astype(str)

        num_cols_po = ["Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Total_Yuan"]
        for col in num_cols_po:
             if col in df_po_display.columns: df_po_display[col] = pd.to_numeric(df_po_display[col], errors='coerce').fillna(0)

        po_display_cols = [
            "Image", "Product_ID", "PO_Number", "Order_Date", "Received_Date", "Transport_Weight", 
            "Qty_Ordered", "Qty_Remaining", "Yuan_Rate", "Total_Yuan", "Transport_Type"
        ]
        cols_to_show = [c for c in po_display_cols if c in df_po_display.columns]

        # [แก้ไข] เปลี่ยนหัวตาราง Tab 2 ให้เป็นภาษาไทย (ตาม Tab 1)
        st.data_editor(
            df_po_display[cols_to_show],
            column_config={
                "Image": st.column_config.ImageColumn("รูปสินค้า", width=80),
                "Product_ID": st.column_config.TextColumn("รหัสสินค้า", width=100),
                "PO_Number": st.column_config.TextColumn("เลข PO", width=100),
                "Order_Date": st.column_config.TextColumn("วันที่สั่ง", width=100), # เปลี่ยนจาก Order_Date
                "Received_Date": st.column_config.TextColumn("ของมา", width=100), # เปลี่ยนจาก Received_Date
                "Transport_Weight": st.column_config.TextColumn("น้ำหนักขนส่ง", width=200),
                "Qty_Ordered": st.column_config.NumberColumn("สั่งมา", format="%d"),
                "Qty_Remaining": st.column_config.NumberColumn("เหลือ", format="%d"),
                "Yuan_Rate": st.column_config.NumberColumn("เรทหยวน", format="%.2f"),
                "Total_Yuan": st.column_config.NumberColumn("ราคาหยวนทั้งหมด", format="%.2f ¥"),
                "Transport_Type": st.column_config.TextColumn("การขนส่ง"),
            },
            height=700, use_container_width=True, hide_index=True, disabled=True 
        )
    else:
        st.info("ยังไม่มีข้อมูลใบสั่งซื้อ")