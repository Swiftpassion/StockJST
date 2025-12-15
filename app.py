import streamlit as st
import pandas as pd
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ==========================================
# 1. ตั้งค่า Page & CSS Styles
# ==========================================
st.set_page_config(page_title="JST Hybrid Dashboard", layout="wide")

# CSS: Card UI + จัดกึ่งกลางหัวตาราง
st.markdown("""
<style>
    /* Card Container */
    .metric-card {
        background-color: #1a1a1a;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    /* Text Styles */
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
    .metric-sub {
        font-size: 12px;
        margin-top: 5px;
    }
    /* Border & Text Colors */
    .border-cyan { border-left: 4px solid #00e5ff; }
    .border-gold { border-left: 4px solid #ffd700; }
    .border-red  { border-left: 4px solid #ff4d4d; }
    .text-cyan { color: #00e5ff !important; }
    .text-gold { color: #ffd700 !important; }
    .text-red  { color: #ff4d4d !important; }
    
    /* จัดกึ่งกลางหัวตาราง */
    [data-testid="stDataFrame"] th {
        text-align: center !important;
    }
</style>
""", unsafe_allow_html=True)

# Stock: Master Sheet ID
MASTER_SHEET_ID = "1SC_Dpq2aiMWsS3BGqL_Rdf7X4qpTFkPA0wPV6mqqosI"
TAB_NAME_STOCK = "MASTER"

# Sale: Folder ID
FOLDER_ID_DATA_SALE = "12jyMKgFHoc9-_eRZ-VN9QLsBZ31ZJP4T"

# ==========================================
# 2. เชื่อมต่อ Google Cloud
# ==========================================
@st.cache_resource
def get_credentials():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" in st.secrets:
        creds_dict = json.loads(st.secrets["gcp_service_account"]) if isinstance(st.secrets["gcp_service_account"], str) else dict(st.secrets["gcp_service_account"])
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        return service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)
    return service_account.Credentials.from_service_account_file("credentials.json", scopes=scope)

# ==========================================
# 3. ฟังก์ชันดึงข้อมูล
# ==========================================
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
        st.error(f"❌ อ่าน Master Sheet ไม่สำเร็จ: {e}")
        return pd.DataFrame()

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
        st.error(f"❌ อ่านไฟล์ Excel Sale ไม่สำเร็จ: {e}")
        return pd.DataFrame()

# ==========================================
# 4. แสดงผล Dashboard
# ==========================================
st.title("📊 JST Hybrid Dashboard")

if st.button("🔄 อัปเดตข้อมูล"):
    st.cache_data.clear()

with st.spinner('กำลังรวมข้อมูล Stock (Sheet) และ Sale (Excel)...'):
    df_stock = get_stock_from_sheet()
    df_sale = get_sale_from_folder()

if not df_stock.empty and not df_sale.empty:
    # --- Data Processing ---
    sold_summary = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
    merged = pd.merge(df_stock, sold_summary, on='Product_ID', how='left')
    merged['Qty_Sold'] = merged['Qty_Sold'].fillna(0)
    merged['Current_Stock'] = merged['Initial_Stock'] - merged['Qty_Sold']
    
    def get_status(val):
        if val <= 0: return "🔴 หมดเกลี้ยง"
        elif val < 10: return "⚠️ ใกล้หมด"
        else: return "🟢 มีของ"
    merged['Status'] = merged['Current_Stock'].apply(get_status)

    # --- 1. Custom Metrics Cards ---
    total_items = len(merged)
    total_sold = int(merged['Qty_Sold'].sum())
    total_restock = len(merged[merged['Current_Stock'] < 10])

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="metric-card border-cyan">
            <div class="metric-title">สินค้าทั้งหมด</div>
            <div class="metric-value text-cyan">{total_items:,} <span style="font-size:16px; color:#fff;">รายการ</span></div>
            <div class="metric-sub" style="color:#00e5ff;">100% Stock</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="metric-card border-gold">
            <div class="metric-title">💰 ขายไปแล้ว</div>
            <div class="metric-value text-gold">{total_sold:,} <span style="font-size:16px; color:#fff;">ชิ้น</span></div>
            <div class="metric-sub" style="color:#ffd700;">Active Sales</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="metric-card border-red">
            <div class="metric-title">⚠️ ต้องเติมของ</div>
            <div class="metric-value text-red">{total_restock:,} <span style="font-size:16px; color:#fff;">รายการ</span></div>
            <div class="metric-sub" style="color:#ff4d4d;">Critical Stock</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # --- 2. Filter & Search (เปลี่ยนเป็น Dropdown ค้นหาได้) ---
    st.subheader("📦 เช็คสถานะสินค้าล่าสุด")
    
    col_filter_1, col_filter_2 = st.columns([2, 1])
    
    with col_filter_1:
        filter_options = ["📦 สินค้าทั้งหมด", "🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"]
        status_filter = st.multiselect("กรองสถานะ", filter_options, default=["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด"])
        
    with col_filter_2:
        # เตรียมตัวเลือกค้นหา: รวม "ชื่อสินค้า (รหัส)" เพื่อให้ User พิมพ์ค้นหาได้ง่าย
        merged['Search_Label'] = merged.apply(lambda x: f"{x['Product_Name']} ({x['Product_ID']})", axis=1)
        search_options = merged['Search_Label'].tolist()
        
        # ใช้ selectbox แบบ searchable (พิมพ์แล้วตัวเลือกกรองตาม)
        selected_product = st.selectbox(
            "🔍 ค้นหา (พิมพ์ชื่อสินค้า หรือ รหัส)",
            options=search_options,
            index=None,  # เริ่มต้นเป็นค่าว่าง
            placeholder="พิมพ์เพื่อค้นหารายการ..."
        )
    
    # --- Logic การกรอง 2 ชั้น ---
    
    # 1. กรองสถานะ
    if "📦 สินค้าทั้งหมด" in status_filter or not status_filter:
        show_df = merged.copy()
    else:
        show_df = merged[merged['Status'].isin(status_filter)].copy()
        
    # 2. กรองจาก Selectbox ที่เลือกมา
    if selected_product:
        # กรองเอาเฉพาะรายการที่ตรงกับที่เลือกใน Dropdown
        show_df = show_df[show_df['Search_Label'] == selected_product]
    
    show_df = show_df.sort_values(by='Current_Stock')
    
    # --- 3. Table Display ---
    st.data_editor(
        show_df[['Image', 'Product_ID', 'Product_Name', 'Initial_Stock', 'Qty_Sold', 'Current_Stock', 'Status']],
        column_config={
            "Image": st.column_config.ImageColumn(
                "รูปสินค้า", 
                width="medium", 
                help="รูปสินค้าจาก Master Sheet"
            ),
            "Current_Stock": st.column_config.ProgressColumn(
                "คงเหลือ", 
                format="%d", 
                min_value=0, 
                max_value=int(merged['Initial_Stock'].max()) if len(merged) > 0 else 100
            ),
            "Qty_Sold": st.column_config.NumberColumn("ขายแล้ว"),
            "Product_Name": st.column_config.TextColumn("ชื่อสินค้า", width="medium"),
            "Product_ID": st.column_config.TextColumn("รหัสสินค้า"),
        },
        use_container_width=True,
        height=800,
        hide_index=True,
        row_height=80 # คงค่าความสูงไว้ 80 ตามที่คุยกันล่าสุด
    )

else:
    st.warning("⚠️ ข้อมูลยังมาไม่ครบ โปรดตรวจสอบสิทธิ์การเข้าถึงไฟล์")