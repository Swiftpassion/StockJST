import streamlit as st
import pandas as pd
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ==========================================
# 1. ตั้งค่า ID (ถูกต้องตามรูปภาพของคุณ)
# ==========================================
st.set_page_config(page_title="JST Hybrid Dashboard", layout="wide")

# Stock: ใช้ Master Sheet ID (จากรูปที่ 1)
MASTER_SHEET_ID = "1SC_Dpq2aiMWsS3BGqL_Rdf7X4qpTFkPA0wPV6mqqosI"
TAB_NAME_STOCK = "MASTER"  # ชื่อแท็บในรูปที่ 1

# Sale: ใช้ Folder ID (จากรูปที่ 3)
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
# 3. ฟังก์ชันดึงข้อมูล (Hybrid)
# ==========================================
def get_stock_from_sheet():
    """ดึงสต็อกสินค้าจาก Google Sheet (Master)"""
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = sh.worksheet(TAB_NAME_STOCK)
        df = pd.DataFrame(ws.get_all_records())
        
        # แปลงชื่อคอลัมน์ (ตามรูปที่ 1)
        col_map = {'รูปภาพ':'Image', 'รหัสสินค้า':'Product_ID', 'ชื่อสินค้า':'Product_Name', 'สินค้าคงคลัง':'Initial_Stock'}
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
        # แปลงตัวเลข
        if 'Initial_Stock' in df.columns:
            df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
        return df
    except Exception as e:
        st.error(f"❌ อ่าน Master Sheet ไม่สำเร็จ: {e}")
        return pd.DataFrame()

def get_sale_from_folder():
    """ดึงยอดขายจากไฟล์ Excel ล่าสุดใน Drive Folder"""
    try:
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        
        # หาไฟล์ล่าสุดในโฟลเดอร์ Sale
        results = service.files().list(
            q=f"'{FOLDER_ID_DATA_SALE}' in parents and trashed=false",
            orderBy='modifiedTime desc', pageSize=1, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        if not items: return pd.DataFrame()
        
        # ดาวน์โหลด
        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        fh.seek(0)
        
        df = pd.read_excel(fh)
        
        # แปลงชื่อคอลัมน์ (ปรับตามไฟล์ JST 12.12)
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
    # รวมข้อมูล
    sold_summary = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
    merged = pd.merge(df_stock, sold_summary, on='Product_ID', how='left')
    merged['Qty_Sold'] = merged['Qty_Sold'].fillna(0)
    merged['Current_Stock'] = merged['Initial_Stock'] - merged['Qty_Sold']
    
    # กำหนดสถานะ
    def get_status(val):
        if val <= 0: return "🔴 หมดเกลี้ยง"
        elif val < 10: return "⚠️ ใกล้หมด"
        else: return "🟢 มีของ"
    merged['Status'] = merged['Current_Stock'].apply(get_status)

    # Metric
    c1, c2, c3 = st.columns(3)
    c1.metric("📦 สินค้าทั้งหมด", f"{len(merged)} รายการ")
    c2.metric("💰 ขายไปแล้ว", f"{int(merged['Qty_Sold'].sum())} ชิ้น")
    c3.metric("⚠️ ต้องเติมของ", f"{len(merged[merged['Current_Stock'] < 10])} รายการ")
    
    st.divider()
    
    # ตารางเช็คสต็อก
    st.subheader("📦 เช็คสถานะสินค้าล่าสุด")
    status_filter = st.multiselect("กรองสถานะ", ["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"], default=["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด"])
    show_df = merged[merged['Status'].isin(status_filter)].sort_values(by='Current_Stock')
    
    st.data_editor(
        show_df[['Image', 'Product_ID', 'Product_Name', 'Initial_Stock', 'Qty_Sold', 'Current_Stock', 'Status']],
        column_config={
            "Image": st.column_config.ImageColumn("รูป"),
            "Current_Stock": st.column_config.ProgressColumn("คงเหลือ", format="%d", min_value=0, max_value=int(merged['Initial_Stock'].max()))
        },
        use_container_width=True,
        height=600,
        hide_index=True
    )
else:
    st.warning("⚠️ ข้อมูลยังมาไม่ครบ โปรดตรวจสอบสิทธิ์การเข้าถึงไฟล์")