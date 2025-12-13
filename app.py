import streamlit as st
import pandas as pd
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ==========================================
# 1. ตั้งค่าและเตรียมตัวแปร
# ==========================================
st.set_page_config(page_title="JST Smart Dashboard", layout="wide")

# ใส่ ID โฟลเดอร์ของคุณตรงนี้
FOLDER_ID_DATA_SALE = "1jFoara-yXT8FKy1hVjs3MyedG7O6lZRi"
FOLDER_ID_DATA_STOCK = "1x3K-oekbzob1f2wmgRkQfRx8Y4DY5Sq3"

# ==========================================
# 2. ฟังก์ชันเชื่อมต่อ Google Cloud (Drive & Sheets) - ฉบับแก้ไข
# ==========================================
@st.cache_resource
def get_credentials():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # กรณีรันบน Streamlit Cloud
    if "gcp_service_account" in st.secrets:
        secret_value = st.secrets["gcp_service_account"]
        
        # ตรวจสอบว่า Streamlit แปลงเป็น Dict ให้แล้วหรือยัง (กรณีใส่แบบ TOML)
        if isinstance(secret_value, dict):
            creds_dict = secret_value
        else:
            # กรณีเป็น String (ใส่แบบ JSON ในฟันหนู 3 ตัว)
            try:
                creds_dict = json.loads(secret_value)
            except json.JSONDecodeError:
                st.error("❌ รูปแบบ JSON ใน Secrets ไม่ถูกต้อง กรุณาตรวจสอบปีกกา { } หรือเครื่องหมายฟันหนู")
                return None

        # แก้ปัญหา \n ใน Private Key
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
        return service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)

    # กรณีรันในเครื่อง (Local)
    else:
        try:
            return service_account.Credentials.from_service_account_file("credentials.json", scopes=scope)
        except Exception:
            st.warning("ไม่พบไฟล์ credentials.json ในเครื่อง")
            return None
# ==========================================
# 3. ฟังก์ชันค้นหาและดาวน์โหลดไฟล์จากโฟลเดอร์
# ==========================================
def get_latest_dataframe_from_folder(folder_id, file_type="stock"):
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    
    try:
        # 1. ค้นหาไฟล์ในโฟลเดอร์ (เรียงตามเวลาแก้ไขล่าสุด)
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            orderBy='modifiedTime desc', # เอาไฟล์ใหม่สุดขึ้นก่อน
            pageSize=1, 
            fields="files(id, name, mimeType)"
        ).execute()
        
        items = results.get('files', [])
        
        if not items:
            st.warning(f"📂 ไม่พบไฟล์ในโฟลเดอร์ ID: {folder_id}")
            return pd.DataFrame()
            
        latest_file = items[0]
        file_id = latest_file['id']
        file_name = latest_file['name']
        mime_type = latest_file['mimeType']
        
        # แสดงชื่อไฟล์ที่กำลังใช้งาน
        st.toast(f"กำลังอ่านไฟล์: {file_name}", icon="📄")
        
        # 2. กรณีเป็น Google Sheet -> ใช้ gspread อ่าน
        if mime_type == 'application/vnd.google-apps.spreadsheet':
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(file_id)
            worksheet = sh.get_worksheet(0) # อ่านชีทแรก
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            
        # 3. กรณีเป็น Excel (xlsx) หรือ CSV -> ดาวน์โหลดแล้วอ่านด้วย Pandas
        else:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            fh.seek(0)
            if 'csv' in file_name.lower():
                df = pd.read_csv(fh)
            else:
                df = pd.read_excel(fh)

        # 4. แปลงหัวตาราง (Mapping)
        if file_type == 'stock':
            # แปลงชื่อไทย -> อังกฤษ (Stock)
            cols_map = {'รูปภาพ':'Image', 'รหัสสินค้า':'Product_ID', 'ชื่อสินค้า':'Product_Name', 'สินค้าคงคลัง':'Initial_Stock'}
            # กรองเฉพาะคอลัมน์ที่มี
            df = df.rename(columns={k:v for k,v in cols_map.items() if k in df.columns})
            # แปลงตัวเลข
            if 'Initial_Stock' in df.columns:
                df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'], errors='coerce').fillna(0)
                
        elif file_type == 'sale':
            # แปลงชื่อไทย -> อังกฤษ (Sale)
            cols_map = {'เวลาสั่งซื้อ':'Order_Time', 'ร้านค้า':'Shop', 'รหัสสินค้า':'Product_ID', 'จำนวน':'Qty_Sold'}
            df = df.rename(columns={k:v for k,v in cols_map.items() if k in df.columns})
            # แปลงวันที่และตัวเลข
            if 'Order_Time' in df.columns:
                df['Order_Time'] = pd.to_datetime(df['Order_Time'], errors='coerce')
                df['Date'] = df['Order_Time'].dt.date
            if 'Qty_Sold' in df.columns:
                df['Qty_Sold'] = pd.to_numeric(df['Qty_Sold'], errors='coerce').fillna(0)
                
        return df
        
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดในการอ่านไฟล์: {e}")
        return pd.DataFrame()

# ==========================================
# 4. ส่วนแสดงผล Dashboard
# ==========================================
st.title("📊 JST Auto-Sync Dashboard")
st.caption("ระบบจะดึงไฟล์ที่ **ใหม่ที่สุด** ในโฟลเดอร์ Google Drive มาแสดงผลอัตโนมัติ")

with st.spinner('กำลังสแกนหาไฟล์ล่าสุดใน Drive...'):
    df_stock = get_latest_dataframe_from_folder(FOLDER_ID_DATA_STOCK, "stock")
    df_sale = get_latest_dataframe_from_folder(FOLDER_ID_DATA_SALE, "sale")

if not df_stock.empty and not df_sale.empty:
    
    # --- ประมวลผลข้อมูล ---
    # รวมยอดขายตามรหัสสินค้า
    sold_summary = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
    
    # เชื่อมตาราง Stock + Sale
    merged = pd.merge(df_stock, sold_summary, on='Product_ID', how='left')
    merged['Qty_Sold'] = merged['Qty_Sold'].fillna(0)
    merged['Current_Stock'] = merged['Initial_Stock'] - merged['Qty_Sold']
    
    # สร้างสถานะแจ้งเตือน
    def get_status(val):
        if val <= 0: return "🔴 หมดเกลี้ยง"
        elif val < 10: return "⚠️ ใกล้หมด"
        else: return "🟢 มีของ"
    merged['Status'] = merged['Current_Stock'].apply(get_status)

    # --- แสดงผล Metric ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 รายการสินค้า (Stock)", f"{len(merged)} รายการ")
    c2.metric("💰 ขายออกไปแล้ว (Sale)", f"{int(df_sale['Qty_Sold'].sum())} ชิ้น")
    c3.metric("⚠️ ต้องเติมของ", f"{len(merged[merged['Current_Stock'] < 10])} รายการ")
    c4.metric("🏪 ร้านค้าที่ขาย", f"{df_sale['Shop'].nunique() if 'Shop' in df_sale.columns else 0} ร้าน")

    st.divider()

    # --- แสดงตารางและกราฟ ---
    tab1, tab2 = st.tabs(["📉 วิเคราะห์ยอดขาย", "📦 เช็คสต็อกคงเหลือ"])
    
    with tab1:
        col1, col2 = st.columns([2,1])
        with col1:
            if 'Date' in df_sale.columns:
                st.subheader("ยอดขายรายวัน")
                daily = df_sale.groupby('Date')['Qty_Sold'].sum().reset_index()
                st.bar_chart(daily.set_index('Date'))
        with col2:
            if 'Shop' in df_sale.columns:
                st.subheader("สัดส่วนร้านค้า")
                # แสดงเป็นตารางง่ายๆ หรือใช้ plotly ถ้า import มา
                shop_summ = df_sale.groupby('Shop')['Qty_Sold'].sum().reset_index()
                st.dataframe(shop_summ, hide_index=True, use_container_width=True)
                
        st.subheader("รายการขายล่าสุด (จากไฟล์ล่าสุด)")
        st.dataframe(df_sale.head(10), use_container_width=True)

    with tab2:
        st.subheader("สถานะสต็อกปัจจุบัน (Stock ล่าสุด - Sale ล่าสุด)")
        
        status_filter = st.multiselect("กรองสถานะ:", ["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด", "🟢 มีของ"], default=["🔴 หมดเกลี้ยง", "⚠️ ใกล้หมด"])
        display_df = merged[merged['Status'].isin(status_filter)]
        
        st.data_editor(
            display_df[['Image', 'Product_ID', 'Product_Name', 'Initial_Stock', 'Qty_Sold', 'Current_Stock', 'Status']],
            column_config={
                "Image": st.column_config.ImageColumn("รูปสินค้า"),
                "Product_ID": "รหัส",
                "Product_Name": "ชื่อสินค้า",
                "Initial_Stock": st.column_config.NumberColumn("สต็อกตั้งต้น"),
                "Qty_Sold": st.column_config.NumberColumn("ขายไป"),
                "Current_Stock": st.column_config.ProgressColumn("คงเหลือ", format="%d", min_value=0, max_value=int(merged['Initial_Stock'].max())),
            },
            use_container_width=True,
            height=600,
            hide_index=True
        )

else:
    st.info("กำลังรอการเชื่อมต่อ... หรือไม่พบไฟล์ในโฟลเดอร์")
    st.write("คำแนะนำ: โปรดตรวจสอบว่ามีไฟล์ Excel (.xlsx) อยู่ในโฟลเดอร์ DATA STOCK และ DATA SALE บน Google Drive แล้ว")