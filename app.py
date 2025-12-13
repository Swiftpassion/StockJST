import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px

# --- ตั้งค่าหน้าเว็บ ---
st.set_page_config(page_title="JST Stock Dashboard", layout="wide")

# --- ฟังก์ชันเชื่อมต่อ Google ---
@st.cache_resource
def init_connection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"เชื่อมต่อไม่ได้: {e}")
        return None

# --- ฟังก์ชันโหลดข้อมูล ---
def load_data(sheet_id, type_):
    client = init_connection()
    if not client: return pd.DataFrame()
    try:
        sheet = client.open_by_key(sheet_id).sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        if type_ == 'stock':
            # แปลงชื่อไทย -> อังกฤษ
            cols = {'รูปภาพ':'Image', 'รหัสสินค้า':'Product_ID', 'ชื่อสินค้า':'Product_Name', 'สินค้าคงคลัง':'Initial_Stock'}
            df = df.rename(columns={k:v for k,v in cols.items() if k in df.columns})
            if 'Initial_Stock' in df.columns:
                df['Initial_Stock'] = pd.to_numeric(df['Initial_Stock'], errors='coerce').fillna(0)
            return df
            
        elif type_ == 'sale':
            cols = {'เวลาสั่งซื้อ':'Order_Time', 'ร้านค้า':'Shop', 'รหัสสินค้า':'Product_ID', 'จำนวน':'Qty_Sold'}
            df = df.rename(columns={k:v for k,v in cols.items() if k in df.columns})
            if 'Order_Time' in df.columns:
                df['Order_Time'] = pd.to_datetime(df['Order_Time'], errors='coerce')
                df['Date'] = df['Order_Time'].dt.date
            if 'Qty_Sold' in df.columns:
                df['Qty_Sold'] = pd.to_numeric(df['Qty_Sold'], errors='coerce').fillna(0)
            return df
    except Exception as e:
        st.error(f"อ่านไฟล์ผิดพลาด: {e}")
        return pd.DataFrame()

# ==========================================
# ⚡ แก้ ID ตรงนี้ครับ ⚡
# ==========================================
STOCK_ID = "1x3K-oekbzob1f2wmgRkQfRx8Y4DY5Sq3"
SALE_ID = "1jFoara-yXT8FKy1hVjs3MyedG7O6lZRi"
# ==========================================

st.title("📊 JST Dashboard: สรุปยอดขาย & สต็อกคงเหลือ")

with st.spinner('กำลังดึงข้อมูล...'):
    if "1vnn913SYfbgqYHmCdL9Qho7R54q4AKshv2s92IPs-XQ" in SALE_ID:
        st.warning("⚠️ กรุณาใส่รหัสไฟล์ Sale ในโค้ดบรรทัดที่ 84 ก่อนครับ")
        st.stop()
        
    df_stock = load_data(STOCK_ID, 'stock')
    df_sale = load_data(SALE_ID, 'sale')

if not df_stock.empty and not df_sale.empty:
    # คำนวณสต็อก
    sold = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
    merged = pd.merge(df_stock, sold, on='Product_ID', how='left')
    merged['Qty_Sold'] = merged['Qty_Sold'].fillna(0)
    merged['Current_Stock'] = merged['Initial_Stock'] - merged['Qty_Sold']
    
    merged['Status'] = merged['Current_Stock'].apply(lambda x: "🔴 หมด" if x<=0 else ("⚠️ ใกล้หมด" if x<10 else "🟢 ปกติ"))

    # แสดงผล
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 สินค้าทั้งหมด", len(merged))
    c2.metric("💰 ขายออก (ชิ้น)", int(df_sale['Qty_Sold'].sum()))
    c3.metric("⚠️ ต้องเติมของ", len(merged[merged['Current_Stock']<10]))
    c4.metric("🏪 ร้านค้า", df_sale['Shop'].nunique() if 'Shop' in df_sale.columns else 0)

    tab1, tab2 = st.tabs(["📉 วิเคราะห์ยอดขาย", "📦 เช็คสต็อก (Real-time)"])
    
    with tab1:
        if 'Date' in df_sale.columns:
            st.subheader("ยอดขายรายวัน")
            daily = df_sale.groupby('Date')['Qty_Sold'].sum().reset_index()
            st.bar_chart(daily.set_index('Date'))
        
        if 'Shop' in df_sale.columns:
            st.subheader("สัดส่วนยอดขาย")
            fig = px.pie(df_sale, values='Qty_Sold', names='Shop', hole=0.4)
            st.plotly_chart(fig)
            
        st.dataframe(df_sale.sort_values('Order_Time', ascending=False).head(10), use_container_width=True)

    with tab2:
        st.subheader("ตารางตัดสต็อก (Stock - Sale = คงเหลือ)")
        status_filter = st.multiselect("เลือกสถานะ:", ["🔴 หมด", "⚠️ ใกล้หมด", "🟢 ปกติ"], default=["🔴 หมด", "⚠️ ใกล้หมด"])
        show = merged[merged['Status'].isin(status_filter)]
        
        st.data_editor(
            show[['Image', 'Product_ID', 'Product_Name', 'Initial_Stock', 'Qty_Sold', 'Current_Stock', 'Status']],
            column_config={
                "Image": st.column_config.ImageColumn("รูป"),
                "Product_ID": "รหัส",
                "Product_Name": "ชื่อสินค้า",
                "Initial_Stock": "ตั้งต้น",
                "Qty_Sold": "ขายไป",
                "Current_Stock": st.column_config.ProgressColumn("คงเหลือ", format="%d", min_value=0, max_value=int(merged['Initial_Stock'].max())),
            },
            use_container_width=True, height=600, hide_index=True
        )
else:
    st.error("ไม่พบข้อมูล ตรวจสอบ ID ไฟล์ หรือ ชื่อหัวตารางใน Excel")