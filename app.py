import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
import json

# --- ตั้งค่าหน้าเว็บ ---
st.set_page_config(page_title="JST Stock Dashboard", layout="wide")

# --- ฟังก์ชันเชื่อมต่อ Google ---
@st.cache_resource
def init_connection():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        # อ่าน Secrets (รองรับทั้ง String และ Dict)
        if "gcp_service_account" in st.secrets:
            secret_value = st.secrets["gcp_service_account"]
            if isinstance(secret_value, str):
                creds_dict = json.loads(secret_value)
            else:
                creds_dict = dict(secret_value)
            
            # แก้ \n ใน Private Key
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ เชื่อมต่อไม่ได้: {e}")
        return None

# --- ฟังก์ชันโหลดข้อมูล ---
def load_data(sheet_id, type_):
    client = init_connection()
    if not client: return pd.DataFrame()
    try:
        # เปิดไฟล์ด้วย ID
        sheet = client.open_by_key(sheet_id).sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        if type_ == 'stock':
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
        st.info(f"เช็ค ID: {sheet_id} ว่าเป็น ID ของไฟล์ (ไม่ใช่โฟลเดอร์) หรือไม่")
        return pd.DataFrame()

# ==========================================
# ⚡ แก้ ID ตรงนี้ครับ ⚡
# ==========================================

# ✅ STOCK_ID: อันนี้ผมแก้ให้ถูกต้องแล้ว (เป็น ID ไฟล์ ไม่ใช่ ID โฟลเดอร์)
STOCK_ID = "1vnn913SYfbgqYHmCdL9Qho7R54q4AKshv2s92IPs-XQ"

# ⚠️ SALE_ID: อันนี้คุณต้องแก้เอง! (ตอนนี้มันยังเป็น ID โฟลเดอร์อยู่ ใช้ไม่ได้)
# วิธีหา: เข้าโฟลเดอร์ DATA SALE -> เปิดไฟล์ Excel -> ก๊อป ID บนลิงก์มาใส่
SALE_ID = "1jFoara-yXT8FKy1hVjs3MyedG7O6lZRi"  # <--- ❌ ลบอันนี้ แล้วเอา ID ไฟล์มาใส่

# ==========================================

st.title("📊 JST Dashboard: สรุปยอดขาย & สต็อกคงเหลือ")

with st.spinner('กำลังดึงข้อมูล...'):
    # เช็คว่าผู้ใช้ใส่ ID โฟลเดอร์มาหรือไม่ (ดักจับ ID โฟลเดอร์ที่คุณชอบเผลอใส่มา)
    if SALE_ID == "1jFoara-yXT8FKy1hVjs3MyedG7O6lZRi":
        st.error("🚨 คุณยังใส่ ID ของ 'โฟลเดอร์' อยู่ครับ!")
        st.warning("โปรแกรมต้องการ ID ของ 'ไฟล์'.. กรุณาเปิดไฟล์ Excel ยอดขาย แล้วก๊อปปี้ ID จาก URL มาใส่ในบรรทัดที่ 88 ครับ")
        st.image("https://i.imgur.com/K3bM5bB.png", caption="ตัวอย่าง: ต้องเอา ID ตรงกรอบสีแดง (ของไฟล์) มาใส่นะครับ")
        st.stop()
        
    df_stock = load_data(STOCK_ID, 'stock')
    df_sale = load_data(SALE_ID, 'sale')

if not df_stock.empty and not df_sale.empty:
    sold = df_sale.groupby('Product_ID')['Qty_Sold'].sum().reset_index()
    merged = pd.merge(df_stock, sold, on='Product_ID', how='left')
    merged['Qty_Sold'] = merged['Qty_Sold'].fillna(0)
    merged['Current_Stock'] = merged['Initial_Stock'] - merged['Qty_Sold']
    
    merged['Status'] = merged['Current_Stock'].apply(lambda x: "🔴 หมด" if x<=0 else ("⚠️ ใกล้หมด" if x<10 else "🟢 ปกติ"))

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
        st.subheader("ตารางตัดสต็อก")
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
    st.info("...รอข้อมูล...")