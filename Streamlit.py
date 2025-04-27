import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os
import requests
from io import BytesIO
import zipfile

# Sahifa sarlavhasi
st.set_page_config(page_title="Chinook Ma'lumotlar Bazasi Tahlili", layout="wide")
st.title("Chinook Ma'lumotlar Bazasi Tahlili")

# Funktsiya GitHub'dan ma'lumotlarni yuklab olish uchun
@st.cache_data
def download_and_extract_data():
    # GitHub API orqali fayllarni topish
    github_repo = "UktambekA/Chinook-Data"
    github_url = f"https://api.github.com/repos/{github_repo}/contents"
    try:
        # Repo contentlarini olish
        r = requests.get(github_url)
        r.raise_for_status()
        contents = r.json()
        
        # Vaqtinchalik joyga saqlash
        temp_dir = "temp_data"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        
        # SQLite fayl qidirish
        db_path = None
        for item in contents:
            if item['name'].endswith('.db') or item['name'].endswith('.sqlite'):
                # Faylni yuklab olish
                download_url = item['download_url']
                db_filename = os.path.join(temp_dir, item['name'])
                
                # Faylni yuklab olish
                db_response = requests.get(download_url)
                db_response.raise_for_status()
                
                # Faylni saqlash
                with open(db_filename, 'wb') as f:
                    f.write(db_response.content)
                
                db_path = db_filename
                break
        
        # Agar repo fayllar ro'yxatida topolmasak, rekursiv qidiruv
        if not db_path:
            # Kataloglarni qidirish
            for item in contents:
                if item['type'] == 'dir':
                    dir_url = f"https://api.github.com/repos/{github_repo}/contents/{item['path']}"
                    dir_response = requests.get(dir_url)
                    if dir_response.status_code == 200:
                        dir_contents = dir_response.json()
                        for dir_item in dir_contents:
                            if dir_item['name'].endswith('.db') or dir_item['name'].endswith('.sqlite'):
                                # Faylni yuklab olish
                                download_url = dir_item['download_url']
                                db_filename = os.path.join(temp_dir, dir_item['name'])
                                
                                # Faylni yuklab olish
                                db_response = requests.get(download_url)
                                db_response.raise_for_status()
                                
                                # Faylni saqlash
                                with open(db_filename, 'wb') as f:
                                    f.write(db_response.content)
                                
                                db_path = db_filename
                                break
        
        # Agar db fayl topilsa
        if db_path:
            return db_path
        
        # Topilmasa xatolik
        st.error("GitHub repozitorida ma'lumotlar bazasi (.db yoki .sqlite) fayli topilmadi!")
        
        # Agar hech qanday db fayl topilmasa, Chinook DB ni internetdan yuklab olishga urinish
        st.info("Chinook ma'lumotlar bazasini internetdan yuklab olishga urinish...")
        chinook_url = "https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"
        try:
            chinook_response = requests.get(chinook_url)
            chinook_response.raise_for_status()
            
            # Zip faylni ochish
            z = zipfile.ZipFile(BytesIO(chinook_response.content))
            z.extractall(temp_dir)
            
            # Chinook.db ni qidirish
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith(".db") or file.endswith(".sqlite"):
                        db_path = os.path.join(root, file)
                        return db_path
            
            st.error("Internetdan ham ma'lumotlar bazasi faylini topolmadik!")
            return None
        except Exception as e:
            st.error(f"Internetdan ma'lumotlarni yuklab olishda xatolik: {e}")
            return None
            
    except Exception as e:
        st.error(f"Ma'lumotlarni yuklab olishda xatolik: {e}")
        return None

# Ma'lumotlar bazasiga ulanish
@st.cache_resource
def connect_to_db(db_path):
    if db_path:
        try:
            # SQLite ulanishni yaratishda check_same_thread=False parametrini qo'shamiz
            conn = sqlite3.connect(db_path, check_same_thread=False)
            return conn
        except Exception as e:
            st.error(f"Ma'lumotlar bazasiga ulanishda xatolik: {e}")
            return None
    return None

# Ma'lumotlarni olib kelish
@st.cache_data
def get_table_names(_conn):
    if _conn:
        cursor = _conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    return []

@st.cache_data
def get_table_data(_conn, table_name):
    if _conn:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql_query(query, _conn)
    return None

@st.cache_data
def get_artists_with_album_count(_conn):
    if _conn:
        query = """
        SELECT Artist.ArtistId, Artist.Name, COUNT(Album.AlbumId) as AlbumCount
        FROM Artist
        LEFT JOIN Album ON Artist.ArtistId = Album.ArtistId
        GROUP BY Artist.ArtistId
        ORDER BY AlbumCount DESC
        """
        return pd.read_sql_query(query, _conn)
    return None

@st.cache_data
def get_top_selling_tracks(_conn):
    if _conn:
        query = """
        SELECT Track.Name as TrackName, Artist.Name as ArtistName, 
               COUNT(InvoiceLine.InvoiceLineId) as Sales, SUM(InvoiceLine.UnitPrice) as Revenue
        FROM InvoiceLine
        JOIN Track ON InvoiceLine.TrackId = Track.TrackId
        JOIN Album ON Track.AlbumId = Album.AlbumId
        JOIN Artist ON Album.ArtistId = Artist.ArtistId
        GROUP BY Track.TrackId
        ORDER BY Sales DESC
        LIMIT 20
        """
        return pd.read_sql_query(query, _conn)
    return None

@st.cache_data
def get_sales_by_country(_conn):
    if _conn:
        query = """
        SELECT Customer.Country, COUNT(Invoice.InvoiceId) as InvoiceCount, 
               SUM(Invoice.Total) as TotalSales
        FROM Invoice
        JOIN Customer ON Invoice.CustomerId = Customer.CustomerId
        GROUP BY Customer.Country
        ORDER BY TotalSales DESC
        """
        return pd.read_sql_query(query, _conn)
    return None

@st.cache_data
def get_genre_stats(_conn):
    if _conn:
        query = """
        SELECT Genre.Name, COUNT(Track.TrackId) as TrackCount
        FROM Track
        JOIN Genre ON Track.GenreId = Genre.GenreId
        GROUP BY Genre.GenreId
        ORDER BY TrackCount DESC
        """
        return pd.read_sql_query(query, _conn)
    return None

# Ma'lumotlarni yuklab olish
with st.spinner("Ma'lumotlar bazasini yuklab olish..."):
    db_path = download_and_extract_data()
    
if db_path:
    st.success(f"Ma'lumotlar bazasi muvaffaqiyatli yuklandi: {os.path.basename(db_path)}")
    conn = connect_to_db(db_path)
else:
    st.error("Ma'lumotlar bazasini yuklab olishda muammo yuzaga keldi.")
    conn = None

if conn:
    # Menyu
    menu = st.sidebar.selectbox(
        "Bo'limni tanlang",
        ["Umumiy Ma'lumot", "Jadvallar", "Artistlar", "Treklar", "Sotuvlar", "Janrlar"]
    )
    
    if menu == "Umumiy Ma'lumot":
        st.header("Chinook Ma'lumotlar Bazasi Haqida Umumiy Ma'lumot")
        st.write("""
        Chinook ma'lumotlar bazasi - bu raqamli media do'koni uchun namunali ma'lumotlar bazasi. 
        U quyidagi asosiy jadvallarni o'z ichiga oladi:
        - **Artist va Album**: Musiqa ijodkorlari va ularning albumlari
        - **Track**: Barcha musiqiy treklar va ularning ma'lumotlari
        - **Customer va Invoice**: Mijozlar va ularning xaridlari
        - **Employee**: Do'kon xodimlari
        - **Genre va MediaType**: Musiqa janrlari va media turlari
        """)
        
        # Database schema rasmi (agar bo'lsa)
        st.subheader("Ma'lumotlar bazasi sxemasi")
        st.image("https://www.sqlitetutorial.net/wp-content/uploads/2015/11/sqlite-sample-database-color.jpg", 
                 caption="Chinook ma'lumotlar bazasi sxemasi")
        
        # Bazadagi jadvallar haqida ma'lumot
        tables = get_table_names(conn)
        st.subheader(f"Bazada {len(tables)} ta jadval mavjud:")
        for table in tables:
            df = get_table_data(conn, table)
            if df is not None:
                st.write(f"**{table}**: {df.shape[0]} ta qator, {df.shape[1]} ta ustun")
    
    elif menu == "Jadvallar":
        st.header("Jadvallar ma'lumotlari")
        
        tables = get_table_names(conn)
        selected_table = st.selectbox("Jadvalni tanlang", tables)
        
        if selected_table:
            df = get_table_data(conn, selected_table)
            if df is not None:
                st.write(f"**{selected_table}** jadvali ({df.shape[0]} ta qator, {df.shape[1]} ta ustun)")
                
                # Jadvalni ko'rsatish
                st.dataframe(df)
                
                # Jadval ma'lumotlari haqida qo'shimcha statistika
                st.subheader("Jadval statistikasi")
                st.write(df.describe())
    
    elif menu == "Artistlar":
        st.header("Artistlar va Albumlar")
        
        # Top artistlar
        artists_data = get_artists_with_album_count(conn)
        if artists_data is not None:
            st.subheader("Top 20 eng ko'p albumli artistlar")
            fig, ax = plt.subplots(figsize=(12, 8))
            top_artists = artists_data.head(20)
            sns.barplot(x="AlbumCount", y="Name", data=top_artists, ax=ax)
            plt.xlabel("Albumlar soni")
            plt.ylabel("Artist nomi")
            plt.tight_layout()
            st.pyplot(fig)
            
            # Artist ma'lumotlarini ko'rsatish
            st.subheader("Barcha artistlar")
            st.dataframe(artists_data)
    
    elif menu == "Treklar":
        st.header("Musiqiy Treklar")
        
        # Top-selling tracks
        top_tracks = get_top_selling_tracks(conn)
        if top_tracks is not None:
            st.subheader("Eng ko'p sotilgan 20 ta trek")
            fig, ax = plt.subplots(figsize=(12, 8))
            sns.barplot(x="Sales", y="TrackName", data=top_tracks, ax=ax)
            plt.xlabel("Sotuvlar soni")
            plt.ylabel("Trek nomi")
            plt.tight_layout()
            st.pyplot(fig)
            
            # Trek ma'lumotlarini ko'rsatish
            st.dataframe(top_tracks)
    
    elif menu == "Sotuvlar":
        st.header("Sotuvlar Statistikasi")
        
        # Sales by country
        sales_data = get_sales_by_country(conn)
        if sales_data is not None:
            st.subheader("Mamlakatlar bo'yicha sotuvlar")
            
            # Diagramma
            fig, ax = plt.subplots(figsize=(12, 8))
            top_countries = sales_data.head(15)
            sns.barplot(x="TotalSales", y="Country", data=top_countries, ax=ax)
            plt.xlabel("Jami sotuvlar ($)")
            plt.ylabel("Mamlakat")
            plt.tight_layout()
            st.pyplot(fig)
            
            # Sotuvlar ma'lumotlarini ko'rsatish
            st.dataframe(sales_data)
    
    elif menu == "Janrlar":
        st.header("Musiqa Janrlari")
        
        # Genre statistics
        genre_data = get_genre_stats(conn)
        if genre_data is not None:
            st.subheader("Janrlar bo'yicha treklar soni")
            
            # Diagramma
            fig, ax = plt.subplots(figsize=(12, 8))
            colors = sns.color_palette("viridis", len(genre_data))
            plt.pie(genre_data["TrackCount"], labels=genre_data["Name"], autopct='%1.1f%%', 
                    startangle=90, colors=colors)
            plt.axis('equal')
            st.pyplot(fig)
            
            # Janrlar ma'lumotlarini ko'rsatish
            st.dataframe(genre_data)
else:
    st.error("Ma'lumotlar bazasiga ulanib bo'lmadi. Iltimos, qayta urinib ko'ring.")

# Footer
st.markdown("---")
st.caption("Chinook ma'lumotlar bazasi tahlili | UktambekA/Chinook-Data")
