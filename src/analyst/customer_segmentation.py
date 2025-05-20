import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import glob
import os

def run_kmeans(input_path, output_dir="data/kmeans", n_clusters=3):

    csv_files = glob.glob(f"{input_path}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"Không tìm thấy file CSV nào trong {input_path}")
    csv_file = csv_files[0]
    df = pd.read_csv(csv_file)

    df["Behavior"] = df["Behavior"].astype("category")
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    df["Day_of_Week"] = df["Day_of_Week"].astype("category")
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    # 1. Calculate recency
    recency = df[df['Behavior'] == 'Buy'].groupby(by='User_ID', as_index=False)['Date'].max()
    recency.columns = ['User_ID', 'LastPurshaceDate']
    current_date = df[df['Behavior'] == 'Buy']['Date'].max()
    recency['Recency'] = recency['LastPurshaceDate'].apply(lambda x: (current_date - x).days)

    # 2. Calculate Frequency
    frequency = df[df['Behavior'] == 'Buy'].groupby('User_ID')['Behavior'].count().reset_index()
    frequency.columns = ['User_ID', 'Frequency']

    # 3. Create RFM table
    rfm = recency.merge(frequency, on='User_ID')
    rfm.drop(columns=['LastPurshaceDate'], inplace=True)

    # 4. Assign R, F quartile values
    quantiles = rfm.quantile(q=[0.25, 0.5, 0.75])
    quantiles = quantiles.to_dict()
    def r_score(x):    
        if x <= quantiles['Recency'][0.25]:        
            return 4    
        elif x <= quantiles['Recency'][0.5]:        
            return 3    
        elif x <= quantiles['Recency'][0.75]:        
            return 2    
        else:        
            return 1
    
    def f_score(x):    
        if x <= quantiles['Frequency'][0.25]:        
            return 1    
        elif x <= quantiles['Frequency'][0.5]:        
            return 2    
        elif x <= quantiles['Frequency'][0.75]:        
            return 3    
        else:        
            return 4

    rfm['R_score'] = rfm['Recency'].apply(r_score)
    rfm['F_score'] = rfm['Frequency'].apply(f_score)

    rfm_val = rfm[['Recency', 'Frequency']]
    scaler = StandardScaler()
    rfm_scaled = scaler.fit(rfm_val)
    rfm_scaled = scaler.fit_transform(rfm_val)

    kmeans_scaled = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans_scaled.fit(rfm_scaled)
    identified_clusters = kmeans_scaled.fit_predict(rfm_val)
    rfm['Cluster'] = kmeans_scaled.fit_predict(rfm_scaled)
    
    def categorize_customers(row):    
        if row['Cluster'] == 0:        
            return 'Churn Risk Customers'    
        elif row['Cluster'] == 1:        
            return 'Potential Customers'    
        elif row['Cluster'] == 2:        
            return 'High-Value Customers'

    rfm['class'] = rfm.apply(categorize_customers, axis=1)

    customer_class = rfm.groupby('class')['User_ID'].count().reset_index()
    customer_class.columns = ['Customer Class', 'Counts']

    plt.figure(figsize=(4, 4))
    plt.pie(customer_class['Counts'], labels=customer_class['Customer Class'], autopct='%1.1f%%', explode=[0.05]*3)
    plt.title('RFM Analysis', fontsize=15)
    plt.savefig(f"{output_dir}/RFM Analysis.png", format='png', bbox_inches='tight')


def run_kmeans_realtime(df, output_dir="data/kmeans", n_clusters=3):

    # Chuyển đổi kiểu dữ liệu
    df["Behavior"] = df["Behavior"].astype("category")
    
    # Đảm bảo Datetime và Date được xử lý đúng
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors='coerce')
    df["Day_of_Week"] = df["Day_of_Week"].astype("category")
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce').dt.date

    # 1. Tính Recency
    buy_df = df[df['Behavior'] == 'Buy'].copy()  # Sửa 'Buy' thành 'buy' để khớp với data_generator.py
    recency = buy_df.groupby('User_ID')['Date'].max().reset_index()
    recency.columns = ['User_ID', 'LastPurchaseDate']
    
    # Lấy ngày hiện tại
    current_date = buy_df['Date'].max()
    current_date = pd.to_datetime(current_date).date()

    # Tính Recency
    recency['Recency'] = recency['LastPurchaseDate'].apply(lambda x: (current_date - x).days if pd.notna(x) else None)
    
    # Loại bỏ các giá trị NaN
    recency = recency.dropna(subset=['Recency'])

    # Đảm bảo Recency là kiểu int
    recency['Recency'] = recency['Recency'].astype(int)

    # 2. Tính Frequency
    frequency = buy_df.groupby('User_ID')['Behavior'].count().reset_index()
    frequency.columns = ['User_ID', 'Frequency']

    # 3. Tạo bảng RFM
    rfm = recency.merge(frequency, on='User_ID')

    # 4. Gán điểm R, F
    quantiles = rfm[['Recency', 'Frequency']].quantile(q=[0.25, 0.5, 0.75])
    quantiles = quantiles.to_dict()

    def r_score(x):
        if x <= quantiles['Recency'][0.25]:
            return 4
        elif x <= quantiles['Recency'][0.5]:
            return 3
        elif x <= quantiles['Recency'][0.75]:
            return 2
        else:
            return 1

    def f_score(x):
        if x <= quantiles['Frequency'][0.25]:
            return 1
        elif x <= quantiles['Frequency'][0.5]:
            return 2
        elif x <= quantiles['Frequency'][0.75]:
            return 3
        else:
            return 4

    rfm['R_score'] = rfm['Recency'].apply(r_score)
    rfm['F_score'] = rfm['Frequency'].apply(f_score)

    # 5. Chuẩn hóa dữ liệu và chạy KMeans
    rfm_val = rfm[['Recency', 'Frequency']]
    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(rfm_val)  # Chỉ gọi fit_transform một lần

    kmeans_scaled = KMeans(n_clusters=n_clusters, random_state=42)
    rfm['Cluster'] = kmeans_scaled.fit_predict(rfm_scaled)  # Chỉ gọi fit_predict một lần

    # 6. Phân loại khách hàng
    def categorize_customers(row):
        if row['Cluster'] == 0:
            return 'Churn Risk Customers'
        elif row['Cluster'] == 1:
            return 'Potential Customers'
        elif row['Cluster'] == 2:
            return 'High-Value Customers'

    rfm['class'] = rfm.apply(categorize_customers, axis=1)

    # 7. Tạo biểu đồ pie
    customer_class = rfm.groupby('class')['User_ID'].count().reset_index()
    customer_class.columns = ['Customer Class', 'Counts']

    plt.figure(figsize=(4, 4))
    plt.pie(customer_class['Counts'], labels=customer_class['Customer Class'], autopct='%1.1f%%', explode=[0.05]*len(customer_class))
    plt.title('RFM Analysis', fontsize=15)

    # Đảm bảo thư mục output_dir tồn tại
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f"{output_dir}/RFM_Analysis.png", format='png', bbox_inches='tight')
    plt.close()
