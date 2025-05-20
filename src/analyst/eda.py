import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import glob


def run_eda(input_path, output_dir="data/eda"):
    os.makedirs(output_dir, exist_ok=True)

    # Đọc dữ liệu
    csv_files = glob.glob(f"{input_path}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"Không tìm thấy file CSV nào trong {input_path}")
    csv_file = csv_files[0]
    df = pd.read_csv(csv_file)

    # Tính số lượng hành vi
    behavior_counts = df['Behavior'].value_counts()

    # Biểu đồ tròn
    plt.figure(figsize=(6, 6))
    plt.pie(behavior_counts, labels=behavior_counts.index, autopct='%1.1f%%')
    plt.title('Phân phối hành vi người dùng')
    plt.savefig(f"{output_dir}/behavior_pie.png")
    plt.show()

    # Biểu đồ cột
    plt.figure(figsize=(8, 6))
    sns.countplot(x='Behavior', data=df, order=['PageView', 'Favorite', 'AddToCart', 'Buy'])
    plt.title('Số lượng các hành vi')
    plt.savefig(f"{output_dir}/behavior_bar.png")
    plt.show()

    # Top sản phẩm được mua
    top_products_buy = df[df['Behavior'] == 'Buy']['Product_ID'].value_counts().head(10)
    if not top_products_buy.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(x=top_products_buy.values, y=top_products_buy.index.astype(str))
        plt.title('Top 10 sản phẩm được mua nhiều nhất')
        plt.savefig(f"{output_dir}/top_products_buy.png")
        plt.show()

    # Top sản phẩm được thêm vào giỏ
    top_products_cart = df[df['Behavior'] == 'AddToCart']['Product_ID'].value_counts().head(10)
    if not top_products_cart.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(x=top_products_cart.values, y=top_products_cart.index.astype(str))
        plt.title('Top 10 sản phẩm được thêm vào giỏ nhiều nhất')
        plt.savefig(f"{output_dir}/top_products_cart.png")
        plt.show()


    # Theo giờ
    plt.figure(figsize=(12, 6))
    sns.countplot(x='Hour', hue='Behavior', data=df)
    plt.title('Hành vi người dùng theo giờ trong ngày')
    plt.savefig(f"{output_dir}/behavior_hour.png")
    plt.show()

    # Theo ngày trong tuần
    plt.figure(figsize=(12, 6))
    sns.countplot(x='Day_of_Week', hue='Behavior', data=df, order=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    plt.title('Hành vi người dùng theo ngày trong tuần')
    plt.savefig(f"{output_dir}/behavior_day.png")
    plt.show()

    # Xu hướng theo ngày
    daily_behavior = df.groupby(['Date', 'Behavior']).size().unstack().fillna(0)
    daily_behavior.plot(kind='line', figsize=(12, 6))
    plt.title('Xu hướng hành vi theo ngày')
    plt.savefig(f"{output_dir}/daily_trend.png")
    plt.show()

    # Heatmap cho AddToCart
    heatmap_data = df[df['Behavior'] == 'AddToCart'].pivot_table(index='Day_of_Week', columns='Hour', values='Behavior', aggfunc='count')
    if not heatmap_data.empty:
        plt.figure(figsize=(12, 8))
        sns.heatmap(heatmap_data, cmap='YlGnBu')
        plt.title('Mật độ hành vi AddToCart theo giờ và ngày trong tuần')
        plt.savefig(f"{output_dir}/addtocart_heatmap.png")
        plt.show()

def run_eda_realtime(df, output_dir="data/eda"):

    # Tính số lượng hành vi
    behavior_counts = df['Behavior'].value_counts()

    # Biểu đồ tròn
    plt.figure(figsize=(6, 6))
    plt.pie(behavior_counts, labels=behavior_counts.index, autopct='%1.1f%%')
    plt.title('Phân phối hành vi người dùng')
    plt.savefig(f"{output_dir}/behavior_pie.png")

    # Biểu đồ cột
    plt.figure(figsize=(8, 6))
    sns.countplot(x='Behavior', data=df, order=['PageView', 'Favorite', 'AddToCart', 'Buy'])
    plt.title('Số lượng các hành vi')
    plt.savefig(f"{output_dir}/behavior_bar.png")

    # Top sản phẩm được mua
    top_products_buy = df[df['Behavior'] == 'Buy']['Product_ID'].value_counts().head(10)
    if not top_products_buy.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(x=top_products_buy.values, y=top_products_buy.index.astype(str))
        plt.title('Top 10 sản phẩm được mua nhiều nhất')
        plt.savefig(f"{output_dir}/top_products_buy.png")

    # Top sản phẩm được thêm vào giỏ
    top_products_cart = df[df['Behavior'] == 'AddToCart']['Product_ID'].value_counts().head(10)
    if not top_products_cart.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(x=top_products_cart.values, y=top_products_cart.index.astype(str))
        plt.title('Top 10 sản phẩm được thêm vào giỏ nhiều nhất')
        plt.savefig(f"{output_dir}/top_products_cart.png")


    # Theo giờ
    plt.figure(figsize=(12, 6))
    sns.countplot(x='Hour', hue='Behavior', data=df)
    plt.title('Hành vi người dùng theo giờ trong ngày')
    plt.savefig(f"{output_dir}/behavior_hour.png")

    # Theo ngày trong tuần
    plt.figure(figsize=(12, 6))
    sns.countplot(x='Day_of_Week', hue='Behavior', data=df, order=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    plt.title('Hành vi người dùng theo ngày trong tuần')
    plt.savefig(f"{output_dir}/behavior_day.png")

    # Xu hướng theo ngày
    daily_behavior = df.groupby(['Date', 'Behavior']).size().unstack().fillna(0)
    daily_behavior.plot(kind='line', figsize=(12, 6))
    plt.title('Xu hướng hành vi theo ngày')
    plt.savefig(f"{output_dir}/daily_trend.png")

    # Heatmap cho AddToCart
    heatmap_data = df[df['Behavior'] == 'AddToCart'].pivot_table(index='Day_of_Week', columns='Hour', values='Behavior', aggfunc='count')
    if not heatmap_data.empty:
        plt.figure(figsize=(12, 8))
        sns.heatmap(heatmap_data, cmap='YlGnBu')
        plt.title('Mật độ hành vi AddToCart theo giờ và ngày trong tuần')
        plt.savefig(f"{output_dir}/addtocart_heatmap.png")