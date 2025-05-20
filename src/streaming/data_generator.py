import pandas as pd
import time
import random
from datetime import datetime
import os

def generate_user_behavior():
    return {
        'User_ID': str(random.randint(100000, 999999)),
        'Product_ID': str(random.randint(1000000, 9999999)),
        'Category_ID': str(random.randint(1000000, 9999999)),
        'Behavior': random.choice(['pv', 'buy', 'cart', 'fav']),
        'Timestamp': int(time.time()),  # Epoch seconds
    }

def main():
    # Tạo header nếu file chưa tồn tại
    # Đường dẫn file (dùng đường dẫn tương đối)
    file_path = "data/stream_input/UserBehavior_realtime.csv"
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Tạo header nếu file chưa tồn tại
    if not os.path.exists(file_path):
        df = pd.DataFrame(columns=['User_ID', 'Product_ID', 'Category_ID', 'Behavior', 'Timestamp'])
        df.to_csv(file_path, index=False)

    # Chạy liên tục, thêm dữ liệu mỗi 1 giây
    print("Generating user behavior data...")
    while True:
        data = generate_user_behavior()
        df = pd.DataFrame([data])
        df.to_csv(file_path, mode='a', header=False, index=False)
        time.sleep(1)  # Thêm dữ liệu mỗi 1 giây

if __name__ == "__main__":
    main()