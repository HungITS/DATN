import streamlit as st
import time
import os
from PIL import Image

st.title("Real-Time User Behavior Analysis")

# Hàm đọc ảnh mới nhất từ một thư mục
def load_image(path, pattern=None):
    if not os.path.exists(path):
        return None
    files = [f for f in os.listdir(path) if f.endswith((".png", ".jpg"))]
    if pattern:
        files = [f for f in files if pattern in f]
    if files:
        latest_file = max([os.path.join(path, f) for f in files], key=os.path.getmtime)
        return Image.open(latest_file)
    return None

# Vòng lặp cập nhật dữ liệu
placeholder = st.empty()
while True:
    with placeholder.container():
        # Hiển thị EDA
        st.subheader("EDA Results")

        # Nhóm 1: Phân tích hành vi
        st.markdown("### Phân tích hành vi người dùng")
        eda_files_behavior = {
            "behavior_pie.png": "Phân phối hành vi (Pie Chart)",
            "behavior_bar.png": "Số lượng hành vi (Bar Chart)",
            "behavior_hour.png": "Hành vi theo giờ",
            "behavior_day.png": "Hành vi theo ngày"
        }
        for file_name, caption in eda_files_behavior.items():
            file_path = f"data/eda/{file_name}"
            if os.path.exists(file_path):
                st.image(file_path, caption=caption, use_container_width=True)
            else:
                st.warning(f"Không tìm thấy biểu đồ: {caption}")

        # Nhóm 2: Phân tích sản phẩm và danh mục
        st.markdown("### Phân tích sản phẩm và danh mục")
        eda_files_product = {
            "top_products_buy.png": "Top sản phẩm được mua",
            "top_products_cart.png": "Top sản phẩm thêm vào giỏ",
            "addtocart_heatmap.png": "Mật độ AddToCart (Heatmap)"
        }
        for file_name, caption in eda_files_product.items():
            file_path = f"data/eda/{file_name}"
            if os.path.exists(file_path):
                st.image(file_path, caption=caption, use_container_width=True)
            else:
                st.warning(f"Không tìm thấy biểu đồ: {caption}")

        # Nhóm 3: Xu hướng
        st.markdown("### Xu hướng")
        eda_files_trend = {
            "daily_trend.png": "Xu hướng theo ngày"
        }
        for file_name, caption in eda_files_trend.items():
            file_path = f"data/eda/{file_name}"
            if os.path.exists(file_path):
                st.image(file_path, caption=caption, use_container_width=True)
            else:
                st.warning(f"Không tìm thấy biểu đồ: {caption}")

        # Hiển thị biểu đồ phân khúc từ run_kmeans
        st.markdown("### Phân khúc khách hàng")
        kmeans_image = load_image("data/kmeans/")
        if kmeans_image:
            st.image(kmeans_image, caption="Tỷ lệ phân khúc khách hàng (Pie Chart)", use_container_width=True)
        else:
            st.warning("Không tìm thấy biểu đồ phân khúc khách hàng.")

    time.sleep(5)  # Cập nhật mỗi 5 giây