import streamlit as st
import pandas as pd
import os
import subprocess

st.title("Real-Time User Behavior Analysis")

# Tab để chọn giữa phân tích file và tra cứu User_ID
tab1, tab2 = st.tabs(["Phân tích dữ liệu", "Tra cứu phân khúc khách hàng"])

# Tab 1: Phân tích dữ liệu (upload file và hiển thị kết quả)
with tab1:
    uploaded_file = st.file_uploader("Upload CSV File", type="csv")

    if st.button("Run Analysis"):
        if uploaded_file:
            with st.spinner("Running pipeline..."):
                try:
                    # Lưu file upload
                    os.makedirs("data/raw", exist_ok=True)
                    input_path = f"data/raw/{uploaded_file.name}"
                    with open(input_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    # Cập nhật config
                    config_content = f"""
                    input:
                        local_path: {input_path}
                    output:
                        local_clean_path: data/processed
                    """
                    
                    os.makedirs("config", exist_ok=True)
                    with open("config/local_config.yaml", "w") as f:
                        f.write(config_content)

                    # Chạy pipeline
                    result = subprocess.run(
                        ["python3", "-m", "src.pipelines.pipeline"],
                        capture_output=True,
                        text=True,
                        check=True
                    )

                    st.success("Analysis completed!")

                    # Hiển thị kết quả EDA
                    st.subheader("EDA Results")
                    eda_files = {
                        "behavior_pie.png": "Phân phối hành vi",
                        "behavior_bar.png": "Số lượng hành vi",
                        "top_products_buy.png": "Top sản phẩm mua",
                        "top_products_cart.png": "Top sản phẩm thêm vào giỏ",
                        "behavior_hour.png": "Hành vi theo giờ",
                        "behavior_day.png": "Hành vi theo ngày",
                        "daily_trend.png": "Xu hướng theo ngày",
                        "addtocart_heatmap.png": "Mật độ AddToCart"
                    }

                    for file_name, caption in eda_files.items():
                        file_path = f"data/eda/{file_name}"
                        if os.path.exists(file_path):
                            st.image(file_path, caption=caption)
                        else:
                            st.warning(f"Không tìm thấy biểu đồ: {caption}")

                    # Hiển thị kết quả RFM và K-means
                    st.subheader("RFM và K-means")
                    kmeans_plot = "data/kmeans/RFM_Analysis.png"  # Đổi tên file cho khớp với run_kmeans_realtime

                    if os.path.exists(kmeans_plot):
                        st.image(kmeans_plot, caption="Phân khúc khách hàng (RFM Analysis)")
                    else:
                        st.warning("Không tìm thấy biểu đồ RFM Analysis.")

                    # Hiển thị biểu đồ Elbow (nếu có)
                    elbow_plot = "data/kmeans/Elbow_Plot.png"
                    if os.path.exists(elbow_plot):
                        st.image(elbow_plot, caption="Biểu đồ Elbow để chọn số cụm tối ưu")

                    # Trích xuất Silhouette Score và WCSS từ log
                    log_output = result.stdout
                    silhouette_line = [line for line in log_output.split("\n") if "Silhouette Score" in line]
                    wcss_line = [line for line in log_output.split("\n") if "WCSS" in line]

                    if silhouette_line:
                        st.write(silhouette_line[0])
                    if wcss_line:
                        st.write(wcss_line[0])

                except subprocess.CalledProcessError as e:
                    st.error(f"Lỗi pipeline: {e.stderr}")
                except Exception as e:
                    st.error(f"Lỗi xảy ra: {str(e)}")
        else:
            st.error("Vui lòng upload file CSV.")

# Tab 2: Tra cứu phân khúc khách hàng dựa trên User_ID
with tab2:
    st.subheader("Tra cứu phân khúc khách hàng")

    # Đọc file kết quả RFM
    rfm_file = "data/kmeans/rfm_results.csv"
    if os.path.exists(rfm_file):
        rfm_df = pd.read_csv(rfm_file, dtype={'User_ID': str})
        user_ids = rfm_df['User_ID'].tolist()
    else:
        st.error("File rfm_results.csv không tồn tại. Vui lòng chạy pipeline trước.")
        user_ids = []

    # Giao diện nhập User_ID với gợi ý tự động
    user_id = st.text_input("Nhập User_ID để xem phân khúc khách hàng", value="", key="user_id_input", help="Nhập ID của người dùng để xem phân khúc.")

    # Gợi ý tự động (autocompletion)
    if user_id:
        filtered_ids = [uid for uid in user_ids if uid.startswith(user_id)]
        if filtered_ids:
            st.write("Gợi ý User_ID:", filtered_ids)

    # Nút "Results" để hiển thị phân khúc
    if st.button("Results"):
        if user_id in user_ids:
            segment = rfm_df[rfm_df['User_ID'] == user_id]['class'].iloc[0]
            st.success(f"Phân khúc khách hàng của User_ID {user_id} là: **{segment}**")
        else:
            st.error("User_ID không tồn tại trong dữ liệu. Vui lòng kiểm tra lại hoặc chạy pipeline để cập nhật dữ liệu.")