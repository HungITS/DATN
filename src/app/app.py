import streamlit as st
import os
import subprocess

st.title("User Behavior Analysis Demo")

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
                    ["python3", "-m", "pipelines.pipeline"],
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
                    "category_behavior.png": "Hành vi theo danh mục",
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
                kmeans_plot = "data/kmeans/RFM Analysis.png"

                if os.path.exists(kmeans_plot):
                    st.image(kmeans_plot, caption="Phân khúc khách hàng (RFM Analysis)")
                else:
                    st.warning("Không tìm thấy biểu đồ RFM Analysis.")

            except subprocess.CalledProcessError as e:
                st.error(f"Lỗi pipeline: {e.stderr}")
            except Exception as e:
                st.error(f"Lỗi xảy ra: {str(e)}")

    else:
        st.error("Vui lòng upload file CSV.")