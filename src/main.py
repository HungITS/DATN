import subprocess
import time
import os
import signal
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
def run_pipeline(duration=180):  # Duration là 120 giây (2 phút)
    
    generator_script = os.path.join("src", "streaming", "data_generator.py")
    streaming_script = os.path.join("src", "streaming", "spark_streaming.py")
    streamlit_script = os.path.join("src", "app", "app_real_time.py")

    print("Starting data generator...")
    generator_process = subprocess.Popen(["python", generator_script])
    time.sleep(2)

    print("Starting Spark Streaming...")
    streaming_process = subprocess.Popen(["python", streaming_script])

    print("Starting Streamlit app...")
    streamlit_process = subprocess.Popen(["streamlit", "run", streamlit_script])

    print(f"Pipeline will run for {duration} seconds...")
    time.sleep(duration)

    print("Stopping pipeline...")
    generator_process.terminate()
    streaming_process.terminate()
    streamlit_process.terminate()

    generator_process.wait(timeout=5)
    streaming_process.wait(timeout=5)
    streamlit_process.wait(timeout=5)

if __name__ == "__main__":
    run_pipeline(duration=180)