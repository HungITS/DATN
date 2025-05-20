import os
import logging

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:  # tránh gắn nhiều handler lặp
        # Tạo thư mục logs nếu chưa có
        os.makedirs("logs", exist_ok=True)

        # File log riêng theo tên logger
        file_handler = logging.FileHandler(f"logs/{name}.log", mode='a')
        file_handler.setLevel(logging.DEBUG)

        # Console handler (hiện trên terminal)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Gắn vào logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False  # không đẩy log lên logger cha

    return logger
