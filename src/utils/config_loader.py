import yaml
import os

def load_config(path):
  project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))  # Lùi một cấp từ thư mục hiện tại
  full_path = os.path.join(project_root, path)
  with open(path, 'r', encoding='utf-8') as f: 
    return yaml.safe_load(f)
