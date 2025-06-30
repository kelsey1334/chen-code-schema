import os
import pandas as pd
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()
WP_API_URL = os.getenv("WP_API_URL")        # VD: https://yourdomain.com
WP_JWT_TOKEN = os.getenv("WP_JWT_TOKEN")    # JWT token WordPress

def get_post_id_from_url(url):
    """
    Lấy post ID từ URL qua slug bài viết.
    """
    slug = urlparse(url).path.rstrip('/').split('/')[-1]
    api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/posts"
    params = {"per_page": 1, "slug": slug}
    resp = requests.get(api_endpoint, params=params)
    if resp.status_code == 200 and resp.json():
        return resp.json()[0]['id']
    return None

def get_current_schema(post_id):
    """
    Lấy nội dung synth_header_script hiện tại từ REST API.
    """
    api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/posts/{post_id}"
    headers = {
        "Authorization": f"Bearer {WP_JWT_TOKEN}",
        "Content-Type": "application/json"
    }
    resp = requests.get(api_endpoint, headers=headers)
    if resp.status_code == 200:
        meta = resp.json().get('meta', {})
        inpost = meta.get('_inpost_head_script', {})
        if isinstance(inpost, dict):
            return inpost.get('synth_header_script', '') or ''
    return ''

def update_schema(post_id, script_schema):
    """
    Giữ nội dung cũ, nối thêm schema mới phía dưới nếu chưa có, rồi PATCH lại.
    """
    old_schema = get_current_schema(post_id)
    script_schema = script_schema.strip()
    # Tránh nối lặp lại nếu schema đã có trong nội dung cũ
    if old_schema and script_schema in old_schema:
        new_schema = old_schema
    elif old_schema:
        new_schema = (old_schema.rstrip() + "\n" + script_schema)
    else:
        new_schema = script_schema

    api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/posts/{post_id}"
    headers = {
        "Authorization": f"Bearer {WP_JWT_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "meta": {
            "_inpost_head_script": {
                "synth_header_script": new_schema
            }
        }
    }
    resp = requests.patch(api_endpoint, json=payload, headers=headers)
    return resp.status_code == 200

def process_excel(file_path):
    """
    Đọc file excel và cập nhật schema cho từng bài viết.
    """
    df = pd.read_excel(file_path)
    if not {'url', 'script_schema'}.issubset(df.columns):
        raise Exception("File Excel phải có 2 cột: 'url' và 'script_schema'")

    for idx, row in df.iterrows():
        url = row['url']
        schema = row['script_schema']
        post_id = get_post_id_from_url(url)
        if not post_id:
            print(f"[{idx+1}] Không tìm thấy post_id cho URL: {url}")
            continue
        ok = update_schema(post_id, schema)
        if ok:
            print(f"[{idx+1}] Đã cập nhật schema cho bài viết ID {post_id}")
        else:
            print(f"[{idx+1}] Lỗi khi cập nhật schema cho bài viết ID {post_id}")

if __name__ == "__main__":
    # Đặt tên file Excel cần nhập
    process_excel("input.xlsx")
