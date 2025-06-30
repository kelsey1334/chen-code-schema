import os
import pandas as pd
import requests
import asyncio
from urllib.parse import urlparse
from dotenv import load_dotenv
from telegram import Update, Document
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from telegram.constants import ChatAction
from datetime import datetime

from requests.auth import HTTPBasicAuth

# Load biến môi trường
load_dotenv()
WP_API_URL = os.getenv("WP_API_URL")
WP_USER = os.getenv("WP_USER")
WP_APP_PASS = os.getenv("WP_APP_PASS")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

user_task = {}
user_cancel = {}

def is_homepage_url(url):
    parsed = urlparse(url)
    # Chuẩn hóa: domain.com, domain.com/ hoặc domain.com?...
    path = parsed.path.rstrip('/')
    if not path and (not parsed.query and not parsed.fragment):
        return True
    # Ngoài ra, nếu path chỉ là '/' hoặc rỗng, đều coi là homepage
    return path == ''

def get_homepage_id():
    api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/settings"
    resp = requests.get(api_endpoint, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
    if resp.status_code == 200:
        page_id = resp.json().get('page_on_front', 0)
        try:
            page_id = int(page_id)
        except Exception:
            page_id = 0
        if page_id > 0:
            return page_id
    return None

def get_id_from_url(url, type_):
    if type_ in ["post", "page"]:
        # Nếu là url trang chủ, lấy ID page_on_front
        if is_homepage_url(url):
            homepage_id = get_homepage_id()
            if homepage_id:
                return homepage_id
            # Nếu không có homepage, return None
        slug = urlparse(url).path.rstrip('/').split('/')[-1]
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/{type_}s"
        params = {"per_page": 1, "slug": slug}
        resp = requests.get(api_endpoint, params=params, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]['id']
    elif type_ == "category":
        slug = urlparse(url).path.rstrip('/').split('/')[-1]
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/categories"
        params = {"per_page": 1, "slug": slug}
        resp = requests.get(api_endpoint, params=params, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]['id']
    return None

def get_current_schema(post_id, type_):
    if type_ in ["post", "page"]:
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/{type_}s/{post_id}"
        resp = requests.get(api_endpoint, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if resp.status_code == 200:
            meta = resp.json().get('meta', {})
            inpost = meta.get('_inpost_head_script', {})
            if isinstance(inpost, dict):
                return inpost.get('synth_header_script', '') or ''
    elif type_ == "category":
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/categories/{post_id}"
        resp = requests.get(api_endpoint, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if resp.status_code == 200:
            meta = resp.json().get('meta', {})
            return meta.get('category_schema', '') or ''
    return ''

def update_schema(item_id, script_schema, type_):
    script_schema = script_schema.strip() if script_schema else ""
    if type_ in ["post", "page"]:
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/{type_}s/{item_id}"

        if script_schema == "":
            # Xóa: set meta về rỗng
            payload = {
                "meta": {
                    "_inpost_head_script": {
                        "synth_header_script": ""
                    }
                }
            }
        else:
            old_schema = get_current_schema(item_id, type_)
            # Nếu nội dung cũ khác nội dung mới thì cộng thêm, còn trùng thì giữ nguyên
            if old_schema and script_schema in old_schema:
                new_schema = old_schema
            elif old_schema:
                new_schema = (old_schema.rstrip() + "\n" + script_schema)
            else:
                new_schema = script_schema

            payload = {
                "meta": {
                    "_inpost_head_script": {
                        "synth_header_script": new_schema
                    }
                }
            }

        resp = requests.patch(api_endpoint, json=payload, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if resp.status_code == 200:
            return True, None
        else:
            try:
                error_detail = resp.json()
            except Exception:
                error_detail = resp.text
            return False, error_detail

    elif type_ == "category":
        # PATCH CHỈ FIELD meta.category_schema, không PATCH toàn bộ object
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/categories/{item_id}"
        payload = {
            "meta": {
                "category_schema": script_schema
            }
        }
        patch_resp = requests.patch(api_endpoint, json=payload, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if patch_resp.status_code == 200:
            return True, None
        else:
            try:
                error_detail = patch_resp.json()
            except Exception:
                error_detail = patch_resp.text
            return False, error_detail
    else:
        return False, f"Loại '{type_}' không hỗ trợ"

def process_excel(file_path, send_log=None, cancel_flag=None, delete_mode=False):
    df = pd.read_excel(file_path)
    require_cols = {'url', 'type'} if delete_mode else {'url', 'script_schema', 'type'}
    if not require_cols.issubset(df.columns):
        raise Exception(
            "File Excel phải có cột: 'url', 'type'" +
            ("" if delete_mode else ", 'script_schema'")
        )

    results = []
    for idx, row in df.iterrows():
        if cancel_flag and cancel_flag():
            msg = f"🛑 Đã hủy theo yêu cầu của bạn! Đã dừng ở dòng {idx+1}."
            if send_log: send_log(msg)
            break

        url = row['url']
        type_ = row['type'].strip().lower()
        schema = "" if delete_mode else row['script_schema']
        item_id = get_id_from_url(url, type_)

        if not item_id:
            msg = f"🚫❌ [{idx+1}] Không tìm thấy ID cho URL: {url} (loại: {type_})"
            if send_log: send_log(msg)
            results.append({"stt": idx+1, "url": url, "type": type_, "result": "Không tìm thấy ID"})
            continue
        ok, detail = update_schema(item_id, schema, type_)
        if ok:
            action = "Xoá" if delete_mode else "Cập nhật"
            msg = f"✨✅ [{idx+1}] {action} schema cho {type_} ID {item_id} thành công"
            result = "Thành công"
        else:
            msg = f"🚫❌ [{idx+1}] Lỗi khi {('xoá' if delete_mode else 'cập nhật')} schema cho {type_} ID {item_id}"
            result = f"Lỗi: {detail}"
            if send_log: send_log(f"💥⚠️ [{idx+1}] Chi tiết lỗi: {detail}")
        if send_log: send_log(msg)
        results.append({"stt": idx+1, "url": url, "type": type_, "result": result})

    return pd.DataFrame(results)

# ----- Bot Telegram -----

async def chencode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_task and not user_task[user_id].done():
        await update.message.reply_text("🛑 Bạn đang có tiến trình chưa hoàn thành! Gõ /cancel để hủy hoặc đợi hoàn tất.")
        return
    context.chat_data[user_id] = {'waiting_for_file': 'chencode'}
    await update.message.reply_text(
        "📤 Gửi file Excel (.xlsx) gồm 3 cột: url, script_schema, type (post/page/category). Gõ /cancel để dừng lại nếu muốn."
    )

async def xoascript(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_task and not user_task[user_id].done():
        await update.message.reply_text("🛑 Bạn đang có tiến trình chưa hoàn thành! Gõ /cancel để hủy hoặc đợi hoàn tất.")
        return
    context.chat_data[user_id] = {'waiting_for_file': 'xoascript'}
    await update.message.reply_text(
        "📤 Gửi file Excel (.xlsx) gồm 2 cột: url, type (post/page/category) để xoá schema. Gõ /cancel để dừng lại nếu muốn."
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_state = context.chat_data.get(user_id, {})
    waiting = user_state.get('waiting_for_file')
    if not waiting:
        await update.message.reply_text("⏰ Bạn phải dùng lệnh /chencode hoặc /xoascript trước khi gửi file.")
        return

    document = update.message.document
    file = await context.bot.get_file(document.file_id)
    filename = f"/tmp/{datetime.now().strftime('%Y%m%d%H%M%S')}_{document.file_name}"
    await file.download_to_drive(filename)
    await update.message.reply_text("📥 File đã nhận. Đang xử lý, bạn chờ chút... ⏳")

    # Clear flag
    context.chat_data[user_id]['waiting_for_file'] = None

    # Tạo và chạy task tương ứng
    task = None
    if waiting == 'chencode':
        task = asyncio.create_task(handle_process_excel(update, context, filename, user_id))
    elif waiting == 'xoascript':
        task = asyncio.create_task(handle_process_excel(update, context, filename, user_id, delete_mode=True))
    user_task[user_id] = task
    await task

async def handle_process_excel(update, context, file_path, user_id, delete_mode=False):
    log_messages = []
    async def send_log(msg):
        await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
        log_messages.append(msg)
    def cancel_flag():
        return user_cancel.get(user_id, False)
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    loop = asyncio.get_running_loop()
    try:
        df_result = await loop.run_in_executor(
            None,
            lambda: process_excel(
                file_path,
                send_log=lambda m: asyncio.run_coroutine_threadsafe(send_log(m), loop),
                cancel_flag=cancel_flag,
                delete_mode=delete_mode
            )
        )
        out_file = f"/tmp/result_{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        df_result.to_excel(out_file, index=False)
        await context.bot.send_document(chat_id=update.effective_chat.id, document=open(out_file, 'rb'), filename="result.xlsx")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="🥳 Hoàn tất! File kết quả đã gửi.")
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"🚫❌ Lỗi khi xử lý: {e}")
    finally:
        user_task.pop(user_id, None)
        user_cancel[user_id] = False

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.chat_data[user_id] = {}
    if user_id in user_task and not user_task[user_id].done():
        user_cancel[user_id] = True
        await update.message.reply_text("🛑 Đã gửi yêu cầu hủy tiến trình của bạn. Đang dừng...")
    else:
        await update.message.reply_text("⏹️ Bạn không có tiến trình nào đang chạy hoặc chưa gửi file!")

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("chencode", chencode))
    app.add_handler(CommandHandler("xoascript", xoascript))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    print("Bot đã sẵn sàng! 🚀")
    app.run_polling()

if __name__ == "__main__":
    main()
