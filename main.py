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

def get_id_from_url(url, type_):
    slug = urlparse(url).path.rstrip('/').split('/')[-1]
    if type_ == "post":
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/posts"
    elif type_ == "page":
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/pages"
    elif type_ == "category":
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/categories"
    else:
        return None
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
    if type_ in ["post", "page"]:
        old_schema = get_current_schema(item_id, type_)
        script_schema = script_schema.strip()
        if old_schema and script_schema in old_schema:
            new_schema = old_schema
        elif old_schema and script_schema:
            new_schema = (old_schema.rstrip() + "\n" + script_schema)
        else:
            new_schema = script_schema

        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/{type_}s/{item_id}"
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
        # Lấy lại toàn bộ thông tin category hiện tại!
        api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/categories/{item_id}"
        get_resp = requests.get(api_endpoint, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
        if get_resp.status_code != 200:
            return False, f"Lỗi khi GET thông tin category: {get_resp.text}"
        data = get_resp.json()
        # Lấy lại toàn bộ field mặc định (không lấy _links)
        safe_fields = {}
        for field in ["name", "slug", "description", "parent", "meta"]:
            safe_fields[field] = data.get(field)
        # Cập nhật schema mới
        safe_fields["meta"] = safe_fields.get("meta", {}) or {}
        safe_fields["meta"]["category_schema"] = script_schema.strip()
        # PATCH với toàn bộ data
        patch_resp = requests.patch(api_endpoint, json=safe_fields, auth=HTTPBasicAuth(WP_USER, WP_APP_PASS))
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
            msg = f"Đã hủy theo yêu cầu của bạn! Đã dừng ở dòng {idx+1}."
            if send_log: send_log(msg)
            break

        url = row['url']
        type_ = row['type'].strip().lower()
        schema = "" if delete_mode else row['script_schema']
        item_id = get_id_from_url(url, type_)

        if not item_id:
            msg = f"[{idx+1}] ❌ Không tìm thấy ID cho URL: {url} (loại: {type_})"
            if send_log: send_log(msg)
            results.append({"stt": idx+1, "url": url, "type": type_, "result": "Không tìm thấy ID"})
            continue
        ok, detail = update_schema(item_id, schema, type_)
        if ok:
            action = "Xoá" if delete_mode else "Cập nhật"
            msg = f"[{idx+1}] ✅ {action} schema cho {type_} ID {item_id} thành công"
            result = "Thành công"
        else:
            msg = f"[{idx+1}] ❌ Lỗi khi {('xoá' if delete_mode else 'cập nhật')} schema cho {type_} ID {item_id}"
            result = f"Lỗi: {detail}"
            if send_log: send_log(f"[{idx+1}] ⚠️ Chi tiết lỗi: {detail}")
        if send_log: send_log(msg)
        results.append({"stt": idx+1, "url": url, "type": type_, "result": result})

    return pd.DataFrame(results)

# ----- Bot Telegram -----

async def chencode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_task and not user_task[user_id].done():
        await update.message.reply_text("Bạn đang có tiến trình chưa hoàn thành! Gõ /cancel để hủy hoặc đợi hoàn tất.")
        return

    await update.message.reply_text(
        "Gửi file Excel (.xlsx) gồm 3 cột: url, script_schema, type (post/page/category). "
        "Gõ /cancel để dừng lại nếu muốn."
    )
    user_cancel[user_id] = False

    try:
        for _ in range(30):
            await asyncio.sleep(1)
            if user_id in context.chat_data and 'pending_file' in context.chat_data[user_id]:
                break
            if user_cancel.get(user_id, False):
                await update.message.reply_text("Đã hủy tiến trình theo yêu cầu của bạn.")
                return
        else:
            await update.message.reply_text("Bạn không gửi file đúng thời gian, lệnh đã bị hủy.")
            return

        document = context.chat_data[user_id].pop('pending_file')
        file = await context.bot.get_file(document.file_id)
        filename = f"/tmp/{datetime.now().strftime('%Y%m%d%H%M%S')}_{document.file_name}"
        await file.download_to_drive(filename)
        await update.message.reply_text("File đã nhận. Đang xử lý, bạn chờ chút...")

        task = asyncio.create_task(handle_process_excel(update, context, filename, user_id))
        user_task[user_id] = task
        await task

    except Exception as e:
        await update.message.reply_text(f"Lỗi khi nhận file: {e}")

async def xoascript(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_task and not user_task[user_id].done():
        await update.message.reply_text("Bạn đang có tiến trình chưa hoàn thành! Gõ /cancel để hủy hoặc đợi hoàn tất.")
        return

    await update.message.reply_text(
        "Gửi file Excel (.xlsx) gồm 2 cột: url, type (post/page/category) để xoá schema. "
        "Gõ /cancel để dừng lại nếu muốn."
    )
    user_cancel[user_id] = False

    try:
        for _ in range(30):
            await asyncio.sleep(1)
            if user_id in context.chat_data and 'pending_file' in context.chat_data[user_id]:
                break
            if user_cancel.get(user_id, False):
                await update.message.reply_text("Đã hủy tiến trình theo yêu cầu của bạn.")
                return
        else:
            await update.message.reply_text("Bạn không gửi file đúng thời gian, lệnh đã bị hủy.")
            return

        document = context.chat_data[user_id].pop('pending_file')
        file = await context.bot.get_file(document.file_id)
        filename = f"/tmp/{datetime.now().strftime('%Y%m%d%H%M%S')}_{document.file_name}"
        await file.download_to_drive(filename)
        await update.message.reply_text("File đã nhận. Đang xử lý xóa script, bạn chờ chút...")

        task = asyncio.create_task(handle_process_excel(update, context, filename, user_id, delete_mode=True))
        user_task[user_id] = task
        await task

    except Exception as e:
        await update.message.reply_text(f"Lỗi khi nhận file: {e}")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_cancel.get(user_id, False):
        return
    if user_id not in context.chat_data:
        context.chat_data[user_id] = {}
    context.chat_data[user_id]['pending_file'] = update.message.document

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
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Hoàn tất! File kết quả đã gửi.")
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Lỗi khi xử lý: {e}")
    finally:
        user_task.pop(user_id, None)
        user_cancel[user_id] = False

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_task and not user_task[user_id].done():
        user_cancel[user_id] = True
        await update.message.reply_text("Đã gửi yêu cầu hủy tiến trình của bạn. Đang dừng...")
    else:
        await update.message.reply_text("Bạn không có tiến trình nào đang chạy!")

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("chencode", chencode))
    app.add_handler(CommandHandler("xoascript", xoascript))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    print("Bot đã sẵn sàng!")
    app.run_polling()

if __name__ == "__main__":
    main()
