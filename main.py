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

load_dotenv()
WP_API_URL = os.getenv("WP_API_URL")
WP_JWT_TOKEN = os.getenv("WP_JWT_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

user_task = {}   # user_id -> asyncio.Task
user_cancel = {} # user_id -> bool

# ===== Các hàm xử lý WordPress =====

def get_post_id_from_url(url):
    slug = urlparse(url).path.rstrip('/').split('/')[-1]
    api_endpoint = f"{WP_API_URL}/wp-json/wp/v2/posts"
    params = {"per_page": 1, "slug": slug}
    resp = requests.get(api_endpoint, params=params)
    if resp.status_code == 200 and resp.json():
        return resp.json()[0]['id']
    return None

def get_current_schema(post_id):
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
    old_schema = get_current_schema(post_id)
    script_schema = script_schema.strip()
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

def process_excel(file_path, send_log=None, cancel_flag=None):
    df = pd.read_excel(file_path)
    if not {'url', 'script_schema'}.issubset(df.columns):
        raise Exception("File Excel phải có 2 cột: 'url' và 'script_schema'")

    results = []
    for idx, row in df.iterrows():
        if cancel_flag and cancel_flag():
            msg = f"Đã hủy theo yêu cầu của bạn! Đã dừng ở dòng {idx+1}."
            if send_log: send_log(msg)
            break

        url = row['url']
        schema = row['script_schema']
        post_id = get_post_id_from_url(url)
        if not post_id:
            msg = f"[{idx+1}] ❌ Không tìm thấy post_id cho URL: {url}"
            if send_log: send_log(msg)
            results.append({"stt": idx+1, "url": url, "result": "Không tìm thấy post_id"})
            continue
        ok = update_schema(post_id, schema)
        if ok:
            msg = f"[{idx+1}] ✅ Đã cập nhật schema cho bài viết ID {post_id}"
            result = "Thành công"
        else:
            msg = f"[{idx+1}] ❌ Lỗi khi cập nhật schema cho bài viết ID {post_id}"
            result = "Lỗi"
        if send_log: send_log(msg)
        results.append({"stt": idx+1, "url": url, "result": result})

    # Trả về DataFrame kết quả
    return pd.DataFrame(results)

# ====== Telegram Bot logic =======

async def chencode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Nếu user đang có tiến trình chưa xong, không cho chạy tiếp
    if user_id in user_task and not user_task[user_id].done():
        await update.message.reply_text("Bạn đang có tiến trình chưa hoàn thành! Gõ /cancel để hủy hoặc đợi hoàn tất.")
        return

    await update.message.reply_text("Gửi file Excel (.xlsx) trong vòng 30 giây để bắt đầu chèn schema. Gõ /cancel để dừng lại nếu muốn.")
    user_cancel[user_id] = False

    # Đợi user gửi file trong 30s, nếu không thì hủy
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

        # Nhận file
        document = context.chat_data[user_id].pop('pending_file')
        file = await context.bot.get_file(document.file_id)
        filename = f"/tmp/{datetime.now().strftime('%Y%m%d%H%M%S')}_{document.file_name}"
        await file.download_to_drive(filename)
        await update.message.reply_text("File đã nhận. Đang xử lý, bạn chờ chút...")

        # Tạo task xử lý
        task = asyncio.create_task(handle_process_excel(update, context, filename, user_id))
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

async def handle_process_excel(update, context, file_path, user_id):
    log_messages = []
    async def send_log(msg):
        await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)
        log_messages.append(msg)
    # Biến kiểm tra hủy
    def cancel_flag():
        return user_cancel.get(user_id, False)
    # Thông báo đang nhập liệu
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    loop = asyncio.get_running_loop()
    try:
        # Hàm blocking chạy trong executor
        df_result = await loop.run_in_executor(
            None,
            lambda: process_excel(file_path, send_log=lambda m: asyncio.run_coroutine_threadsafe(send_log(m), loop), cancel_flag=cancel_flag)
        )
        # Xuất file kết quả
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
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    print("Bot đã sẵn sàng!")
    app.run_polling()

if __name__ == "__main__":
    main()
