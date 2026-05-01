import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pyrogram import Client
import motor.motor_asyncio
from aiohttp import web

# --- IMPORT ENGINE ---
try:
    from pahe_engine import PaheEngine
except ImportError:
    print("❌ CRITICAL: 'pahe_engine.py' not found!")
    sys.exit(1)

# --- CONFIGURATION ---
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
TARGET_GROUP = int(os.getenv("TARGET_GROUP", "0"))
MONGO_URL = os.getenv("MONGO_URL", "")
PORT = int(os.getenv("PORT", 8080))

def get_env_list(var_name):
    val = os.getenv(var_name)
    if val:
        return [int(x.strip()) for x in val.split(',') if x.strip().lstrip("-").isdigit()]
    return []

ADMIN_IDS = get_env_list("ADMIN_IDS")

# --- SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Controller")

if not MONGO_URL or not SESSION_STRING:
    logger.error("❌ Config Missing!")
    sys.exit(1)

mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = mongo["AnimePaheBot"]
col_queue = db["queue"]

app = Client("controller_user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
engine = PaheEngine()

# --- DUMMY WEB SERVER ---
async def web_server():
    async def handle(request): return web.Response(text="Controller Bot is Running!")
    server = web.Application()
    server.router.add_get("/", handle)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌍 Web server started on port {PORT}")

# --- HELPERS ---

def normalize_for_cmd(text):
    if not text: return ""
    clean = re.sub(r"['\"!?;:]", "", text)
    return clean.strip()

def is_admin_id(user_id):
    if not ADMIN_IDS: return True
    # user_id is None when posting as the group/channel (only admins can do this)
    if user_id is None: return True
    return user_id in ADMIN_IDS

async def wait_for_result(start_time, target_title, target_ep, timeout=7200):
    logger.info(f"👀 Watching for: '{target_title} - Ep {target_ep} Uploaded!'")
    loop = asyncio.get_running_loop()
    loop_start = loop.time()

    expected_success = f"{target_title} - Ep {target_ep} Uploaded!".lower()
    safe_title = normalize_for_cmd(target_title)
    expected_safe_success = f"{safe_title} - Ep {target_ep} Uploaded!".lower()

    while True:
        if (loop.time() - loop_start) > timeout:
            return "timeout"

        try:
            async for msg in app.get_chat_history(TARGET_GROUP, limit=20):
                if not msg.text: continue

                msg_time = msg.edit_date or msg.date
                if msg_time < (start_time - timedelta(seconds=10)):
                    continue

                text = msg.text.lower()

                if (expected_success in text) or (expected_safe_success in text):
                    logger.info(f"✅ Detect: Success for {target_title}")
                    return "success"

                if "task finished" in text and "errors occurred" in text:
                    logger.info(f"❌ Detect: Explicit Failure")
                    return "failed"

        except Exception as e:
            logger.error(f"⚠️ Watcher Error: {e}")
            await asyncio.sleep(5)

        await asyncio.sleep(10)

async def warm_up():
    logger.info("🔥 Syncing dialogs to cache peers...")
    try:
        async for _ in app.get_dialogs():
            pass
        logger.info("✅ Dialogs synced.")
    except Exception as e:
        logger.warning(f"⚠️ Dialog sync warning: {e}")

    logger.info("🔥 Checking Target Group access...")
    try:
        chat = await app.get_chat(TARGET_GROUP)
        logger.info(f"✅ Target Group Found: {chat.title}")
    except Exception as e:
        logger.error(f"❌ Cannot access TARGET_GROUP ({TARGET_GROUP}): {e}")

async def reset_stuck_downloads():
    result = await col_queue.update_many(
        {"status": "downloading"},
        {"$set": {"status": "pending"}}
    )
    if result.modified_count > 0:
        logger.warning(f"⚠️ Reset {result.modified_count} stuck 'downloading' item(s) back to 'pending'.")

# --- COMMAND HANDLERS (called by the polling listener) ---

async def cmd_start():
    return (
        "👋 Controller is Online!\n\n"
        "/list — Latest releases\n"
        "/queue — View queue status\n"
        "/retry_all — Retry failed items\n"
        "/clear_queue — Delete pending items\n"
        "/restart — Restart the bot"
    )

async def cmd_list():
    try:
        releases = await asyncio.to_thread(engine.get_latest_releases)
        if not releases:
            return "❌ Failed to fetch releases."
        text = "📅 **Latest Anime Releases (Page 1):**\n\n"
        for i, item in enumerate(releases, 1):
            text += f"**{i}.** `{item['title']}` - Ep {item['ep']} ({item['time']})\n"
        return text
    except Exception as e:
        logger.error(f"List Error: {e}")
        return f"❌ Error: {e}"

async def cmd_queue():
    try:
        pending_count = await col_queue.count_documents({"status": "pending"})
        downloading_count = await col_queue.count_documents({"status": "downloading"})
        failed_count = await col_queue.count_documents({"status": "failed_dl"})

        cursor = col_queue.find(
            {"status": {"$in": ["pending", "downloading", "failed_dl"]}}
        ).sort([("status", -1), ("found_at", 1)])
        items = await cursor.to_list(length=30)

        if not items:
            return "✅ **Queue is empty!**"

        text = (
            f"📋 **Queue Status**\n"
            f"⏳ Pending: `{pending_count}` | ⬇️ Downloading: `{downloading_count}` | ⚠️ Failed: `{failed_count}`\n\n"
        )
        icons = {"pending": "⏳", "downloading": "⬇️", "failed_dl": "⚠️"}
        for i, item in enumerate(items, 1):
            icon = icons.get(item['status'], "❓")
            text += f"**{i}.** {item['title']} - Ep {item['ep']} `{icon}`\n"

        total = pending_count + downloading_count + failed_count
        if total > 30:
            text += f"\n...and {total - 30} more."
        return text
    except Exception as e:
        return f"❌ Error: {e}"

async def cmd_retry_all():
    try:
        r = await col_queue.update_many(
            {"status": {"$in": ["failed_dl", "downloading"]}},
            {"$set": {"status": "pending"}}
        )
        return f"🔄 Queued {r.modified_count} item(s) for retry."
    except Exception as e:
        return f"❌ Error: {e}"

async def cmd_clear_queue():
    try:
        r = await col_queue.delete_many({"status": {"$in": ["pending", "failed_dl", "downloading"]}})
        return f"🗑️ Deleted {r.deleted_count} items from queue."
    except Exception as e:
        return f"❌ Error: {e}"

# --- POLLING-BASED COMMAND LISTENER ---
# Pyrogram userbot on_message events are NOT delivered for supergroups/channels.
# We poll get_chat_history (proven to work) to detect and respond to commands.

async def task_command_listener():
    logger.info("🎧 Command Listener Started (polling mode).")

    # Bootstrap: get the latest message ID so we only process NEW messages
    last_seen_id = 0
    try:
        async for msg in app.get_chat_history(TARGET_GROUP, limit=1):
            last_seen_id = msg.id
        logger.info(f"🎧 Starting from message ID {last_seen_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not get initial message ID: {e}")

    while True:
        try:
            new_messages = []
            async for msg in app.get_chat_history(TARGET_GROUP, limit=10):
                if msg.id <= last_seen_id:
                    break
                new_messages.append(msg)

            # Process in chronological order (oldest first)
            for msg in reversed(new_messages):
                last_seen_id = max(last_seen_id, msg.id)

                if not msg.text:
                    continue

                text = msg.text.strip()
                if not text.startswith('/'):
                    continue

                # Extract command (strip @mention suffix if any)
                cmd = text.split()[0].split('@')[0].lower()

                sender_id = msg.from_user.id if msg.from_user else None
                if not is_admin_id(sender_id):
                    logger.info(f"⛔ Ignored command '{cmd}' from non-admin {sender_id}")
                    continue

                logger.info(f"📩 Command: {cmd} (msg_id={msg.id})")

                if cmd == '/start':
                    reply = await cmd_start()
                elif cmd == '/list':
                    await app.send_message(TARGET_GROUP, "🔍 Fetching releases...")
                    reply = await cmd_list()
                elif cmd == '/queue':
                    reply = await cmd_queue()
                elif cmd == '/retry_all':
                    reply = await cmd_retry_all()
                elif cmd == '/clear_queue':
                    reply = await cmd_clear_queue()
                elif cmd == '/restart':
                    await app.send_message(TARGET_GROUP, "🔄 Restarting...")
                    os.execl(sys.executable, sys.executable, *sys.argv)
                else:
                    continue

                await app.send_message(TARGET_GROUP, reply)

        except Exception as e:
            logger.error(f"Command Listener Error: {e}")

        await asyncio.sleep(3)

# --- TASKS ---

async def task_poller():
    logger.info("📡 Polling Task Started.")
    while True:
        try:
            releases = await asyncio.to_thread(engine.get_latest_releases)
            if releases:
                for item in releases:
                    session = item["session"]

                    if await col_queue.find_one({"_id": session}):
                        continue

                    # Check by title+ep across ALL statuses (including "done").
                    # Once an episode is in the DB in any state, never re-queue it
                    # even if AnimePahe rotates its session ID.
                    existing = await col_queue.find_one({
                        "title": item["title"],
                        "ep": item["ep"],
                    })
                    if existing:
                        continue

                    entry = {
                        "_id": session,
                        "title": item["title"],
                        "ep": item["ep"],
                        "time": item["time"],
                        "found_at": datetime.utcnow(),
                        "status": "pending",
                        "retry_count": 0
                    }
                    await col_queue.insert_one(entry)
                    logger.info(f"🆕 Queued: {item['title']} - Ep {item['ep']}")
        except Exception as e:
            logger.error(f"Polling Error: {e}")
        await asyncio.sleep(30)  # TEST: change back to 600 in production

async def task_downloader():
    logger.info("⬇️ Downloader Worker Started.")
    while True:
        item = await col_queue.find_one({"status": "pending"}, sort=[("found_at", 1)])

        if not item:
            await asyncio.sleep(10)
            continue

        title = item["title"]
        ep = item["ep"]
        safe_title = normalize_for_cmd(title)

        try:
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloading"}})

            logger.info(f"▶️ [START] Processing: {safe_title} Ep {ep}")
            start_time = datetime.utcnow()

            dl_cmd = f'/anime {safe_title} -e {ep} -r all'
            await app.send_message(TARGET_GROUP, dl_cmd)

            result = await wait_for_result(start_time, safe_title, ep, timeout=7200)

            if result == "success":
                logger.info(f"✅ [FINISH] {title} completed.")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloaded"}})

                logger.info("❄️ Success. Cooling down for 30 minutes...")
                await app.send_message(TARGET_GROUP, f"💤 Success! Cooling down 30m after **{safe_title}**...")
                await asyncio.sleep(1800)

            else:
                logger.warning(f"❌ [FAILED] {title}")
                await col_queue.update_one(
                    {"_id": item["_id"]},
                    {"$set": {"status": "failed_dl"}, "$inc": {"retry_count": 1}}
                )
                await asyncio.sleep(120)

        except Exception as e:
            logger.error(f"Downloader Error: {e}")
            await col_queue.update_one(
                {"_id": item["_id"]},
                {"$set": {"status": "pending"}, "$inc": {"retry_count": 1}}
            )
            await asyncio.sleep(60)

async def task_uploader():
    logger.info("⬆️ Uploader Monitor Started.")
    while True:
        item = await col_queue.find_one({"status": "downloaded"})
        if not item:
            await asyncio.sleep(10)
            continue
        await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "done"}})
        await asyncio.sleep(1)

async def main():
    # Start web server FIRST as a background task so Render/hosting platforms
    # detect the open port immediately, even before Telegram connects.
    asyncio.create_task(web_server())
    await asyncio.sleep(1)  # give it a moment to bind

    await app.start()
    await warm_up()
    await reset_stuck_downloads()
    await asyncio.gather(
        task_command_listener(),
        task_poller(),
        task_downloader(),
        task_uploader()
    )

if __name__ == "__main__":
    asyncio.run(main())
