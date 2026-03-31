import os
import json
import logging
import asyncio
import secrets
import io
import random
import hashlib
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, CallbackQuery,
    InputMediaPhoto, InputMediaVideo
)
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = Client(
    "storage_bot",
    bot_token=os.getenv("BOT_TOKEN"),
    api_id=int(os.getenv("APP_ID")),
    api_hash=os.getenv("API_HASH")
)

# 🔒 YOUR TELEGRAM USER ID (from @userinfobot)
OWNER_ID = 6575486956  # <-- REPLACE WITH YOUR ACTUAL USER ID!

STORAGE_FILE = "storage.json"
RECYCLE_FILE = "recycle_bin.json"
RECYCLE_TTL_DAYS = 30
MAX_HASH_SIZE = 10 * 1024 * 1024   # 10 MB
REMINDER_FILE = "reminder.json"
REMINDER_INTERVAL_DAYS = 30

# Pending state tracking
pending_deletes          = {}   # { user_id: [file_id, ...] }
pending_restores         = {}   # { user_id: [file_id, ...] }
pending_bulk_category    = {}   # { user_id: category }
pending_bulk_delete_category = {}
user_state               = {}
rename_pending           = {}
sort_pref                = {}
expired_links            = {}
pending_filename_dups    = {}   # { token: upload_data }
pending_content_dups     = {}   # { token: upload_data }
pending_all_uploads = {}   # { token: upload_data } – holds every upload until resolved

# ── Upload queue — one file at a time ────────────────────────────────────────
_upload_queue: "asyncio.Queue | None" = None
_upload_queue_running = False
_upload_confirm_events = {}   # { token: asyncio.Event }


# QR code (optional)
try:
    import qrcode as _qr
    HAS_QR = True
except ImportError:
    HAS_QR = False


# ─────────────────────────────────────────────
# STORAGE HELPERS
# ─────────────────────────────────────────────

def load_storage():
    if os.path.exists(STORAGE_FILE):
        try:
            with open(STORAGE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_storage(data):
    with open(STORAGE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def load_recycle_bin():
    if os.path.exists(RECYCLE_FILE):
        try:
            with open(RECYCLE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_recycle_bin(data):
    with open(RECYCLE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def load_last_reminder():
    if os.path.exists(REMINDER_FILE):
        try:
            with open(REMINDER_FILE, 'r') as f:
                data = json.load(f)
                return data.get('last_reminder', 0)
        except Exception:
            return 0
    return 0

def save_last_reminder(timestamp):
    with open(REMINDER_FILE, 'w') as f:
        json.dump({'last_reminder': timestamp}, f)

def get_existing_names_set():
    return {os.path.basename(info.get('name', '')) for info in stored_files.values()}

def is_duplicate(file_name, tg_file_id=None, file_unique_id=None):
    name_normalized = os.path.basename(file_name).lower().strip()
    for info in stored_files.values():
        stored_tg = info.get('tg_file_id', '')
        if tg_file_id and stored_tg and stored_tg == tg_file_id:
            return True, 'content'
        stored_uid = info.get('file_unique_id', '')
        if file_unique_id and stored_uid and stored_uid == file_unique_id:
            return True, 'content'
        stored_name = os.path.basename(info.get('name', '')).lower().strip()
        if stored_name and stored_name == name_normalized:
            return True, 'filename'
    return False, None

def check_duplicate_details(file_name, tg_file_id=None, file_unique_id=None, file_hash=None):
    name_normalized = os.path.basename(file_name).lower().strip()
    content_dup = False
    name_dup_info = None
    for info in stored_files.values():
        stored_tg = info.get('tg_file_id', '')
        if tg_file_id and stored_tg and stored_tg == tg_file_id:
            content_dup = True
        stored_uid = info.get('file_unique_id', '')
        if file_unique_id and stored_uid and stored_uid == file_unique_id:
            content_dup = True
        if file_hash and info.get('hash') == file_hash:
            content_dup = True
        stored_name = os.path.basename(info.get('name', '')).lower().strip()
        if stored_name and stored_name == name_normalized:
            name_dup_info = info
    return content_dup, name_dup_info

def generate_unique_name(base_name, existing_names_set):
    name, ext = os.path.splitext(base_name)
    counter = 1
    new_name = base_name
    while new_name in existing_names_set:
        new_name = f"{name}_{counter}{ext}"
        counter += 1
    return new_name

async def compute_file_hash(client, file_id: str, file_size: int) -> str:
    if file_size > MAX_HASH_SIZE:
        return None
    try:
        file_data = await client.download_media(file_id, in_memory=True)
        if file_data:
            hash_obj = hashlib.sha256()
            hash_obj.update(file_data.getvalue())
            return hash_obj.hexdigest()
    except Exception as e:
        logging.warning(f"Could not compute hash for file {file_id}: {e}")
    return None


# ─────────────────────────────────────────────
# FILE TYPE HELPERS (unchanged)
# ─────────────────────────────────────────────

def get_file_category(ext):
    ext = ext.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.ico']:
        return "📸 Images"
    elif ext == '.pdf':
        return "📑 PDF Documents"
    elif ext in ['.doc', '.docx']:
        return "📝 Word Documents"
    elif ext in ['.xls', '.xlsx']:
        return "📊 Excel Spreadsheets"
    elif ext in ['.ppt', '.pptx']:
        return "📽️ PowerPoint"
    elif ext == '.txt':
        return "📄 Text Files"
    elif ext in ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv']:
        return "🎬 Videos"
    elif ext in ['.mp3', '.wav', '.flac', '.aac', '.ogg']:
        return "🎵 Audio"
    elif ext in ['.zip', '.rar', '.7z', '.tar', '.gz']:
        return "🗜️ Archives"
    elif ext in ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c']:
        return "💻 Code Files"
    elif ext == '.apk':
        return "📱 Android Apps"
    else:
        return "📁 Other Files"

def get_file_icon(ext):
    ext = ext.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.ico']:
        return "📸"
    elif ext == '.pdf':
        return "📑"
    elif ext in ['.doc', '.docx']:
        return "📝"
    elif ext in ['.xls', '.xlsx']:
        return "📊"
    elif ext in ['.ppt', '.pptx']:
        return "📽️"
    elif ext == '.txt':
        return "📄"
    elif ext in ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv']:
        return "🎬"
    elif ext in ['.mp3', '.wav', '.flac', '.aac', '.ogg']:
        return "🎵"
    elif ext in ['.zip', '.rar', '.7z', '.tar', '.gz']:
        return "🗜️"
    elif ext in ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c']:
        return "💻"
    elif ext == '.apk':
        return "📱"
    else:
        return "📁"

def format_file_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.1f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.1f} GB"

def format_date(timestamp):
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "Unknown"

def group_files_by_category():
    categories = {}
    for fid, info in stored_files.items():
        ext = os.path.splitext(info.get('name', ''))[1].lower()
        cat = get_file_category(ext)
        categories.setdefault(cat, []).append({
            'id': fid,
            'name': info.get('name', 'Unknown'),
            'size': info.get('size', 0),
            'date': info.get('date', 0),
            'icon': get_file_icon(ext)
        })
    return categories


# ─────────────────────────────────────────────
# STATS PIE CHART (optional)
# ─────────────────────────────────────────────

CAT_COLORS = {
    "📸 Images":           "#4A90D9",
    "🎬 Videos":           "#E74C3C",
    "🎵 Audio":            "#9B59B6",
    "📑 PDF Documents":    "#27AE60",
    "📝 Word Documents":   "#2ECC71",
    "📊 Excel Spreadsheets": "#F39C12",
    "📽️ PowerPoint":       "#E67E22",
    "📄 Text Files":       "#95A5A6",
    "🗜️ Archives":         "#C0392B",
    "💻 Code Files":       "#1ABC9C",
    "📱 Android Apps":     "#3498DB",
    "📁 Other Files":      "#BDC3C7",
}

def generate_stats_pie_chart(cats, stored_files):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        labels, sizes, colors = [], [], []
        for cat, files in sorted(cats.items(), key=lambda x: -len(x[1])):
            count = len(files)
            if count == 0:
                continue
            labels.append(f"{cat} ({count})")
            sizes.append(count)
            colors.append(CAT_COLORS.get(cat, "#BDC3C7"))
        if not sizes:
            return None
        fig, ax = plt.subplots(figsize=(9, 6), facecolor="#1a1a2e")
        ax.set_facecolor("#1a1a2e")
        wedges, texts, autotexts = ax.pie(
            sizes,
            labels=None,
            colors=colors,
            autopct=lambda p: f"{p:.1f}%" if p > 3 else "",
            startangle=140,
            pctdistance=0.78,
            wedgeprops={"linewidth": 2, "edgecolor": "#1a1a2e"},
            shadow=False,
        )
        for at in autotexts:
            at.set_fontsize(9)
            at.set_color("white")
            at.set_fontweight("bold")
        legend_patches = [mpatches.Patch(color=colors[i], label=labels[i]) for i in range(len(labels))]
        ax.legend(
            handles=legend_patches,
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            fontsize=9,
            framealpha=0.15,
            facecolor="#2c2c54",
            edgecolor="#444",
            labelcolor="white",
        )
        total = sum(sizes)
        ax.set_title(f"📊 Storage by Category  |  {total} files", color="white", fontsize=13, fontweight="bold", pad=16)
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf
    except ImportError:
        return None
    except Exception as e:
        logging.warning(f"Pie chart generation failed: {e}")
        return None


# ─────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────

def get_main_menu():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📤 Upload"), KeyboardButton("🖼️ Gallery View")],
            [KeyboardButton("📂 Categories"), KeyboardButton("🔍 Search")],
            [KeyboardButton("ℹ️ Help"), KeyboardButton("📊 Stats")],
            [KeyboardButton("⭐ Favourites"), KeyboardButton("♻️ Recycle Bin")],
            [KeyboardButton("🔁 Duplicates")]
        ],
        resize_keyboard=True
    )

def get_inline_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Upload File", callback_data="upload")],
        [InlineKeyboardButton("🖼️ Gallery View", callback_data="gallery_1")],
        [InlineKeyboardButton("📂 Categories", callback_data="categories")],
        [InlineKeyboardButton("🔍 Search", callback_data="search")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="help")],
        [InlineKeyboardButton("⭐ Favourites", callback_data="favourites_1"),
         InlineKeyboardButton("♻️ Recycle Bin", callback_data="recycle_bin_1")],
        [InlineKeyboardButton("🔁 Duplicates", callback_data="duplicates")]
    ])

def is_owner(user_id):
    return user_id == OWNER_ID


# ─────────────────────────────────────────────
# RECYCLE BIN FUNCTIONS
# ─────────────────────────────────────────────

def get_recycle_categories():
    categories = {}
    for fid, info in recycle_bin.items():
        ext = os.path.splitext(info.get('name', ''))[1].lower()
        cat = get_file_category(ext)
        categories.setdefault(cat, []).append({
            'id': fid, 'name': info.get('name', 'Unknown'),
            'size': info.get('size', 0), 'deleted_at': info.get('deleted_at', 0)
        })
    return categories

async def show_recycle_categories(message, client, page=1):
    if not recycle_bin:
        await message.reply_text(
            "♻️ **Recycle Bin is empty!**\n\nDeleted files will appear here.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
        )
        return

    categories = get_recycle_categories()
    total = len(recycle_bin)

    now = datetime.now().timestamp()
    soonest = min(recycle_bin.values(), key=lambda i: i.get('deleted_at', now))
    soonest_deleted = soonest.get('deleted_at', now)
    days_left = max(0, RECYCLE_TTL_DAYS - int((now - soonest_deleted) / 86400))
    expiry_str = format_date(soonest_deleted + RECYCLE_TTL_DAYS * 86400)

    text = (
        f"♻️ **RECYCLE BIN** — {total} file(s)\n"
        f"{'─' * 30}\n"
        f"⏳ Files are auto-deleted after **{RECYCLE_TTL_DAYS} days**\n"
        f"🔜 Next auto-delete in **{days_left} day(s)** ({expiry_str})\n"
        f"{'─' * 30}\n\n"
    )

    buttons = []
    for cat, files in sorted(categories.items()):
        text += f"{cat} — {len(files)} file(s)\n"
        buttons.append([InlineKeyboardButton(
            f"{cat} ({len(files)})", callback_data=f"rb_cat_{cat}_1"
        )])

    text += "\n" + "─" * 30
    buttons.append([
        InlineKeyboardButton("🗑️ Empty Recycle Bin", callback_data="rb_empty_ask"),
        InlineKeyboardButton("♻️ Bulk Restore", callback_data="rb_bulk_restore_start"),
        InlineKeyboardButton("🏠 Main Menu",          callback_data="main_menu"),
    ])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_bulk_restore_menu(message, client, user_id, page=1):
    items_per_page = 10
    category = pending_bulk_category.get(user_id)

    if category:
        all_files = []
        for fid, info in recycle_bin.items():
            ext = os.path.splitext(info.get('name', ''))[1].lower()
            if get_file_category(ext) == category:
                all_files.append((fid, info))
        all_files.sort(key=lambda x: x[1].get('deleted_at', 0), reverse=True)
    else:
        all_files = list(recycle_bin.items())
        all_files.sort(key=lambda x: x[1].get('deleted_at', 0), reverse=True)

    total = len(all_files)
    if total == 0:
        await message.reply_text("♻️ Recycle bin is empty!")
        return

    total_pages = max(1, (total + items_per_page - 1) // items_per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * items_per_page
    end = min(start + items_per_page, total)
    page_files = all_files[start:end]

    selected = pending_restores.get(user_id, [])

    buttons = []
    for fid, info in page_files:
        name = os.path.basename(info.get('name', 'Unknown'))
        is_sel = fid in selected
        tick = "✅ " if is_sel else "☐ "
        cb = f"rb_bulk_toggle_{fid}_{page}"
        buttons.append([InlineKeyboardButton(f"{tick}{name}", callback_data=cb)])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"rb_bulk_page_{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"rb_bulk_page_{page+1}"))
    if nav:
        buttons.append(nav)

    sel_count = len(selected)
    buttons.append([
        InlineKeyboardButton(f"♻️ Restore {sel_count} selected", callback_data="rb_bulk_restore_confirm") if sel_count else InlineKeyboardButton("☐ No files selected", callback_data="noop"),
        InlineKeyboardButton("❌ Cancel", callback_data="rb_bulk_restore_cancel")
    ])
    buttons.append([
        InlineKeyboardButton(f"✅ Select all {len(page_files)} files on this page", callback_data=f"rb_bulk_select_all_{page}")
    ])

    if category:
        buttons.append([InlineKeyboardButton("🔙 Back to Category", callback_data=f"rb_cat_{category}_1")])
    else:
        buttons.append([InlineKeyboardButton("🔙 Back to Recycle Bin", callback_data="recycle_bin_1")])

    header = (
        f"♻️ **BULK RESTORE** — Page {page}/{total_pages}\n"
        f"✅ Selected: {sel_count} file{'s' if sel_count != 1 else ''}\n\n"
        f"_Tap a file to select/deselect it._\n"
        f"Files will be restored to your main storage."
    )
    await message.reply_text(header, reply_markup=InlineKeyboardMarkup(buttons))


async def show_recycle_category_page(message, client, category, page=1):
    from pyrogram.types import InputMediaPhoto, InputMediaVideo
    IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov'}

    cat_files = []
    for fid, info in recycle_bin.items():
        ext = os.path.splitext(info.get('name', ''))[1].lower()
        if get_file_category(ext) == category:
            cat_files.append((fid, info))

    cat_files.sort(key=lambda x: x[1].get('deleted_at', 0), reverse=True)
    total = len(cat_files)

    if total == 0:
        await message.reply_text(f"♻️ {category}\n\nNo files in this category.")
        return

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total)
    page_files = cat_files[start:end]

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"rb_cat_{category}_{page - 1}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"rb_cat_{category}_{page + 1}"))

    await message.reply_text(
        f"♻️ **{category}** — Page {page}/{total_pages}  •  {start+1}–{end} of {total}\n"
        f"_Tap **View** to preview • **Restore** to recover • **Delete** to erase forever_",
        reply_markup=InlineKeyboardMarkup([nav])
    )

    image_bucket, video_bucket, other_list = [], [], []
    for idx, (fid, info) in enumerate(page_files, start + 1):
        ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
        if ext in IMAGE_EXTS:
            image_bucket.append((idx, fid, info))
        elif ext in VIDEO_EXTS:
            video_bucket.append((idx, fid, info))
        else:
            other_list.append((idx, fid, info))

    def rb_control_panel(bucket):
        rows = []
        for idx, fid, info in bucket:
            deleted_str = format_date(info.get('deleted_at', 0))
            days_left = max(0, RECYCLE_TTL_DAYS - int((datetime.now().timestamp() - info.get('deleted_at', 0)) / 86400))
            rows.append([
                InlineKeyboardButton(f"#{idx} 👁️ View",  callback_data=f"rb_view_{fid}"),
                InlineKeyboardButton(f"♻️ Restore",       callback_data=f"rb_restore_ask_{fid}"),
                InlineKeyboardButton(f"🗑️ Del Forever",   callback_data=f"rb_permdelete_{fid}"),
            ])
            rows.append([InlineKeyboardButton(f"🗑️ Deleted: {deleted_str} · ⏳ {days_left}d left", callback_data="noop")])
        return InlineKeyboardMarkup(rows)

    if image_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in image_bucket:
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            deleted_str = format_date(info.get('deleted_at', 0))
            media_group.append(InputMediaPhoto(tg_id, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n🗑️ {deleted_str}"))
            valid_bucket.append((idx, fid, info))
        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Album error: {e}")
            if valid_bucket:
                await message.reply_text(
                    f"🖼️ **Image Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})",
                    reply_markup=rb_control_panel(valid_bucket)
                )

    if video_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in video_bucket:
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            deleted_str = format_date(info.get('deleted_at', 0))
            media_group.append(InputMediaVideo(tg_id, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n🗑️ {deleted_str}", supports_streaming=True))
            valid_bucket.append((idx, fid, info))
        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Video album error: {e}")
            if valid_bucket:
                await message.reply_text(
                    f"🎬 **Video Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})",
                    reply_markup=rb_control_panel(valid_bucket)
                )

    for idx, fid, info in other_list:
        file_name = info.get('name', 'Unknown')
        clean_name = os.path.basename(file_name)
        ext = info.get('ext', os.path.splitext(file_name)[1]).lower()
        size_str = format_file_size(info.get('size', 0))
        deleted_str = format_date(info.get('deleted_at', 0))
        icon = get_file_icon(ext)
        caption = f"{icon} **#{idx}. {clean_name}**\n📏 {size_str}\n🗑️ Deleted: {deleted_str}"
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁️ View",       callback_data=f"rb_view_{fid}"),
            InlineKeyboardButton("♻️ Restore",     callback_data=f"rb_restore_ask_{fid}"),
            InlineKeyboardButton("🗑️ Del Forever", callback_data=f"rb_permdelete_{fid}"),
        ]])
        try:
            await send_file_smart(client, message.chat.id, info, caption, buttons)
        except Exception as e:
            await client.send_message(message.chat.id, f"{icon} **#{idx}. {clean_name}**\n❌ {e}", reply_markup=buttons)

    footer_nav = []
    if page > 1:
        footer_nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"rb_cat_{category}_{page - 1}"))
    if page < total_pages:
        footer_nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"rb_cat_{category}_{page + 1}"))

    footer_rows = []
    if footer_nav:
        footer_rows.append(footer_nav)
    footer_rows.append([
        InlineKeyboardButton("♻️ Bulk Restore", callback_data=f"rb_bulk_restore_start_cat_{category}"),
        InlineKeyboardButton("🔙 Back to Recycle Bin", callback_data="recycle_bin_1"),
        InlineKeyboardButton("🏠 Main Menu",            callback_data="main_menu"),
    ])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(footer_rows))


async def view_recycle_file(client, message, fid):
    info = recycle_bin.get(fid)
    if not info:
        await message.reply_text("❌ File not found in recycle bin.")
        return

    ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
    file_name = info.get('name', 'Unknown')
    size_str = format_file_size(info.get('size', 0))
    deleted_str = format_date(info.get('deleted_at', 0))

    caption = (
        f"♻️ **{os.path.basename(file_name)}**\n"
        f"📏 {size_str}\n"
        f"🗑️ Deleted: {deleted_str}"
    )
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("♻️ Restore",        callback_data=f"rb_restore_ask_{fid}"),
         InlineKeyboardButton("🗑️ Delete Forever", callback_data=f"rb_permdelete_{fid}")],
        [InlineKeyboardButton("🔙 Recycle Bin",    callback_data="recycle_bin_1"),
         InlineKeyboardButton("🏠 Main Menu",       callback_data="main_menu")],
    ])

    try:
        await send_file_smart(client, message.chat.id, info, caption, buttons)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}", reply_markup=buttons)


# ─────────────────────────────────────────────
# FAVOURITES
# ─────────────────────────────────────────────

async def show_favourites_page(message, client, page=1, user_id=None):
    from pyrogram.types import InputMediaPhoto, InputMediaVideo
    IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov'}

    favs = [(fid, info) for fid, info in stored_files.items() if info.get('favourite')]
    favs.sort(key=lambda x: x[1].get('date', 0), reverse=True)
    total = len(favs)

    if total == 0:
        await message.reply_text(
            "⭐ **No favourites yet!**\n\nTap ☆ Fav on any file to star it.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
            ]])
        )
        return

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total)
    page_files = favs[start:end]

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"favourites_{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"favourites_{page+1}"))

    await message.reply_text(
        f"⭐ **FAVOURITES** — Page {page}/{total_pages}  •  {start+1}–{end} of {total}",
        reply_markup=InlineKeyboardMarkup([nav])
    )

    image_bucket, video_bucket, other_list = [], [], []
    for idx, (fid, info) in enumerate(page_files, start + 1):
        ext = info.get('ext', os.path.splitext(info.get('name',''))[1]).lower()
        if ext in IMAGE_EXTS:
            image_bucket.append((idx, fid, info))
        elif ext in VIDEO_EXTS:
            video_bucket.append((idx, fid, info))
        else:
            other_list.append((idx, fid, info))

    def fav_panel(bucket):
        rows = []
        for idx, fid, info in bucket:
            rows.append([
                InlineKeyboardButton(f"#{idx} 👁️ View",  callback_data=f"view_{fid}"),
                InlineKeyboardButton("⭐ Unfav",           callback_data=f"fav_{fid}"),
                InlineKeyboardButton("🗑️ Del",             callback_data=f"delete_{fid}"),
            ])
        return InlineKeyboardMarkup(rows)

    if image_bucket:
        mg, vb = [], []
        for idx, fid, info in image_bucket:
            tg_id = info.get('tg_file_id','')
            if not tg_id: continue
            date_str = format_date(info.get('date',0))
            mg.append(InputMediaPhoto(tg_id, caption=f"⭐#{idx} {os.path.basename(info.get('name',''))}\n📤 {date_str}"))
            vb.append((idx, fid, info))
        if mg:
            try:
                await client.send_media_group(message.chat.id, mg)
            except Exception as e:
                await message.reply_text(f"⚠️ {e}")
            if vb:
                await message.reply_text(
                    f"⭐ **Controls** (#{vb[0][0]}–#{vb[-1][0]})",
                    reply_markup=fav_panel(vb)
                )

    if video_bucket:
        mg, vb = [], []
        for idx, fid, info in video_bucket:
            tg_id = info.get('tg_file_id','')
            if not tg_id: continue
            mg.append(InputMediaVideo(tg_id, caption=f"⭐#{idx} {os.path.basename(info.get('name',''))}", supports_streaming=True))
            vb.append((idx, fid, info))
        if mg:
            try:
                await client.send_media_group(message.chat.id, mg)
            except Exception as e:
                await message.reply_text(f"⚠️ {e}")
            if vb:
                await message.reply_text(
                    f"⭐ **Controls** (#{vb[0][0]}–#{vb[-1][0]})",
                    reply_markup=fav_panel(vb)
                )

    for idx, fid, info in other_list:
        clean = os.path.basename(info.get('name','Unknown'))
        icon = get_file_icon(info.get('ext','.'))
        caption = f"{icon} ⭐ **#{idx}. {clean}**\n📏 {format_file_size(info.get('size',0))}"
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁️ View",   callback_data=f"view_{fid}"),
            InlineKeyboardButton("⭐ Unfav",  callback_data=f"fav_{fid}"),
            InlineKeyboardButton("🗑️ Del",    callback_data=f"delete_{fid}"),
        ]])
        try:
            await send_file_smart(client, message.chat.id, info, caption, buttons)
        except Exception as e:
            await client.send_message(message.chat.id, f"{icon} #{idx}. {clean}\n❌ {e}", reply_markup=buttons)

    footer = []
    if page > 1:
        footer.append(InlineKeyboardButton("◀️ Prev", callback_data=f"favourites_{page-1}"))
    if page < total_pages:
        footer.append(InlineKeyboardButton("Next ▶️", callback_data=f"favourites_{page+1}"))
    rows = [footer] if footer else []
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(rows))


# ─────────────────────────────────────────────
# SECURITY — MALICIOUS FILE DETECTION
# ─────────────────────────────────────────────

BLOCKED_EXTENSIONS = {
    '.exe', '.bat', '.cmd', '.com', '.pif', '.scr', '.vbs', '.vbe',
    '.js', '.jse', '.ws', '.wsf', '.wsc', '.wsh', '.ps1', '.ps1xml',
    '.ps2', '.ps2xml', '.psc1', '.psc2', '.msh', '.msh1', '.msh2',
    '.mshxml', '.msh1xml', '.msh2xml', '.scf', '.lnk', '.inf',
    '.reg', '.dll', '.sys', '.drv', '.cpl', '.ocx', '.jar',
    '.msi', '.msp', '.hta', '.htm_embedded', '.xbap',
}

SUSPICIOUS_PATTERNS = [
    'invoice', 'payment', 'urgent', 'account-suspended',
    'verify-now', 'click-here', 'free-download',
]

def check_malicious(file_name: str, file_size: int) -> tuple[bool, str]:
    name_lower = file_name.lower()
    ext = os.path.splitext(name_lower)[1]
    if ext in BLOCKED_EXTENSIONS:
        return True, f"Blocked file type: `{ext}`"
    parts = name_lower.split('.')
    if len(parts) >= 3:
        second_ext = '.' + parts[-2]
        if second_ext in BLOCKED_EXTENSIONS:
            return True, f"Double extension attack detected: `{file_name}`"
    if chr(0) in file_name:
        return True, "Null byte in filename — potential injection attempt"
    for pattern in SUSPICIOUS_PATTERNS:
        if pattern in name_lower:
            return True, f"Suspicious filename pattern: `{pattern}`"
    if file_size < 100 and ext in {'.pdf', '.docx', '.xlsx', '.jpg', '.png'}:
        return True, f"Suspiciously small file ({file_size} bytes) for type `{ext}`"
    return False, ""


# ─────────────────────────────────────────────
# DUPLICATE FILE MANAGER
# ─────────────────────────────────────────────

async def show_duplicates(message, client):
    by_uid = {}
    by_name = {}
    for fid, info in stored_files.items():
        uid = info.get('file_unique_id', '')
        name = os.path.basename(info.get('name', '')).lower()
        if uid:
            by_uid.setdefault(uid, []).append((fid, info))
        by_name.setdefault(name, []).append((fid, info))
    seen_fids = set()
    dupe_groups = {}
    for uid, entries in by_uid.items():
        if len(entries) > 1:
            label = f"[same content] {os.path.basename(entries[0][1].get('name',''))}"
            dupe_groups[label] = entries
            for fid, _ in entries:
                seen_fids.add(fid)
    for name, entries in by_name.items():
        if len(entries) > 1 and not all(fid in seen_fids for fid, _ in entries):
            dupe_groups[f"[same name] {name}"] = entries
    dupes = dupe_groups
    if not dupes:
        await message.reply_text(
            "✅ **No duplicate files found!**\n\nAll files are unique.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
            ]])
        )
        return
    total_extra = sum(len(v) - 1 for v in dupes.values())
    text = f"🔁 **DUPLICATES** — {len(dupes)} group(s), {total_extra} extra copy/copies\n" + "─"*30 + "\n\n"
    buttons = []
    for label, entries in sorted(dupes.items()):
        icon = "🔴" if "[same content]" in label else "🟡"
        display = label.replace('[same content] ', '').replace('[same name] ', '')
        text += f"{icon} **{display}** — {len(entries)} copies\n"
        # Show each file with a view button
        for fid, info in entries:
            date_str = format_date(info.get('date', 0))
            size_str = format_file_size(info.get('size', 0))
            text += f"  · {size_str}  📅 {date_str}"
            # Add a view button next to each file
            buttons.append([InlineKeyboardButton(f"👁️ View", callback_data=f"view_duplicate_{fid}")])
            text += "\n"
        # Add the keep buttons for the whole group
        safe_key = label
        buttons.append([
            InlineKeyboardButton(f"🗑️ Keep newest · {display[:22]}", callback_data=f"dedup_keepnewest_{safe_key}"),
            InlineKeyboardButton(f"🗑️ Keep oldest · {display[:22]}", callback_data=f"dedup_keepoldest_{safe_key}")
        ])
        text += "\n"
    text += "🔴 = identical file content  |  🟡 = same filename\n"
    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ─────────────────────────────────────────────
# GALLERY VIEW (10 items per page)
# ─────────────────────────────────────────────

ITEMS_PER_PAGE = 10

def get_sorted_files(user_id=None):
    pref = sort_pref.get(user_id, 'date') if user_id else 'date'
    items = list(stored_files.items())
    if pref == 'name':
        items.sort(key=lambda x: x[1].get('name', '').lower())
    elif pref == 'size':
        items.sort(key=lambda x: x[1].get('size', 0), reverse=True)
    else:  # date
        items.sort(key=lambda x: x[1].get('date', 0), reverse=True)
    return items

async def show_gallery_page(message, client, page=1, user_id=None):
    from pyrogram.types import InputMediaPhoto, InputMediaVideo
    all_files = get_sorted_files(user_id)
    total = len(all_files)
    if total == 0:
        await message.reply_text("📁 **No files stored yet!**\n\nSend me a file to get started. 📤")
        return

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total)
    page_files = all_files[start:end]

    # Header navigation
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gallery_{page - 1}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"gallery_{page + 1}"))

    pref_label = {'date': '🕐 Date', 'name': '🔤 Name', 'size': '💾 Size'}.get(sort_pref.get(user_id, 'date'), '🕐 Date')
    await message.reply_text(
        f"🖼️ **GALLERY** — Page {page}/{total_pages}  •  Files {start+1}–{end} of {total}\n"
        f"_Sorted by: {pref_label}_",
        reply_markup=InlineKeyboardMarkup([nav, [
            InlineKeyboardButton("🔤 Name",  callback_data="sort_name"),
            InlineKeyboardButton("💾 Size",  callback_data="sort_size"),
            InlineKeyboardButton("🕐 Date",  callback_data="sort_date"),
        ]])
    )

    IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov'}
    image_bucket = []
    video_bucket = []
    other_list = []
    for idx, (fid, info) in enumerate(page_files, start + 1):
        ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
        if ext in IMAGE_EXTS:
            image_bucket.append((idx, fid, info))
        elif ext in VIDEO_EXTS:
            video_bucket.append((idx, fid, info))
        else:
            other_list.append((idx, fid, info))

    def control_panel(bucket):
        rows = []
        for idx, fid, info in bucket:
            date_str = format_date(info.get('date', 0))
            rows.append([
                InlineKeyboardButton(f"#{idx} 👁️ View",   callback_data=f"view_{fid}"),
                InlineKeyboardButton(f"🔗 Link",           callback_data=f"link_{fid}"),
                InlineKeyboardButton(f"🗑️ Del",            callback_data=f"delete_{fid}"),
            ])
            rows.append([InlineKeyboardButton(f"📤 Uploaded: {date_str}", callback_data="noop")])
        return InlineKeyboardMarkup(rows)

    if image_bucket:
        media_group = []
        valid_bucket = []
        for idx, fid, info in image_bucket:
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            date_str = format_date(info.get('date', 0))
            caption = f"#{idx} {os.path.basename(info.get('name', ''))}\n📤 {date_str}"
            media_group.append(InputMediaPhoto(tg_id, caption=caption))
            valid_bucket.append((idx, fid, info))
        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Album send error: {e}")
            if valid_bucket:
                label = f"🖼️ **Image Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})"
                await message.reply_text(label, reply_markup=control_panel(valid_bucket))

    if video_bucket:
        media_group = []
        valid_bucket = []
        for idx, fid, info in video_bucket:
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            date_str = format_date(info.get('date', 0))
            caption = f"#{idx} {os.path.basename(info.get('name', ''))}\n📤 {date_str}"
            media_group.append(InputMediaVideo(tg_id, caption=caption, supports_streaming=True))
            valid_bucket.append((idx, fid, info))
        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Video album error: {e}")
            if valid_bucket:
                label = f"🎬 **Video Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})"
                await message.reply_text(label, reply_markup=control_panel(valid_bucket))

    for idx, fid, info in other_list:
        file_name = info.get('name', 'Unknown')
        clean_name = os.path.basename(file_name)
        ext = info.get('ext', os.path.splitext(file_name)[1]).lower()
        size_str = format_file_size(info.get('size', 0))
        date_str = format_date(info.get('date', 0))
        icon = get_file_icon(ext)
        caption = f"{icon} **#{idx}. {clean_name}**\n📏 {size_str}\n📤 {date_str}"
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
            InlineKeyboardButton("🔗 Link", callback_data=f"link_{fid}"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{fid}"),
        ]])
        try:
            await send_file_smart(client, message.chat.id, info, caption, buttons)
        except Exception as e:
            await client.send_message(message.chat.id, f"{icon} **#{idx}. {clean_name}**\n❌ {e}", reply_markup=buttons)

    # Numbered pagination footer
    if total_pages <= 10:
        page_range = range(1, total_pages + 1)
    else:
        start_range = max(1, page - 3)
        end_range = min(total_pages, page + 3)
        if start_range == 1:
            end_range = min(total_pages, 7)
        elif end_range == total_pages:
            start_range = max(1, total_pages - 6)
        page_range = range(start_range, end_range + 1)

    pagination_row = []
    if page > 1:
        pagination_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gallery_{page-1}"))
    for p in page_range:
        pagination_row.append(InlineKeyboardButton(str(p), callback_data=f"gallery_{p}"))
    if page < total_pages:
        pagination_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"gallery_{page+1}"))

    footer_rows = []
    if pagination_row:
        footer_rows.append(pagination_row)
    footer_rows.append([
        InlineKeyboardButton("🗑️ Bulk Delete", callback_data="bulk_delete_start"),
        InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
    ])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(footer_rows))


# ─────────────────────────────────────────────
# CATEGORIES VIEW
# ─────────────────────────────────────────────

async def show_categories(message, client):
    categories = group_files_by_category()
    if not categories:
        await message.reply_text("📁 No files stored yet!")
        return

    text = "📂 **FILES BY CATEGORY**\n" + "─" * 30 + "\n\n"
    buttons = []
    for cat, files in sorted(categories.items()):
        text += f"{cat} — {len(files)} file{'s' if len(files) != 1 else ''}\n"
        buttons.append([InlineKeyboardButton(f"{cat} ({len(files)})", callback_data=f"category_{cat}")])

    text += "\n" + "─" * 30 + f"\n📊 Total: {len(stored_files)} files"
    buttons.append([InlineKeyboardButton("🖼️ Gallery View", callback_data="gallery_1")])
    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_category_page(message, client, category, page=1):
    from pyrogram.types import InputMediaPhoto, InputMediaVideo
    IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov'}

    cat_files = []
    for fid, info in stored_files.items():
        ext = os.path.splitext(info.get('name', ''))[1].lower()
        if get_file_category(ext) == category:
            cat_files.append((fid, info))
    cat_files.sort(key=lambda x: x[1].get('date', 0), reverse=True)
    total = len(cat_files)
    if total == 0:
        await message.reply_text(f"📂 {category}\n\nNo files in this category.")
        return

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total)
    page_files = cat_files[start:end]

    # Header
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"catpage_{category}_{page - 1}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"catpage_{category}_{page + 1}"))

    await message.reply_text(
        f"📂 **{category}** — Page {page}/{total_pages}  •  {start+1}–{end} of {total}",
        reply_markup=InlineKeyboardMarkup([nav])
    )

    image_bucket, video_bucket, other_list = [], [], []
    for idx, (fid, info) in enumerate(page_files, start + 1):
        ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
        if ext in IMAGE_EXTS:
            image_bucket.append((idx, fid, info))
        elif ext in VIDEO_EXTS:
            video_bucket.append((idx, fid, info))
        else:
            other_list.append((idx, fid, info))

    def control_panel(bucket):
        rows = []
        for idx, fid, info in bucket:
            date_str = format_date(info.get('date', 0))
            rows.append([
                InlineKeyboardButton(f"#{idx} 👁️ View", callback_data=f"view_{fid}"),
                InlineKeyboardButton("🔗 Link",          callback_data=f"link_{fid}"),
                InlineKeyboardButton("🗑️ Del",           callback_data=f"delete_{fid}"),
            ])
            rows.append([InlineKeyboardButton(f"📤 Uploaded: {date_str}", callback_data="noop")])
        return InlineKeyboardMarkup(rows)

    # Image album
    if image_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in image_bucket:
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            date_str = format_date(info.get('date', 0))
            media_group.append(InputMediaPhoto(tg_id, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n📤 {date_str}"))
            valid_bucket.append((idx, fid, info))
        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Album error: {e}")
            await message.reply_text(
                f"🖼️ **Image Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})",
                reply_markup=control_panel(valid_bucket)
            )

    # Video album
    if video_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in video_bucket:
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            date_str = format_date(info.get('date', 0))
            media_group.append(InputMediaVideo(tg_id, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n📤 {date_str}", supports_streaming=True))
            valid_bucket.append((idx, fid, info))
        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Video album error: {e}")
            await message.reply_text(
                f"🎬 **Video Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})",
                reply_markup=control_panel(valid_bucket)
            )

    # Other files individually
    for idx, fid, info in other_list:
        file_name = info.get('name', 'Unknown')
        clean_name = os.path.basename(file_name)
        ext = info.get('ext', os.path.splitext(file_name)[1]).lower()
        size_str = format_file_size(info.get('size', 0))
        date_str = format_date(info.get('date', 0))
        icon = get_file_icon(ext)
        caption = f"{icon} **#{idx}. {clean_name}**\n📏 {size_str}\n📤 {date_str}"
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
            InlineKeyboardButton("🔗 Link", callback_data=f"link_{fid}"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{fid}"),
        ]])
        try:
            await send_file_smart(client, message.chat.id, info, caption, buttons)
        except Exception as e:
            await client.send_message(message.chat.id, f"{icon} **#{idx}. {clean_name}**\n❌ {e}", reply_markup=buttons)

    # Footer pagination
    if total_pages <= 10:
        page_range = range(1, total_pages + 1)
    else:
        start_range = max(1, page - 3)
        end_range = min(total_pages, page + 3)
        if start_range == 1:
            end_range = min(total_pages, 7)
        elif end_range == total_pages:
            start_range = max(1, total_pages - 6)
        page_range = range(start_range, end_range + 1)

    pagination_row = []
    if page > 1:
        pagination_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"catpage_{category}_{page-1}"))
    for p in page_range:
        pagination_row.append(InlineKeyboardButton(str(p), callback_data=f"catpage_{category}_{p}"))
    if page < total_pages:
        pagination_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"catpage_{category}_{page+1}"))

    footer_rows = []
    if pagination_row:
        footer_rows.append(pagination_row)
    footer_rows.append([
        InlineKeyboardButton("🗑️ Bulk Delete", callback_data=f"bulk_delete_start_cat_{category}"),
        InlineKeyboardButton("🔙 Categories",  callback_data="categories"),
        InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
    ])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(footer_rows))


# ─────────────────────────────────────────────
# FILE VIEW HELPER
# ─────────────────────────────────────────────

async def send_file_smart(client, chat_id, info, caption, buttons):
    ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
    tg_id = info.get('tg_file_id', '')
    if not tg_id:
        raise ValueError("No Telegram file_id stored for this file.")
    if ext in ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'):
        await client.send_photo(chat_id, tg_id, caption=caption, reply_markup=buttons)
    elif ext in ('.mp4', '.avi', '.mkv', '.mov'):
        await client.send_video(chat_id, tg_id, caption=caption, reply_markup=buttons, supports_streaming=True)
    elif ext in ('.mp3', '.wav', '.flac', '.aac', '.ogg'):
        await client.send_audio(chat_id, tg_id, caption=caption, reply_markup=buttons)
    else:
        await client.send_document(chat_id, tg_id, caption=caption, reply_markup=buttons)


async def view_file(client, message, fid):
    info = stored_files.get(fid)
    if not info or not info.get('tg_file_id'):
        await message.reply_text("❌ File not found or has been deleted.")
        return
    file_name = info.get('name', 'Unknown')
    size_str = format_file_size(info.get('size', 0))
    link_url = f"https://t.me/{client.me.username}?start=file_{fid}"
    is_fav = info.get('favourite', False)
    tag = info.get('tag', '')
    tag_line = f"\n🏷️ Tag: {tag}" if tag else ''
    caption = f"👁️ **{file_name}**\n📏 {size_str}{tag_line}\n🔗 [Shareable link]({link_url})"
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Get Link", callback_data=f"link_{fid}"),
         InlineKeyboardButton("🗑️ Delete",   callback_data=f"delete_{fid}")],
        [InlineKeyboardButton("⭐ Unfav" if is_fav else "☆ Fav", callback_data=f"fav_{fid}"),
         InlineKeyboardButton("✏️ Rename",  callback_data=f"rename_{fid}"),
         InlineKeyboardButton("🏷️ Tag",     callback_data=f"tag_{fid}")],
        [InlineKeyboardButton("⏰ Expiring Link", callback_data=f"explink_{fid}"),
         InlineKeyboardButton("🏠 Main Menu",     callback_data="main_menu")],
    ])
    try:
        await send_file_smart(client, message.chat.id, info, caption, buttons)
    except Exception as e:
        await message.reply_text(f"❌ Error sending file: {e}")


# ─────────────────────────────────────────────
# BULK DELETE HELPERS
# ─────────────────────────────────────────────

async def show_bulk_delete_menu(message, client, user_id, page=1, category=None):
    items_per_page = 10

    if category:
        all_files = []
        for fid, info in stored_files.items():
            ext = os.path.splitext(info.get('name', ''))[1].lower()
            if get_file_category(ext) == category:
                all_files.append((fid, info))
        all_files.sort(key=lambda x: x[1].get('date', 0), reverse=True)
    else:
        all_files = get_sorted_files()
        all_files.sort(key=lambda x: x[1].get('date', 0), reverse=True)

    total = len(all_files)
    if total == 0:
        await message.reply_text("📁 No files to delete.")
        return

    total_pages = max(1, (total + items_per_page - 1) // items_per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * items_per_page
    end = min(start + items_per_page, total)
    page_files = all_files[start:end]

    selected = pending_deletes.get(user_id, [])

    buttons = []
    for fid, info in page_files:
        name = os.path.basename(info.get('name', 'Unknown'))
        is_sel = fid in selected
        tick = "✅ " if is_sel else "☐ "
        cb = f"bulk_toggle_{fid}_{page}" + (f"_{category}" if category else "")
        buttons.append([InlineKeyboardButton(f"{tick}{name}", callback_data=cb)])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"bulk_page_{page-1}" + (f"_{category}" if category else "")))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"bulk_page_{page+1}" + (f"_{category}" if category else "")))
    if nav:
        buttons.append(nav)

    sel_count = len(selected)
    buttons.append([
        InlineKeyboardButton(f"🗑️ Delete {sel_count} selected", callback_data="bulk_delete_confirm") if sel_count else InlineKeyboardButton("☐ Select files above", callback_data="noop"),
        InlineKeyboardButton("❌ Cancel", callback_data="bulk_delete_cancel")
    ])

    if category:
        buttons.append([InlineKeyboardButton("🔙 Back to Category", callback_data=f"category_{category}")])
    else:
        buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])

    header = (
        f"🗑️ **BULK DELETE** — Page {page}/{total_pages}\n"
        f"✅ Selected: {sel_count} file{'s' if sel_count != 1 else ''}\n\n"
        f"_Tap a file to select/deselect it._"
    )
    await message.reply_text(header, reply_markup=InlineKeyboardMarkup(buttons))


# ─────────────────────────────────────────────
# UPLOAD HANDLER (with hash detection and animations)
# ─────────────────────────────────────────────

@app.on_message(filters.private & (filters.document | filters.photo | filters.video | filters.audio))
async def handle_upload(client, message):
    """Enqueue the upload — one file at a time."""
    if not is_owner(message.from_user.id):
        return

    global _upload_queue, _upload_queue_running

    if _upload_queue is None:
        _upload_queue = asyncio.Queue()

    qsize_before = _upload_queue.qsize()
    await _upload_queue.put((client, message))

    if qsize_before > 0:
        pos = qsize_before + 1
        await message.reply_text(
            f"📋 **Queued — position {pos}**\n"
            f"⏳ Waiting for your confirmation on the previous file…\n"
            f"_Files are processed one at a time._"
        )

    if not _upload_queue_running:
        asyncio.create_task(_upload_queue_worker())


async def _upload_queue_worker():
    """Drain the upload queue sequentially."""
    global _upload_queue_running
    _upload_queue_running = True
    try:
        while _upload_queue and not _upload_queue.empty():
            client, message = await _upload_queue.get()
            try:
                await _process_single_upload(client, message)
            except Exception as e:
                import logging
                logging.exception(f"Queue worker error: {e}")
            finally:
                _upload_queue.task_done()
    finally:
        _upload_queue_running = False


async def _process_single_upload(client, message):
    """Process one upload — the original handle_upload logic."""
    token = secrets.token_hex(8)
    processing_msg = await message.reply_text("📥 **Uploading…**\n⬜⬜⬜ Step 1/3 — Receiving file…")

    try:
        # Extract file info
        if message.document:
            file_obj = message.document
            file_name = file_obj.file_name or "unknown_file"
        elif message.photo:
            file_obj = message.photo
            file_name = f"photo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        elif message.video:
            file_obj = message.video
            file_name = file_obj.file_name or f"video_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
        elif message.audio:
            file_obj = message.audio
            file_name = file_obj.file_name or f"audio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp3"
        else:
            await processing_msg.edit_text("❌ Unsupported file type.")
            return

        file_id = file_obj.file_id
        file_unique_id = file_obj.file_unique_id
        file_size = file_obj.file_size

        # Store the upload data immediately (before any duplicate check)
        upload_data = {
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "file_name": file_name,
            "file_size": file_size,
            "user_id": message.from_user.id,
            "hash": None
        }
        pending_all_uploads[token] = upload_data

        await asyncio.sleep(0.8)
        await processing_msg.edit_text("🔍 **Uploading…**\n🟩⬜⬜ Step 2/3 — Checking for duplicates…")

        # Compute hash (if small enough)
        if file_size <= MAX_HASH_SIZE:
            upload_data['hash'] = await compute_file_hash(client, file_id, file_size)

        # Check duplicates in already stored files
        content_dup, name_dup_info = check_duplicate_details(
            file_name, file_id, file_unique_id, upload_data['hash']
        )

        # Check duplicates among other pending uploads
        pending_name_dup = False
        pending_content_dup = False
        for tok, data in pending_all_uploads.items():
            if tok == token:
                continue
            if data['file_name'] == file_name:
                pending_name_dup = True
            if data.get('hash') and upload_data['hash'] and data['hash'] == upload_data['hash']:
                pending_content_dup = True

        print(f"[DEBUG] {file_name} -> content_dup={content_dup}, name_dup_info={bool(name_dup_info)}, pending_name={pending_name_dup}, pending_content={pending_content_dup}")

        # --- Content duplicate (stored or pending) ---
        if content_dup or pending_content_dup:
            # Move to the dedicated pending_content_dups dict (so the callback knows it's a content duplicate)
            pending_content_dups[token] = upload_data

            # Find the existing file info for display (from stored if available)
            existing_info = None
            for info in stored_files.values():
                if (upload_data['hash'] and info.get('hash') == upload_data['hash']) or info.get('file_unique_id') == file_unique_id:
                    existing_info = info
                    break
            existing_name = os.path.basename(existing_info.get('name', 'Unknown')) if existing_info else "Unknown"
            existing_size = format_file_size(existing_info.get('size', 0)) if existing_info else "Unknown"
            existing_date = format_date(existing_info.get('date', 0)) if existing_info else "Unknown"

            _event = asyncio.Event()
            _upload_confirm_events[token] = _event
            await processing_msg.edit_text(
                f"🔴 **Content Duplicate Detected!**\n\n"
                f"📄 **Your file:** `{file_name}`\n"
                f"🔁 **Existing file:** `{existing_name}`\n"
                f"📏 {existing_size} · 📅 {existing_date}\n\n"
                f"Would you like to upload it anyway?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Yes, upload anyway", callback_data=f"confirm_content_{token}"),
                     InlineKeyboardButton("❌ No, cancel", callback_data=f"cancel_content_{token}")]
                ])
            )
            try:
                await asyncio.wait_for(_event.wait(), timeout=600)
            except asyncio.TimeoutError:
                pass
            _upload_confirm_events.pop(token, None)
            return

        # --- Filename duplicate (stored or pending) ---
        if name_dup_info or pending_name_dup:
            pending_filename_dups[token] = upload_data

            _event = asyncio.Event()
            _upload_confirm_events[token] = _event
            await processing_msg.edit_text(
                f"⚠️ **Duplicate Name Detected!**\n"
                f"A file named `{file_name}` already exists.\n\n"
                f"Would you like to rename the new file and upload it anyway?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Yes, rename and upload", callback_data=f"dup_rename_{token}")],
                    [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_content_{token}")]
                ])
            )
            try:
                await asyncio.wait_for(_event.wait(), timeout=600)
            except asyncio.TimeoutError:
                pass
            _upload_confirm_events.pop(token, None)
            return

        # --- No duplicate: store directly ---
        await processing_msg.edit_text("💾 **Uploading…**\n🟩🟩⬜ Step 3/3 — Saving to storage…")
        await asyncio.sleep(0.6)

        fid = secrets.token_hex(4)
        ext = os.path.splitext(file_name)[1].lower()

        stored_files[fid] = {
            "tg_file_id": file_id,
            "file_unique_id": file_unique_id,
            "name": file_name,
            "size": file_size,
            "date": datetime.now().timestamp(),
            "ext": ext,
            "favourite": False,
            "hash": upload_data['hash']
        }
        save_storage(stored_files)

        # Clean up pending entries
        pending_all_uploads.pop(token, None)

        await processing_msg.delete()
        await message.reply_text(
            f"✅ **Stored Successfully!**\n\n"
            f"📄 **File:** `{file_name}`\n"
            f"📏 **Size:** {format_file_size(file_size)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                 InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{fid}"),
                 InlineKeyboardButton("⭐ Fav", callback_data=f"fav_{fid}")],
                [InlineKeyboardButton("📤 Upload More", callback_data="upload"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
            ])
        )

    except Exception as e:
        logging.exception("Upload error")
        pending_all_uploads.pop(token, None)
        await processing_msg.edit_text(f"❌ **Error while uploading:**\n`{str(e)}`")

# ─────────────────────────────────────────────
# CALLBACKS FOR DUPLICATE CONFIRMATIONS
# ─────────────────────────────────────────────

@app.on_callback_query(filters.regex(r"^dup_rename_"))
async def handle_dup_rename(client, query: CallbackQuery):
    token = query.data.replace("dup_rename_", "")
    upload_data = pending_filename_dups.pop(token, None)

    if not upload_data:
        await query.answer("❌ Upload session expired. Please send the file again.", show_alert=True)
        return

    if upload_data['user_id'] != query.from_user.id:
        await query.answer("🚫 This is not your upload session.", show_alert=True)
        return

    await query.message.edit_text("🔄 Processing and renaming...")

    try:
        existing_names = get_existing_names_set()
        new_name = generate_unique_name(upload_data['file_name'], existing_names)

        fid = secrets.token_hex(4)
        ext = os.path.splitext(new_name)[1].lower()

        stored_files[fid] = {
            "tg_file_id": upload_data['file_id'],
            "file_unique_id": upload_data['file_unique_id'],
            "name": new_name,
            "size": upload_data['file_size'],
            "date": datetime.now().timestamp(),
            "ext": ext,
            "favourite": False,
            "hash": upload_data.get('hash')
        }
        save_storage(stored_files)

        await query.message.edit_text(
            f"✅ **Stored Success (Renamed)!**\n\n"
            f"📄 **Original:** `{upload_data['file_name']}`\n"
            f"📝 **New Name:** `{new_name}`\n"
            f"📏 **Size:** {format_file_size(upload_data['file_size'])}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                 InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{fid}"),
                 InlineKeyboardButton("⭐ Fav", callback_data=f"fav_{fid}")],
                [InlineKeyboardButton("📤 Upload More", callback_data="upload"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
            ])
        )
        await query.answer("File saved!")
        _ev = _upload_confirm_events.pop(token, None)
        if _ev: _ev.set()
    except Exception as e:
        await query.message.edit_text(f"❌ **Error while saving:**\n`{str(e)}`")
        await query.answer("Error", show_alert=True)
        _ev = _upload_confirm_events.pop(token, None)
        if _ev: _ev.set()


@app.on_callback_query(filters.regex(r"^confirm_content_"))
async def handle_confirm_content(client, query: CallbackQuery):
    token = query.data.replace("confirm_content_", "")
    upload_data = pending_content_dups.pop(token, None)

    if not upload_data:
        await query.answer("❌ Upload session expired. Please send the file again.", show_alert=True)
        return

    if upload_data['user_id'] != query.from_user.id:
        await query.answer("🚫 This is not your upload session.", show_alert=True)
        return

    await query.message.edit_text("💾 Saving...")

    try:
        fid = secrets.token_hex(4)
        ext = os.path.splitext(upload_data['file_name'])[1].lower()

        stored_files[fid] = {
            "tg_file_id": upload_data['file_id'],
            "file_unique_id": upload_data['file_unique_id'],
            "name": upload_data['file_name'],
            "size": upload_data['file_size'],
            "date": datetime.now().timestamp(),
            "ext": ext,
            "favourite": False,
            "hash": upload_data.get('hash')
        }
        save_storage(stored_files)

        await query.message.edit_text(
            f"✅ **Stored Successfully!**\n\n"
            f"📄 **File:** `{upload_data['file_name']}`\n"
            f"📏 **Size:** {format_file_size(upload_data['file_size'])}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                 InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{fid}"),
                 InlineKeyboardButton("⭐ Fav", callback_data=f"fav_{fid}")],
                [InlineKeyboardButton("📤 Upload More", callback_data="upload"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
            ])
        )
        await query.answer("File saved!")
        _ev = _upload_confirm_events.pop(token, None)
        if _ev: _ev.set()
    except Exception as e:
        await query.message.edit_text(f"❌ **Error while saving:**\n`{str(e)}`")
        await query.answer("Error", show_alert=True)
        _ev = _upload_confirm_events.pop(token, None)
        if _ev: _ev.set()


@app.on_callback_query(filters.regex(r"^cancel_content_"))
async def handle_cancel_content(client, query: CallbackQuery):
    token = query.data.replace("cancel_content_", "")
    pending_content_dups.pop(token, None)
    await query.message.edit_text(
        "❌ **Upload cancelled.**\n\nWhat would you like to do next?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Upload More", callback_data="upload")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
        ])
    )
    _ev = _upload_confirm_events.pop(token, None)
    if _ev: _ev.set()

@app.on_callback_query(filters.regex(r"^view_duplicate_"))
async def handle_view_duplicate(client, query: CallbackQuery):
    fid = query.data.replace("view_duplicate_", "")
    info = stored_files.get(fid)
    if not info:
        await query.answer("File not found", show_alert=True)
        return
    # Show file with a back button
    file_name = os.path.basename(info.get('name', 'Unknown'))
    size_str = format_file_size(info.get('size', 0))
    caption = f"👁️ **Duplicate file preview**\n\n📄 **{file_name}**\n📏 {size_str}"
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Duplicates", callback_data="duplicates")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ])
    try:
        await send_file_smart(client, query.message.chat.id, info, caption, buttons)
        # Optional: delete the duplicates message to keep the chat clean
        await query.message.delete()
    except Exception as e:
        await query.message.reply_text(f"❌ Error viewing file: {e}")
    await query.answer()

# ─────────────────────────────────────────────
# MAIN CALLBACK HANDLER (all the rest)
# ─────────────────────────────────────────────

@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    if not is_owner(callback_query.from_user.id):
        await callback_query.answer("🔒 Private bot.", show_alert=True)
        return
    data = callback_query.data
    msg = callback_query.message
    user_id = callback_query.from_user.id

    # Quick answer for tag actions
    if data.startswith("tag_cancel_"):
        await callback_query.answer("❌ Tag cancelled", show_alert=True)
    elif data.startswith("tag_clear_"):
        await callback_query.answer("🏷️ Tag cleared", show_alert=True)
    else:
        await callback_query.answer()

    if data == "noop":
        return

    # ── Main menu, upload, gallery, categories, etc. ────────────────────
    if data == "main_menu":
        await msg.reply_text(
            f"🏠 **Main Menu**\n📁 Total: `{len(stored_files)}` files",
            reply_markup=get_inline_menu()
        )
    elif data == "upload":
        await msg.reply_text(f"📤 Send me any file!\n💾 Stored: `{len(stored_files)}`")
    elif data.startswith("gallery_"):
        page = int(data.split("_")[1])
        await show_gallery_page(msg, client, page)
    elif data == "categories":
        await show_categories(msg, client)
    elif data.startswith("category_"):
        category = data[len("category_"):]
        await show_category_page(msg, client, category, page=1)
    elif data.startswith("catpage_"):
        parts = data.split("_")
        page = int(parts[-1])
        category = "_".join(parts[1:-1])
        await show_category_page(msg, client, category, page)
    elif data == "search":
        await msg.reply_text("🔍 **Search**\n\nType a filename or keyword.")
    elif data == "help":
        await msg.reply_text(
            f"ℹ️ **Help & Guide**\n"
            f"{chr(8212)*32}\n\n"
            f"📤 **Uploading**\n"
            f"Send any file — photo, video, audio or document.\n"
            f"Stored on **Telegram's cloud**, not your device.\n\n"
            f"🖼️ **Gallery** — 10-file album grid, sort by date/name/size\n"
            f"📂 **Categories** — browse by file type with numbered pagination\n"
            f"🔍 **Search** — find files by keyword\n"
            f"⭐ **Favourites** — star & browse starred files\n"
            f"🔁 **Duplicates** — detect by content 🔴 or name 🟡, keep newest\n\n"
            f"{chr(8212)*32}\n"
            f"🔘 **File Actions** (tap 👁️ View on any file)\n"
            f"👁️ View — preview file\n"
            f"🔗 Link — shareable link + inline preview\n"
            f"⏰ Expiring Link — auto-expires in 1h / 6h / 1d / 7d / 30d\n"
            f"☆ Fav — star a file\n"
            f"✏️ Rename — rename any file\n"
            f"🏷️ Tag — set a custom tag/category\n"
            f"🗑️ Delete — moves file to Recycle Bin\n"
            f"🗑️ Bulk Delete — checkbox-select multiple files (per category or all)\n\n"
            f"{chr(8212)*32}\n"
            f"♻️ **Recycle Bin**\n"
            f"• View · Restore · Delete Forever · Empty Bin\n"
            f"• ♻️ **Bulk Restore** — checkbox-select files to restore\n"
            f"• ✅ Select All button per page\n"
            f"• Auto-purged after **{RECYCLE_TTL_DAYS} days**\n\n"
            f"{chr(8212)*32}\n"
            f"🛡️ **Security**\n"
            f"Owner-only access · malicious file detection\n"
            f"Expiring links · /cleanup removes ghost records\n\n"
            f"{chr(8212)*32}\n"
            f"☁️ Files on **Telegram cloud** — zero local disk usage\n"
            f"📁 Active: `{len(stored_files)}` · ⭐ Favs: `{sum(1 for i in stored_files.values() if i.get('favourite'))}` · 🏷️ Tagged: `{sum(1 for i in stored_files.values() if i.get('tag'))}` · ♻️ Bin: `{len(recycle_bin)}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
        )

    # ── File actions (view, link, delete, etc.) ───────────────────────────
    elif data.startswith("view_"):
        fid = data[5:]
        await view_file(client, msg, fid)
    elif data.startswith("link_"):
        fid = data[5:]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        file_name = info.get('name', 'Unknown')
        clean_name = os.path.basename(file_name)
        link_url = f"https://t.me/{client.me.username}?start=file_{fid}"
        size_str = format_file_size(info.get('size', 0))
        ext = info.get('ext', os.path.splitext(file_name)[1]).lower()
        icon = get_file_icon(ext)
        caption = f"{icon} **{clean_name}**\n📏 {size_str}\n\n🔗 `{link_url}`"
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Copy Link", url=link_url),
             InlineKeyboardButton("🗑️ Delete",    callback_data=f"delete_{fid}")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ])
        try:
            await send_file_smart(client, msg.chat.id, info, caption, buttons)
        except Exception as e:
            await msg.reply_text(f"⚠️ Preview failed: {e}\n\n🔗 `{link_url}`", reply_markup=buttons)
    elif data.startswith("delete_"):
        fid = data[7:]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        name = os.path.basename(info.get('name', 'Unknown'))
        await msg.reply_text(
            f"🗑️ **Delete File?**\n\n📄 `{name}`\n\nThis action cannot be undone.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, Delete", callback_data=f"del_confirm_{fid}"),
                InlineKeyboardButton("❌ No, Keep", callback_data=f"del_cancel_{fid}")
            ]])
        )
    elif data.startswith("del_confirm_"):
        fid = data[len("del_confirm_"):]
        info = stored_files.pop(fid, None)
        if info:
            info['deleted_at'] = datetime.now().timestamp()
            recycle_bin[fid] = info
            save_storage(stored_files)
            save_recycle_bin(recycle_bin)
            name = os.path.basename(info.get('name', 'Unknown'))
            await msg.reply_text(
                f"🗑️ **Moved to Recycle Bin:**\n`{name}`\n\n"
                f"💾 Remaining: `{len(stored_files)}` files\n"
                f"♻️ Recycle Bin: `{len(recycle_bin)}` files",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("♻️ View Recycle Bin", callback_data="recycle_bin_1"),
                    InlineKeyboardButton("🏠 Main Menu",        callback_data="main_menu"),
                ]])
            )
        else:
            await msg.reply_text("❌ File not found.")
    elif data.startswith("del_cancel_"):
        await msg.edit_text("❌ Deletion cancelled.")
    # ── Bulk delete (with category support) ────────────────────────────
    elif data == "bulk_delete_start":
        pending_deletes[user_id] = []
        pending_bulk_delete_category[user_id] = None
        await show_bulk_delete_menu(msg, client, user_id, page=1, category=None)
    elif data.startswith("bulk_delete_start_cat_"):
        category = data[len("bulk_delete_start_cat_"):]
        pending_deletes[user_id] = []
        pending_bulk_delete_category[user_id] = category
        await show_bulk_delete_menu(msg, client, user_id, page=1, category=category)
    elif data.startswith("bulk_page_"):
        parts = data.split("_")
        if len(parts) == 3:
            page = int(parts[2])
            category = None
        else:
            page = int(parts[2])
            category = parts[3]
        await show_bulk_delete_menu(msg, client, user_id, page, category)
    elif data.startswith("bulk_toggle_"):
        parts = data.split("_")
        if len(parts) == 4:
            page = int(parts[3])
            fid = parts[2]
            category = None
        else:
            page = int(parts[3])
            fid = parts[2]
            category = parts[4]
        selected = pending_deletes.setdefault(user_id, [])
        if fid in selected:
            selected.remove(fid)
        else:
            selected.append(fid)
        pending_deletes[user_id] = selected
        await show_bulk_delete_menu(msg, client, user_id, page, category)
    elif data == "bulk_delete_confirm":
        selected = pending_deletes.get(user_id, [])
        if not selected:
            await msg.reply_text("☐ No files selected!")
            return
        names = [os.path.basename(stored_files[fid].get('name', fid)) for fid in selected if fid in stored_files]
        name_list = "\n".join(f"• {n}" for n in names[:10])
        if len(names) > 10:
            name_list += f"\n• ... and {len(names)-10} more"
        await msg.reply_text(
            f"⚠️ **Confirm Bulk Delete**\n\n"
            f"You are about to delete **{len(selected)} file(s)**:\n\n{name_list}\n\n"
            f"This cannot be undone!",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ Delete {len(selected)} files", callback_data="bulk_delete_execute"),
                InlineKeyboardButton("❌ Cancel", callback_data="bulk_delete_cancel")
            ]])
        )
    elif data == "bulk_delete_execute":
        selected = pending_deletes.pop(user_id, [])
        moved = 0
        for fid in selected:
            info = stored_files.pop(fid, None)
            if info:
                info['deleted_at'] = datetime.now().timestamp()
                recycle_bin[fid] = info
                moved += 1
        save_storage(stored_files)
        save_recycle_bin(recycle_bin)
        category = pending_bulk_delete_category.pop(user_id, None)
        await msg.reply_text(
            f"🗑️ **Moved to Recycle Bin!**\n\n"
            f"♻️ Moved: `{moved}` file(s)\n"
            f"💾 Remaining: `{len(stored_files)}` files",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ View Recycle Bin", callback_data="recycle_bin_1"),
                InlineKeyboardButton("🏠 Main Menu",        callback_data="main_menu"),
            ]])
        )
        if category:
            await msg.reply_text(
                f"🔙 Return to **{category}** category?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👆 Yes", callback_data=f"category_{category}")
                ]])
            )
    elif data == "bulk_delete_cancel":
        pending_deletes.pop(user_id, None)
        category = pending_bulk_delete_category.pop(user_id, None)
        buttons = []
        if category:
            buttons.append([InlineKeyboardButton("🔙 Back to Category", callback_data=f"category_{category}")])
        buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
        await msg.reply_text("❌ Bulk delete cancelled.", reply_markup=InlineKeyboardMarkup(buttons))

    # ── Recycle Bin ─────────────────────────────────────────────────────
    elif data.startswith("recycle_bin_"):
        page = int(data.split("_")[-1])
        await show_recycle_categories(msg, client, page)
    elif data.startswith("rb_cat_"):
        parts = data.split("_")
        page = int(parts[-1])
        category = "_".join(parts[2:-1])
        await show_recycle_category_page(msg, client, category, page)
    elif data.startswith("rb_view_"):
        fid = data[8:]
        await view_recycle_file(client, msg, fid)
    elif data.startswith("rb_restore_ask_"):
        fid = data[len("rb_restore_ask_"):]
        info = recycle_bin.get(fid)
        if not info:
            await msg.edit_text("❌ File not found in recycle bin.")
            return
        name = os.path.basename(info.get('name', 'Unknown'))
        await msg.edit_text(
            f"♻️ **Restore File?**\n\n📄 `{name}`\n\nMove back to your storage?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, Restore", callback_data=f"rb_restore_confirm_{fid}"),
                InlineKeyboardButton("❌ Cancel",       callback_data=f"rb_restore_cancel_{fid}"),
            ]])
        )
    elif data.startswith("rb_restore_confirm_"):
        fid = data[len("rb_restore_confirm_"):]
        info = recycle_bin.pop(fid, None)
        if info:
            info.pop('deleted_at', None)
            stored_files[fid] = info
            save_storage(stored_files)
            save_recycle_bin(recycle_bin)
            name = os.path.basename(info.get('name', 'Unknown'))
            restored_str = format_date(datetime.now().timestamp())
            await msg.reply_text(
                f"✅ **Restored!**\n\n📄 `{name}` is back in your storage.\n"
                f"♻️ Restored at: {restored_str}\n"
                f"💾 Total files: `{len(stored_files)}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("♻️ Recycle Bin", callback_data="recycle_bin_1"),
                    InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
                ]])
            )
        else:
            await msg.reply_text("❌ File not found in recycle bin.")
    elif data.startswith("rb_restore_cancel_"):
        fid = data[len("rb_restore_cancel_"):]
        info = recycle_bin.get(fid)
        name = os.path.basename(info.get('name', 'Unknown')) if info else "Unknown"
        await msg.reply_text(
            f"❌ Restore cancelled.\n\n📄 `{name}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ Restore",       callback_data=f"rb_restore_ask_{fid}"),
                InlineKeyboardButton("🗑️ Del Forever",   callback_data=f"rb_permdelete_{fid}"),
                InlineKeyboardButton("🔙 Recycle Bin",    callback_data="recycle_bin_1"),
            ]])
        )
    elif data.startswith("rb_restore_"):
        fid = data[len("rb_restore_"):]
        info = recycle_bin.get(fid)
        if not info:
            await msg.reply_text("❌ File not found in recycle bin.")
            return
        name = os.path.basename(info.get('name', 'Unknown'))
        await msg.reply_text(
            f"♻️ **Restore File?**\n\n📄 `{name}`\n\nMove back to your storage?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, Restore", callback_data=f"rb_restore_confirm_{fid}"),
                InlineKeyboardButton("❌ Cancel",       callback_data=f"rb_restore_cancel_{fid}"),
            ]])
        )
    elif data.startswith("rb_permdelete_confirm_"):
        fid = data[len("rb_permdelete_confirm_"):]
        info = recycle_bin.pop(fid, None)
        if info:
            save_recycle_bin(recycle_bin)
            name = os.path.basename(info.get('name', 'Unknown'))
            await msg.edit_text(
                f"🗑️ **Permanently Deleted:** `{name}`\n♻️ Recycle Bin: `{len(recycle_bin)}` files",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("♻️ Recycle Bin", callback_data="recycle_bin_1"),
                    InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
                ]])
            )
        else:
            await msg.edit_text("❌ File not found.")
    elif data.startswith("rb_permdelete_cancel_"):
        await msg.edit_text("❌ Cancelled.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ Back to Recycle Bin", callback_data="recycle_bin_1"),
            ]])
        )
    elif data.startswith("rb_permdelete_"):
        fid = data[len("rb_permdelete_"):]
        info = recycle_bin.get(fid)
        if not info:
            await msg.reply_text("❌ File not found in recycle bin.")
            return
        name = os.path.basename(info.get('name', 'Unknown'))
        await msg.reply_text(
            f"⚠️ **Permanently Delete?**\n\n📄 `{name}`\n\n"
            f"This will erase the file from disk. **Cannot be undone!**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, Delete Forever", callback_data=f"rb_permdelete_confirm_{fid}"),
                InlineKeyboardButton("❌ Cancel",              callback_data=f"rb_permdelete_cancel_{fid}"),
            ]])
        )
    elif data == "rb_empty_ask":
        if not recycle_bin:
            await msg.reply_text("♻️ Recycle Bin is already empty!")
            return
        await msg.reply_text(
            f"⚠️ **Empty Recycle Bin?**\n\n"
            f"This will permanently delete all `{len(recycle_bin)}` file(s).\n"
            f"**This cannot be undone!**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, Empty All", callback_data="rb_empty_confirm"),
                InlineKeyboardButton("❌ Cancel",         callback_data="rb_empty_cancel"),
            ]])
        )
    elif data == "rb_empty_confirm":
        count = len(recycle_bin)
        recycle_bin.clear()
        save_recycle_bin(recycle_bin)
        await msg.reply_text(
            f"🗑️ **Recycle Bin Emptied!**\n\n`{count}` file(s) permanently deleted.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
        )
    elif data == "rb_empty_cancel":
        await msg.edit_text("❌ Cancelled.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ Back to Recycle Bin", callback_data="recycle_bin_1"),
            ]])
        )

    # ── Bulk Restore ─────────────────────────────────────────────────────
    elif data == "rb_bulk_restore_start":
        pending_restores[user_id] = []
        pending_bulk_category[user_id] = None
        await show_bulk_restore_menu(msg, client, user_id, page=1)
    elif data.startswith("rb_bulk_restore_start_cat_"):
        category = data[len("rb_bulk_restore_start_cat_"):]
        pending_restores[user_id] = []
        pending_bulk_category[user_id] = category
        await show_bulk_restore_menu(msg, client, user_id, page=1)
    elif data.startswith("rb_bulk_page_"):
        page = int(data.split("_")[-1])
        await show_bulk_restore_menu(msg, client, user_id, page)
    elif data.startswith("rb_bulk_toggle_"):
        parts = data.split("_")
        page = int(parts[-1])
        fid = "_".join(parts[3:-1])
        selected = pending_restores.setdefault(user_id, [])
        if fid in selected:
            selected.remove(fid)
        else:
            selected.append(fid)
        pending_restores[user_id] = selected
        await show_bulk_restore_menu(msg, client, user_id, page)
    elif data.startswith("rb_bulk_select_all_"):
        page = int(data.split("_")[-1])
        category = pending_bulk_category.get(user_id)
        if category:
            all_files = []
            for fid, info in recycle_bin.items():
                ext = os.path.splitext(info.get('name', ''))[1].lower()
                if get_file_category(ext) == category:
                    all_files.append((fid, info))
            all_files.sort(key=lambda x: x[1].get('deleted_at', 0), reverse=True)
        else:
            all_files = list(recycle_bin.items())
            all_files.sort(key=lambda x: x[1].get('deleted_at', 0), reverse=True)
        items_per_page = 10
        total_pages = max(1, (len(all_files) + items_per_page - 1) // items_per_page)
        page = max(1, min(page, total_pages))
        start = (page - 1) * items_per_page
        end = min(start + items_per_page, len(all_files))
        page_files = all_files[start:end]
        selected = pending_restores.setdefault(user_id, [])
        for fid, _ in page_files:
            if fid not in selected:
                selected.append(fid)
        pending_restores[user_id] = selected
        await show_bulk_restore_menu(msg, client, user_id, page)
    elif data == "rb_bulk_restore_confirm":
        selected = pending_restores.get(user_id, [])
        if not selected:
            await msg.reply_text("☐ No files selected!")
            return
        names = [os.path.basename(recycle_bin[fid].get('name', fid)) for fid in selected if fid in recycle_bin]
        name_list = "\n".join(f"• {n}" for n in names[:10])
        if len(names) > 10:
            name_list += f"\n• ... and {len(names)-10} more"
        await msg.reply_text(
            f"⚠️ **Confirm Bulk Restore**\n\n"
            f"You are about to restore **{len(selected)} file(s)**:\n\n{name_list}\n\n"
            f"Files will be moved back to your storage.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ Restore {len(selected)} files", callback_data="rb_bulk_restore_execute"),
                InlineKeyboardButton("❌ Cancel", callback_data="rb_bulk_restore_cancel")
            ]])
        )
    elif data == "rb_bulk_restore_execute":
        selected = pending_restores.pop(user_id, [])
        restored = 0
        for fid in selected:
            info = recycle_bin.pop(fid, None)
            if info:
                info.pop('deleted_at', None)
                stored_files[fid] = info
                restored += 1
        save_storage(stored_files)
        save_recycle_bin(recycle_bin)
        category = pending_bulk_category.pop(user_id, None)
        await msg.reply_text(
            f"✅ **Bulk Restore Complete!**\n\n"
            f"♻️ Restored: `{restored}` file(s)\n"
            f"💾 Active files: `{len(stored_files)}`\n"
            f"🗑️ Remaining in bin: `{len(recycle_bin)}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ Recycle Bin", callback_data="recycle_bin_1"),
                InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
            ]])
        )
        if category:
            await msg.reply_text(
                f"🔙 Return to **{category}** category?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👆 Yes", callback_data=f"rb_cat_{category}_1")
                ]])
            )
    elif data == "rb_bulk_restore_cancel":
        pending_restores.pop(user_id, None)
        pending_bulk_category.pop(user_id, None)
        await msg.reply_text("❌ Bulk restore cancelled.")

    # ── Cleanup ─────────────────────────────────────────────────────────
    elif data == "cleanup_confirm":
        missing = [fid for fid, i in stored_files.items() if not i.get('tg_file_id')]
        for fid in missing:
            stored_files.pop(fid, None)
        save_storage(stored_files)
        await msg.reply_text(
            f"🧹 **Cleanup complete!**\n\n"
            f"🗑️ Removed `{len(missing)}` ghost record(s)\n"
            f"💾 Remaining: `{len(stored_files)}` files"
        )
    elif data == "cleanup_cancel":
        await msg.reply_text("❌ Cleanup cancelled.")

    # ── Sort preferences ────────────────────────────────────────────────
    elif data.startswith("sort_"):
        pref = data[5:]
        sort_pref[user_id] = pref
        label = {'name': '🔤 Name', 'size': '💾 Size', 'date': '🕐 Date'}.get(pref, '🕐 Date')
        await msg.reply_text(
            f"✅ Gallery sorted by **{label}**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🖼️ Gallery", callback_data="gallery_1"),
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
        )
    elif data.startswith("favourites_"):
        page = int(data.split("_")[1])
        await show_favourites_page(msg, client, page, user_id)
    elif data.startswith("fav_"):
        fid = data[4:]
        info = stored_files.get(fid)
        if info:
            info['favourite'] = not info.get('favourite', False)
            save_storage(stored_files)
            state = "⭐ Added to Favourites!" if info['favourite'] else "☆ Removed from Favourites"
            name = os.path.basename(info.get('name',''))
            await msg.reply_text(
                f"{state}\n\n📄 `{name}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👁️ View",       callback_data=f"view_{fid}"),
                    InlineKeyboardButton("⭐ Favourites", callback_data="favourites_1"),
                    InlineKeyboardButton("🏠 Main Menu",  callback_data="main_menu"),
                ]])
            )
        else:
            await msg.reply_text("❌ File not found.")
    elif data.startswith("rename_cancel_"):
        rename_pending.pop(user_id, None)
        await msg.edit_text("❌ Rename cancelled.")
    elif data.startswith("rename_"):
        fid = data[7:]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        rename_pending[user_id] = fid
        name = os.path.basename(info.get('name',''))
        await msg.reply_text(
            f"✏️ **Rename File**\n\n"
            f"Current: `{name}`\n\n"
            f"Send me the new filename (extension will be kept automatically):",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data=f"rename_cancel_{fid}")
            ]])
        )
    elif data.startswith("tag_clear_"):
        fid = data[len("tag_clear_"):]
        user_state.pop(str(user_id), None)
        user_state.pop(str(user_id) + '_fid', None)
        if fid in stored_files:
            stored_files[fid].pop('tag', None)
            save_storage(stored_files)
            file_name = os.path.basename(stored_files[fid].get('name', 'Unknown'))
            await msg.reply_text(
                f"🧹 **Tag cleared!**\n\n"
                f"📄 File: `{file_name}`\n"
                f"Tag has been removed.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                    InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
                ]])
            )
        else:
            await msg.reply_text("❌ File not found.")
    elif data.startswith("tag_cancel_"):
        fid = data[len("tag_cancel_"):]
        user_state.pop(str(user_id), None)
        user_state.pop(str(user_id) + '_fid', None)
        file_name = os.path.basename(stored_files.get(fid, {}).get('name', 'Unknown'))
        await msg.reply_text(
            f"❌ **Tag cancelled**\n\n"
            f"📄 File: `{file_name}`\n"
            f"No changes were made.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
            ]])
        )
    elif data.startswith("tag_"):
        fid = data[4:]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        current_tag = info.get('tag', '')
        file_name = os.path.basename(info.get('name', 'Unknown'))
        user_state[str(user_id)] = 'awaiting_tag'
        user_state[str(user_id) + '_fid'] = fid
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧹 Clear Tag", callback_data=f"tag_clear_{fid}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"tag_cancel_{fid}")]
        ])
        await msg.reply_text(
            f"🏷️ **Set Tag**\n\n"
            f"📄 File: `{file_name}`\n"
            f"🏷️ Current tag: `{current_tag if current_tag else 'None'}`\n\n"
            f"Please type and send the new tag for this file:",
            reply_markup=keyboard
        )
    elif data == "duplicates":
        await show_duplicates(msg, client)
    # ── New duplicate management handlers ───────────────────────────────────
    elif data.startswith("dedup_keepnewest_"):
        label = data[len("dedup_keepnewest_"):]
        # Rebuild duplicate groups
        by_uid = {}
        by_name = {}
        for fid, info in stored_files.items():
            uid = info.get('file_unique_id', '')
            name = os.path.basename(info.get('name', '')).lower()
            if uid:
                by_uid.setdefault(uid, []).append((fid, info))
            by_name.setdefault(name, []).append((fid, info))
        seen_fids = set()
        dupe_groups = {}
        for uid, entries in by_uid.items():
            if len(entries) > 1:
                group_label = f"[same content] {os.path.basename(entries[0][1].get('name',''))}"
                dupe_groups[group_label] = entries
                for fid, _ in entries:
                    seen_fids.add(fid)
        for name, entries in by_name.items():
            if len(entries) > 1 and not all(fid in seen_fids for fid, _ in entries):
                group_label = f"[same name] {name}"
                dupe_groups[group_label] = entries

        # Find the group that matches the label (exact match)
        target_group = None
        for group_label, entries in dupe_groups.items():
            if group_label == label:
                target_group = entries
                break
        if not target_group:
            await msg.edit_text("❌ Could not find duplicate group.")
            return
        # Sort by date (newest first)
        target_group.sort(key=lambda x: x[1].get('date', 0), reverse=True)
        kept = target_group[0]
        removed = target_group[1:]
        for fid, info in removed:
            stored_files.pop(fid, None)
        save_storage(stored_files)
        await msg.edit_text(
            f"✅ **Deduplicated (Kept newest)!**\n\n"
            f"📄 Kept: `{os.path.basename(kept[1].get('name',''))}` (newest)\n"
            f"🗑️ Removed: `{len(removed)}` older copy/copies",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔁 Check Again", callback_data="duplicates"),
                InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
            ]])
        )
    elif data.startswith("dedup_keepoldest_"):
        label = data[len("dedup_keepoldest_"):]
        # Rebuild duplicate groups (same as above)
        by_uid = {}
        by_name = {}
        for fid, info in stored_files.items():
            uid = info.get('file_unique_id', '')
            name = os.path.basename(info.get('name', '')).lower()
            if uid:
                by_uid.setdefault(uid, []).append((fid, info))
            by_name.setdefault(name, []).append((fid, info))
        seen_fids = set()
        dupe_groups = {}
        for uid, entries in by_uid.items():
            if len(entries) > 1:
                group_label = f"[same content] {os.path.basename(entries[0][1].get('name',''))}"
                dupe_groups[group_label] = entries
                for fid, _ in entries:
                    seen_fids.add(fid)
        for name, entries in by_name.items():
            if len(entries) > 1 and not all(fid in seen_fids for fid, _ in entries):
                group_label = f"[same name] {name}"
                dupe_groups[group_label] = entries

        target_group = None
        for group_label, entries in dupe_groups.items():
            if group_label == label:
                target_group = entries
                break
        if not target_group:
            await msg.edit_text("❌ Could not find duplicate group.")
            return
        # Sort by date (oldest first)
        target_group.sort(key=lambda x: x[1].get('date', 0))
        kept = target_group[0]
        removed = target_group[1:]
        for fid, info in removed:
            stored_files.pop(fid, None)
        save_storage(stored_files)
        await msg.edit_text(
            f"✅ **Deduplicated (Kept oldest)!**\n\n"
            f"📄 Kept: `{os.path.basename(kept[1].get('name',''))}` (oldest)\n"
            f"🗑️ Removed: `{len(removed)}` newer copy/copies",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔁 Check Again", callback_data="duplicates"),
                InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
            ]])
        )
    elif data.startswith("explink_cancel_"):
        fid = data[len("explink_cancel_"):]
        name = os.path.basename(stored_files.get(fid, {}).get('name', ''))
        await msg.edit_text(
            f"❌ **Expiring link cancelled**\n\n📄 `{name}` — no link created.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👁️ View",      callback_data=f"view_{fid}"),
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
        )
    elif data.startswith("explink_"):
        fid = data[8:]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        name = os.path.basename(info.get('name',''))
        await msg.reply_text(
            f"⏰ **Expiring Link for:**\n`{name}`\n\n"
            f"Choose how long the link stays active:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("1 Hour",   callback_data=f"explinkset_{fid}_1h"),
                 InlineKeyboardButton("6 Hours",  callback_data=f"explinkset_{fid}_6h")],
                [InlineKeyboardButton("1 Day",    callback_data=f"explinkset_{fid}_1d"),
                 InlineKeyboardButton("7 Days",   callback_data=f"explinkset_{fid}_7d")],
                [InlineKeyboardButton("30 Days",  callback_data=f"explinkset_{fid}_30d")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"explink_cancel_{fid}"),
                 InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
            ])
        )
    elif data.startswith("explinkset_"):
        parts = data.split("_")
        fid = parts[1]
        dur = parts[2]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        secs = {"1h": 3600, "6h": 21600, "1d": 86400, "7d": 604800, "30d": 2592000}.get(dur, 3600)
        expires_at = datetime.now().timestamp() + secs
        token = secrets.token_urlsafe(12)
        expired_links[token] = {'fid': fid, 'expires_at': expires_at}
        exp_str = format_date(expires_at)
        bot_username = client.me.username
        link = f"https://t.me/{bot_username}?start=el_{token}"
        name = os.path.basename(info.get('name',''))
        await msg.edit_text(
            f"⏰ **Expiring Link Created!**\n\n"
            f"📄 `{name}`\n"
            f"🔗 `{link}`\n\n"
            f"⌛ Expires: **{exp_str}**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Open Link", url=link),
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
        )


# ─────────────────────────────────────────────
# BOT HANDLERS (start, cleanup, text)
# ─────────────────────────────────────────────

@app.on_message(filters.command("start"))
async def start(client, message):
    if not is_owner(message.from_user.id):
        await message.reply_text("🔒 This bot is private.")
        return
    parts = message.text.split()
    if len(parts) > 1:
        arg = parts[1]
        if arg.startswith('file_'):
            await view_file(client, message, arg[5:])
            return
        elif arg.startswith('el_'):
            token = arg[3:]
            entry = expired_links.get(token)
            if not entry:
                await message.reply_text('❌ This link is invalid or has expired.')
                return
            if datetime.now().timestamp() > entry['expires_at']:
                expired_links.pop(token, None)
                await message.reply_text('⌛ This link has expired.')
                return
            exp_str = format_date(entry['expires_at'])
            info = stored_files.get(entry['fid'])
            if not info:
                await message.reply_text('❌ File no longer exists.')
                return
            name = os.path.basename(info.get('name', ''))
            caption = f'🔗 **{name}**\n⌛ Link expires: {exp_str}'
            buttons = InlineKeyboardMarkup([[
                InlineKeyboardButton('🏠 Main Menu', callback_data='main_menu')
            ]])
            try:
                await send_file_smart(client, message.chat.id, info, caption, buttons)
            except Exception as e:
                await message.reply_text(f'❌ {e}')
            return
    await message.reply_text(
        f"✨ **Welcome to Your Personal Cloud Bot!** ✨\n\n"
        f"📁 **Total Files:** `{len(stored_files)}`\n\n"
        f"🖼️ Gallery · 📂 Categories · 🔍 Search",
        reply_markup=get_inline_menu()
    )
    await message.reply_text("Use the buttons below anytime:", reply_markup=get_main_menu())


@app.on_message(filters.command("cleanup"))
async def cleanup(client, message):
    if not is_owner(message.from_user.id):
        await message.reply_text("🔒 Private bot.")
        return
    missing = [fid for fid, i in stored_files.items() if not i.get('tg_file_id')]
    if not missing:
        await message.reply_text("✅ All files have valid Telegram file IDs.")
        return
    names = [os.path.basename(stored_files[fid].get('name', fid)) for fid in missing]
    name_list = "\n".join(f"• {n}" for n in names)
    await message.reply_text(
        f"🧹 **Cleanup — {len(missing)} record(s) missing Telegram file ID:**\n\n{name_list}\n\n"
        f"Remove them from storage?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, clean up", callback_data="cleanup_confirm"),
            InlineKeyboardButton("❌ Cancel",        callback_data="cleanup_cancel"),
        ]])
    )

@app.on_message(filters.command("migrate_hashes"))
async def migrate_hashes(client, message):
    if not is_owner(message.from_user.id):
        return
    processing = await message.reply_text("🔁 Computing hashes for existing files... This may take a while.")
    updated = 0
    for fid, info in stored_files.items():
        if info.get('hash'):
            continue
        file_size = info.get('size', 0)
        if file_size > MAX_HASH_SIZE:
            print(f"Skipping {fid} ({file_size} bytes) – too large")
            continue
        file_id = info.get('tg_file_id')
        if not file_id:
            continue
        try:
            file_hash = await compute_file_hash(client, file_id, file_size)
            if file_hash:
                stored_files[fid]['hash'] = file_hash
                updated += 1
                await asyncio.sleep(0.1)  # avoid hitting limits
        except Exception as e:
            print(f"Error hashing {fid}: {e}")
    save_storage(stored_files)
    await processing.edit_text(f"✅ Migration complete! Updated {updated} files with hash.")

@app.on_message(filters.command("reminder"))
async def manual_reminder(client, message):
    if not is_owner(message.from_user.id):
        return
    now = datetime.now().timestamp()
    await message.reply_text(
        f"🗓️ **Manual Backup Reminder**\n\n"
        f"Please upload any important files to your cloud storage bot to keep them safe!\n\n"
        f"📤 Use the **Upload** button or send me files directly.\n\n"
        f"_The next automatic reminder will be in {REMINDER_INTERVAL_DAYS} days._"
    )
    save_last_reminder(now)

@app.on_message(filters.text & filters.private)
async def handle_text(client, message):
    if not is_owner(message.from_user.id):
        await message.reply_text("🔒 Private bot.")
        return
    text = message.text
    if text and text.startswith('/'):
        return
    if text == "📤 Upload":
        await message.reply_text(f"📤 Send me any file!\n\n💾 Stored: `{len(stored_files)}` files", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
    elif text == "🖼️ Gallery View":
        await show_gallery_page(message, client, page=1)
    elif text == "📂 Categories":
        await show_categories(message, client)
    elif text == "🔍 Search":
        await message.reply_text("🔍 **Search Files**\n\nType a filename or keyword.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
    elif text == "⭐ Favourites":
        await show_favourites_page(message, client, page=1)
    elif text == "🔁 Duplicates":
        await show_duplicates(message, client)
    elif text == "♻️ Recycle Bin":
        await show_recycle_categories(message, client)
    elif text == "ℹ️ Help":
        await message.reply_text(
            f"ℹ️ **Help & Guide**\n"
            f"{chr(8212)*32}\n\n"
            f"📤 **Uploading**\n"
            f"Send any file — photo, video, audio or document.\n"
            f"Stored on **Telegram's cloud**, not your device.\n\n"
            f"🖼️ **Gallery** — 10-file album grid, sort by date/name/size\n"
            f"📂 **Categories** — browse by file type with numbered pagination\n"
            f"🔍 **Search** — find files by keyword\n"
            f"⭐ **Favourites** — star & browse starred files\n"
            f"🔁 **Duplicates** — detect by content 🔴 or name 🟡, keep newest\n\n"
            f"{chr(8212)*32}\n"
            f"🔘 **File Actions** (tap 👁️ View on any file)\n"
            f"👁️ View — preview file\n"
            f"🔗 Link — shareable link + inline preview\n"
            f"⏰ Expiring Link — auto-expires in 1h / 6h / 1d / 7d / 30d\n"
            f"☆ Fav — star a file\n"
            f"✏️ Rename — rename any file\n"
            f"🏷️ Tag — set a custom tag/category\n"
            f"🗑️ Delete — moves file to Recycle Bin\n"
            f"🗑️ Bulk Delete — checkbox-select multiple files (per category or all)\n\n"
            f"{chr(8212)*32}\n"
            f"♻️ **Recycle Bin**\n"
            f"• View · Restore · Delete Forever · Empty Bin\n"
            f"• ♻️ **Bulk Restore** — checkbox-select files to restore\n"
            f"• ✅ Select All button per page\n"
            f"• Auto-purged after **{RECYCLE_TTL_DAYS} days**\n\n"
            f"{chr(8212)*32}\n"
            f"🛡️ **Security**\n"
            f"Owner-only access · malicious file detection\n"
            f"Expiring links · /cleanup removes ghost records\n\n"
            f"{chr(8212)*32}\n"
            f"☁️ Files on **Telegram cloud** — zero local disk usage\n"
            f"📁 Active: `{len(stored_files)}` · ⭐ Favs: `{sum(1 for i in stored_files.values() if i.get('favourite'))}` · 🏷️ Tagged: `{sum(1 for i in stored_files.values() if i.get('tag'))}` · ♻️ Bin: `{len(recycle_bin)}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
        )
    elif text == "📊 Stats":
        try:
            now = datetime.now().timestamp()
            total_size   = sum(i.get('size', 0) for i in stored_files.values())
            rb_size      = sum(i.get('size', 0) for i in recycle_bin.values())
            cats         = group_files_by_category()
            missing      = [fid for fid, i in stored_files.items() if not i.get('tg_file_id')]

            if recycle_bin:
                oldest      = min(recycle_bin.values(), key=lambda i: i.get('deleted_at', now))
                days_left   = max(0, RECYCLE_TTL_DAYS - int((now - oldest.get('deleted_at', now)) / 86400))
                expiry_note = f"🔜 Next auto-delete in **{days_left} day(s)**"
            else:
                expiry_note = "✅ Recycle bin is empty"

            storage_json_size = os.path.getsize(STORAGE_FILE) if os.path.exists(STORAGE_FILE) else 0
            recycle_json_size = os.path.getsize(RECYCLE_FILE) if os.path.exists(RECYCLE_FILE) else 0
            local_size        = storage_json_size + recycle_json_size

            favs_count   = sum(1 for i in stored_files.values() if i.get('favourite'))
            tagged_count = sum(1 for i in stored_files.values() if i.get('tag'))
            uid_seen = {}; name_seen = {}; dup_count = 0
            for info in stored_files.values():
                uid = info.get('file_unique_id',''); nm = os.path.basename(info.get('name','')).lower()
                if uid: uid_seen[uid] = uid_seen.get(uid,0)+1
                name_seen[nm] = name_seen.get(nm,0)+1
            dup_count = sum(v-1 for v in uid_seen.values() if v>1) + sum(v-1 for v in name_seen.values() if v>1)

            category_lines = []
            for cat, files in sorted(cats.items()):
                cat_size = sum(stored_files[f["id"]].get("size", 0) for f in files if f["id"] in stored_files)
                category_lines.append(f"  {cat}: `{len(files)}` · `{format_file_size(cat_size)}`")

            stats = (
                f"📊 **BOT STATISTICS**\n"
                f"{'─' * 35}\n\n"
                f"☁️ **CLOUD STORAGE** · Telegram Servers\n"
                f"📁 Active files:     `{len(stored_files)}`\n"
                f"💾 Cloud size:       `{format_file_size(total_size)}`\n"
                f"⭐ Favourites:       `{favs_count}`\n"
                f"🏷️ Tagged files:     `{tagged_count}`\n"
                f"🔁 Duplicate copies: `{dup_count}`\n"
                f"{'─' * 35}\n"
                f"♻️ **RECYCLE BIN**\n"
                f"🗑️ Files in bin:     `{len(recycle_bin)}`\n"
                f"💾 Bin size:         `{format_file_size(rb_size)}`\n"
                f"⏳ Auto-purge:       `{RECYCLE_TTL_DAYS} days`\n"
                f"{expiry_note}\n"
                f"{'─' * 35}\n"
                f"💻 **LOCAL STORAGE** · This Device\n"
                f"_All files stored on Telegram cloud — zero local disk usage_\n"
                f"📄 storage.json:      `{format_file_size(storage_json_size)}`\n"
                f"📄 recycle_bin.json:  `{format_file_size(recycle_json_size)}`\n"
                f"📦 Total local:       `{format_file_size(local_size)}`\n"
                f"{'─' * 35}\n"
                f"📂 **FILES BY CATEGORY**\n"
            )
            for line in category_lines[:15]:
                stats += f"{line}\n"
            if len(category_lines) > 15:
                stats += f"_... and {len(category_lines)-15} more categories_"
            if missing:
                stats += f"\n\n⚠️ `{len(missing)}` file(s) missing Telegram ID — use /cleanup"

            chart_buf = None
            try:
                chart_buf = generate_stats_pie_chart(cats, stored_files)
            except Exception as chart_err:
                logging.warning(f"Pie chart generation failed: {chart_err}")

            main_menu_btn = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
            if chart_buf:
                await message.reply_photo(chart_buf, caption=stats, reply_markup=main_menu_btn)
            else:
                await message.reply_text(stats, reply_markup=main_menu_btn)

        except Exception as e:
            logging.error(f"Stats error: {e}")
            await message.reply_text(
                "❌ **Failed to load statistics.**\n\nPlease check the logs or try again later.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
            )
    elif rename_pending.get(message.from_user.id):
        fid = rename_pending.pop(message.from_user.id)
        info = stored_files.get(fid)
        if info:
            old_name = info.get('name', 'Unknown')
            new_ext  = os.path.splitext(old_name)[1]
            new_name = text.strip() if text.strip().endswith(new_ext) else text.strip() + new_ext
            stored_files[fid]['name'] = new_name
            save_storage(stored_files)
            await message.reply_text(
                f"✏️ **Renamed!**\n\n"
                f"Old: `{os.path.basename(old_name)}`\n"
                f"New: `{new_name}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                    InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
                ]])
            )
        else:
            await message.reply_text("❌ File not found.")
    elif user_state.get(str(message.from_user.id)) == 'awaiting_tag':
        fid = user_state.pop(str(message.from_user.id) + '_fid', None)
        user_state.pop(str(message.from_user.id), None)
        if fid and fid in stored_files:
            tag = text.strip()[:30]
            stored_files[fid]['tag'] = tag
            save_storage(stored_files)
            await message.reply_text(
                f"🏷️ **Tag set!**\n\n"
                f"`{os.path.basename(stored_files[fid].get('name',''))}` → `{tag}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("👁️ View", callback_data=f"view_{fid}"),
                    InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
                ]])
            )
        else:
            await message.reply_text("❌ File not found.")
    else:
        term = text.lower()
        results = [(fid, info) for fid, info in stored_files.items() if term in info.get('name', '').lower()]
        if not results:
            await message.reply_text(f"🔍 No files found matching `{text}`")
            return
        resp = f"🔍 **Results for:** `{text}`\n" + "─" * 30 + "\n\n"
        for fid, info in results[:20]:
            name = os.path.basename(info.get('name', ''))
            size = format_file_size(info.get('size', 0))
            resp += f"• {name} — {size}\n"
        if len(results) > 20:
            resp += f"\n_Showing 20 of {len(results)} results._"
        await message.reply_text(resp, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))


# ─────────────────────────────────────────────
# REMINDER BACKGROUND TASK
# ─────────────────────────────────────────────

async def reminder_task():
    while True:
        await asyncio.sleep(24 * 60 * 60)  # check every 24 hours
        now = datetime.now().timestamp()
        last = load_last_reminder()
        if now - last >= REMINDER_INTERVAL_DAYS * 24 * 60 * 60:
            try:
                await app.send_message(
                    OWNER_ID,
                    f"🗓️ **Backup Reminder**\n\n"
                    f"It's been **{REMINDER_INTERVAL_DAYS} days** since your last backup reminder.\n"
                    f"Please upload any important files to your cloud storage bot to keep them safe!\n\n"
                    f"📤 Use the **Upload** button or send me files directly.\n\n"
                    f"_This reminder repeats every {REMINDER_INTERVAL_DAYS} days._"
                )
                save_last_reminder(now)
                print(f"📬 Sent backup reminder to owner (last reminder {datetime.fromtimestamp(last)})")
            except Exception as e:
                print(f"⚠️ Failed to send reminder: {e}")


# ─────────────────────────────────────────────
# AUTO-PURGE TASK
# ─────────────────────────────────────────────

async def auto_purge_recycle_bin():
    while True:
        await asyncio.sleep(24 * 60 * 60)
        now = datetime.now().timestamp()
        cutoff = RECYCLE_TTL_DAYS * 24 * 60 * 60
        expired = [fid for fid, info in list(recycle_bin.items()) if now - info.get('deleted_at', now) >= cutoff]
        if expired:
            for fid in expired:
                recycle_bin.pop(fid, None)
            save_recycle_bin(recycle_bin)
            print(f"♻️  Auto-purged {len(expired)} file(s) older than {RECYCLE_TTL_DAYS} days")
            try:
                await app.send_message(
                    OWNER_ID,
                    f"🗑️ **Recycle Bin Auto-Purge**\n\n"
                    f"Automatically deleted `{len(expired)}` file(s) that were in the "
                    f"recycle bin for more than **{RECYCLE_TTL_DAYS} days**.\n"
                    f"♻️ Remaining in bin: `{len(recycle_bin)}`"
                )
            except Exception as e:
                print(f"⚠️  Could not notify owner: {e}")
        else:
            print("♻️  Auto-purge check: nothing expired.")


# ─────────────────────────────────────────────
# LOAD & RUN
# ─────────────────────────────────────────────

stored_files = load_storage()
recycle_bin = load_recycle_bin()
print(f"📁 Loaded {len(stored_files)} files from storage")
print(f"♻️  Loaded {len(recycle_bin)} files from recycle bin")
print("🤖 Bot starting…")
print("✅ Features: Gallery (10/page) | View | Link+Preview | Delete | Bulk Delete | Categories | Bulk Restore")

async def main():
    await app.start()
    print("✅ Bot online — auto-purge task started (checks every 24h)")
    asyncio.create_task(auto_purge_recycle_bin())
    asyncio.create_task(reminder_task())   # start the reminder background task
    await idle()
    await app.stop()

app.run(main())