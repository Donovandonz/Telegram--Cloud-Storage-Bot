import os
import json
import logging
import asyncio
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
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

# 🔒 YOUR TELEGRAM USER ID — set in .env as OWNER_ID=your_id
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

STORAGE_FILE    = "storage.json"
RECYCLE_FILE    = "recycle_bin.json"
RECYCLE_TTL_DAYS = 30          # auto-purge files older than this

# Pending state tracking
pending_uploads  = {}   # { pending_id: upload_data }
pending_deletes  = {}   # { user_id: [file_id, ...] }  – bulk-delete selections
user_state       = {}   # { user_id: state_string }
rename_pending   = {}   # { user_id: fid }  – waiting for new name text
sort_pref        = {}   # { user_id: 'date'|'name'|'size' }
expired_links    = {}   # { token: { fid, expires_at } }  – link expiry
import secrets, io
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

def is_duplicate(file_name, tg_file_id=None, file_unique_id=None):
    """
    Check for duplicates using three layers:
    1. tg_file_id match — same Telegram file object (most reliable for resent files)
    2. file_unique_id match — same content fingerprint across different sends
    3. normalized filename match — same name ignoring case/whitespace
    """
    name_normalized = os.path.basename(file_name).lower().strip()
    for info in stored_files.values():
        # Layer 1: tg_file_id (Telegram reuses this for the same file)
        stored_tg = info.get('tg_file_id', '')
        if tg_file_id and stored_tg and stored_tg == tg_file_id:
            return True, 'content'
        # Layer 2: file_unique_id (stable content fingerprint)
        stored_uid = info.get('file_unique_id', '')
        if file_unique_id and stored_uid and stored_uid == file_unique_id:
            return True, 'content'
        # Layer 3: normalized filename
        stored_name = os.path.basename(info.get('name', '')).lower().strip()
        if stored_name and stored_name == name_normalized:
            return True, 'filename'
    return False, None

# ─────────────────────────────────────────────
# FILE TYPE HELPERS
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
# STATS PIE CHART
# ─────────────────────────────────────────────

# Category → color mapping
CAT_COLORS = {
    "📸 Images":           "#4A90D9",   # Blue
    "🎬 Videos":           "#E74C3C",   # Red
    "🎵 Audio":            "#9B59B6",   # Purple
    "📑 PDF Documents":    "#27AE60",   # Green
    "📝 Word Documents":   "#2ECC71",   # Light green
    "📊 Excel Spreadsheets": "#F39C12", # Orange
    "📽️ PowerPoint":       "#E67E22",   # Dark orange
    "📄 Text Files":       "#95A5A6",   # Grey
    "🗜️ Archives":         "#C0392B",   # Dark red
    "💻 Code Files":       "#1ABC9C",   # Teal
    "📱 Android Apps":     "#3498DB",   # Sky blue
    "📁 Other Files":      "#BDC3C7",   # Light grey
}

def generate_stats_pie_chart(cats, stored_files):
    """Generate a high-quality pie chart PNG and return as BytesIO. Returns None if matplotlib unavailable."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import io as _io

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

        # Legend
        legend_patches = [
            mpatches.Patch(color=colors[i], label=labels[i])
            for i in range(len(labels))
        ]
        legend = ax.legend(
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
        ax.set_title(
            f"📊 Storage by Category  |  {total} files",
            color="white", fontsize=13, fontweight="bold", pad=16
        )

        plt.tight_layout()
        buf = _io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
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
    """Group recycle bin files by category."""
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
    """Show recycle bin category list with item counts."""
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

    # Find the file closest to expiry
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
        InlineKeyboardButton("🏠 Main Menu",          callback_data="main_menu"),
    ])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

async def show_recycle_category_page(message, client, category, page=1):
    """Show files in a recycle bin category as a grid (same album approach)."""
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

    # Header + nav
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

    # Bucket by type
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

    # Image album
    if image_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in image_bucket:
            if not info.get('tg_file_id', ''):
                continue
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            src_media = tg_id
            deleted_str = format_date(info.get('deleted_at', 0))
            media_group.append(InputMediaPhoto(src_media, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n🗑️ {deleted_str}"))
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

    # Video album
    if video_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in video_bucket:
            if not info.get('tg_file_id', ''):
                continue
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            src_media = tg_id
            deleted_str = format_date(info.get('deleted_at', 0))
            media_group.append(InputMediaVideo(src_media, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n🗑️ {deleted_str}", supports_streaming=True))
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

    # Others individually
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

    # Footer nav
    footer_nav = []
    if page > 1:
        footer_nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"rb_cat_{category}_{page - 1}"))
    if page < total_pages:
        footer_nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"rb_cat_{category}_{page + 1}"))

    footer_rows = []
    if footer_nav:
        footer_rows.append(footer_nav)
    footer_rows.append([
        InlineKeyboardButton("🔙 Back to Recycle Bin", callback_data="recycle_bin_1"),
        InlineKeyboardButton("🏠 Main Menu",            callback_data="main_menu"),
    ])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(footer_rows))

async def view_recycle_file(client, message, fid):
    """Preview a file from the recycle bin."""
    info = recycle_bin.get(fid)
    if not info:
        await message.reply_text("❌ File not found in recycle bin.")
        return

    ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
    file_path = info.get('path', '')
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
    """Show only starred files, same album-grid layout."""
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
    end   = min(start + ITEMS_PER_PAGE, total)
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
        if ext in IMAGE_EXTS:   image_bucket.append((idx, fid, info))
        elif ext in VIDEO_EXTS: video_bucket.append((idx, fid, info))
        else:                   other_list.append((idx, fid, info))

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
            try: await client.send_media_group(message.chat.id, mg)
            except Exception as e: await message.reply_text(f"⚠️ {e}")
            if vb: await message.reply_text(f"⭐ **Controls** (#{vb[0][0]}–#{vb[-1][0]})", reply_markup=fav_panel(vb))

    if video_bucket:
        from pyrogram.types import InputMediaVideo as IMV
        mg, vb = [], []
        for idx, fid, info in video_bucket:
            tg_id = info.get('tg_file_id','')
            if not tg_id: continue
            mg.append(IMV(tg_id, caption=f"⭐#{idx} {os.path.basename(info.get('name',''))}", supports_streaming=True))
            vb.append((idx, fid, info))
        if mg:
            try: await client.send_media_group(message.chat.id, mg)
            except Exception as e: await message.reply_text(f"⚠️ {e}")
            if vb: await message.reply_text(f"⭐ **Controls** (#{vb[0][0]}–#{vb[-1][0]})", reply_markup=fav_panel(vb))

    for idx, fid, info in other_list:
        clean = os.path.basename(info.get('name','Unknown'))
        icon  = get_file_icon(info.get('ext','.'))
        caption = f"{icon} ⭐ **#{idx}. {clean}**\n📏 {format_file_size(info.get('size',0))}"
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁️ View",   callback_data=f"view_{fid}"),
            InlineKeyboardButton("⭐ Unfav",  callback_data=f"fav_{fid}"),
            InlineKeyboardButton("🗑️ Del",    callback_data=f"delete_{fid}"),
        ]])
        try: await send_file_smart(client, message.chat.id, info, caption, buttons)
        except Exception as e: await client.send_message(message.chat.id, f"{icon} #{idx}. {clean}\n❌ {e}", reply_markup=buttons)

    footer = []
    if page > 1: footer.append(InlineKeyboardButton("◀️ Prev", callback_data=f"favourites_{page-1}"))
    if page < total_pages: footer.append(InlineKeyboardButton("Next ▶️", callback_data=f"favourites_{page+1}"))
    rows = [footer] if footer else []
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(rows))




# ─────────────────────────────────────────────
# SECURITY — MALICIOUS FILE DETECTION
# ─────────────────────────────────────────────

# Dangerous extensions that should never be stored
BLOCKED_EXTENSIONS = {
    '.exe', '.bat', '.cmd', '.com', '.pif', '.scr', '.vbs', '.vbe',
    '.js', '.jse', '.ws', '.wsf', '.wsc', '.wsh', '.ps1', '.ps1xml',
    '.ps2', '.ps2xml', '.psc1', '.psc2', '.msh', '.msh1', '.msh2',
    '.mshxml', '.msh1xml', '.msh2xml', '.scf', '.lnk', '.inf',
    '.reg', '.dll', '.sys', '.drv', '.cpl', '.ocx', '.jar',
    '.msi', '.msp', '.hta', '.htm_embedded', '.xbap',
}

# Suspicious MIME-like name patterns
SUSPICIOUS_PATTERNS = [
    'invoice', 'payment', 'urgent', 'account-suspended',
    'verify-now', 'click-here', 'free-download',
]

def check_malicious(file_name: str, file_size: int) -> tuple[bool, str]:
    """
    Returns (is_malicious, reason) tuple.
    Checks extension, double extensions, suspicious names, and size anomalies.
    """
    name_lower = file_name.lower()
    ext        = os.path.splitext(name_lower)[1]

    # Blocked extension
    if ext in BLOCKED_EXTENSIONS:
        return True, f"Blocked file type: `{ext}`"

    # Double extension trick (e.g. photo.jpg.exe)
    parts = name_lower.split('.')
    if len(parts) >= 3:
        second_ext = '.' + parts[-2]
        if second_ext in BLOCKED_EXTENSIONS:
            return True, f"Double extension attack detected: `{file_name}`"

    # Null byte in filename
    if chr(0) in file_name:
        return True, "Null byte in filename — potential injection attempt"

    # Suspicious name patterns
    for pattern in SUSPICIOUS_PATTERNS:
        if pattern in name_lower:
            return True, f"Suspicious filename pattern: `{pattern}`"

    # Extremely tiny file with executable extension
    if file_size < 100 and ext in {'.pdf', '.docx', '.xlsx', '.jpg', '.png'}:
        return True, f"Suspiciously small file ({file_size} bytes) for type `{ext}`"

    return False, ""


# ─────────────────────────────────────────────
# DUPLICATE FILE MANAGER
# ─────────────────────────────────────────────

async def show_duplicates(message, client):
    """Find files with identical content (file_unique_id) OR identical names."""
    # Group by file_unique_id first (true content duplicates)
    by_uid  = {}
    by_name = {}
    for fid, info in stored_files.items():
        uid  = info.get('file_unique_id', '')
        name = os.path.basename(info.get('name', '')).lower()
        if uid:  by_uid.setdefault(uid, []).append((fid, info))
        by_name.setdefault(name, []).append((fid, info))

    # Merge: content dupes take priority, then name dupes
    seen_fids = set()
    dupe_groups = {}  # label -> [(fid, info)]
    for uid, entries in by_uid.items():
        if len(entries) > 1:
            label = f"[same content] {os.path.basename(entries[0][1].get('name',''))}"
            dupe_groups[label] = entries
            for fid, _ in entries: seen_fids.add(fid)
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
        for fid, info in entries:
            date_str = format_date(info.get('date', 0))
            size_str = format_file_size(info.get('size', 0))
            text += f"  · {size_str}  📅 {date_str}\n"
        safe_key = label[:40]
        buttons.append([InlineKeyboardButton(
            f"🗑️ Keep newest · {display[:22]}", callback_data=f"dedup_{safe_key}"
        )])
        text += "\n"
    text += "🔴 = identical file content  |  🟡 = same filename\n"
    buttons.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ─────────────────────────────────────────────
# GALLERY VIEW  (10 items per page, grid-style)
# ─────────────────────────────────────────────

ITEMS_PER_PAGE = 10

def get_sorted_files(user_id=None):
    """Return files sorted by user preference (default: newest first)."""
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
    """
    Grid gallery — 10 files per page.

    Strategy:
    • Images/GIFs  → sent as a single media group (album) of up to 10,
                     then ONE control message with all View/Link/Delete buttons.
    • Videos       → same but as a video album (send_media_group supports video).
    • Mixed / docs → compact numbered list + one button row per file
                     (Telegram albums require a single media type).

    Each image in the album gets a short caption: "#1 filename".
    The control panel below maps numbers → buttons so scrolling is minimal.
    """
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

    bot_username = client.me.username

    # ── Header ────────────────────────────────────────────────
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

    # ── Bucket files by type for this page ───────────────────
    IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov'}

    image_bucket  = []   # (global_idx, fid, info)
    video_bucket  = []
    other_list    = []   # sent individually

    for idx, (fid, info) in enumerate(page_files, start + 1):
        ext = info.get('ext', os.path.splitext(info.get('name', ''))[1]).lower()
        if ext in IMAGE_EXTS:
            image_bucket.append((idx, fid, info))
        elif ext in VIDEO_EXTS:
            video_bucket.append((idx, fid, info))
        else:
            other_list.append((idx, fid, info))

    # ── Helper: build control-panel buttons for a bucket ─────
    def control_panel(bucket):
        """Return InlineKeyboardMarkup with one row per file: [👁 View] [🔗 Link] [🗑 Del] + date row"""
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

    # ── Send IMAGE album ──────────────────────────────────────
    if image_bucket:
        media_group = []
        valid_bucket = []
        for idx, fid, info in image_bucket:
            tg_id = info.get('tg_file_id', '')
            src_media = tg_id if tg_id else fp
            if not tg_id:
                continue
            date_str = format_date(info.get('date', 0))
            caption = f"#{idx} {os.path.basename(info.get('name', ''))}" + f"\n📤 {date_str}"
            media_group.append(InputMediaPhoto(src_media, caption=caption))
            valid_bucket.append((idx, fid, info))

        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Album send error: {e}")

            # Control panel
            label = f"🖼️ **Image Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})"
            await message.reply_text(label, reply_markup=control_panel(valid_bucket))

    # ── Send VIDEO album ──────────────────────────────────────
    if video_bucket:
        media_group = []
        valid_bucket = []
        for idx, fid, info in video_bucket:
            tg_id = info.get('tg_file_id', '')
            src_media = tg_id if tg_id else fp
            if not tg_id:
                continue
            date_str = format_date(info.get('date', 0))
            caption = f"#{idx} {os.path.basename(info.get('name', ''))}" + f"\n📤 {date_str}"
            media_group.append(InputMediaVideo(src_media, caption=caption, supports_streaming=True))
            valid_bucket.append((idx, fid, info))

        if media_group:
            try:
                await client.send_media_group(message.chat.id, media_group)
            except Exception as e:
                await message.reply_text(f"⚠️ Video album error: {e}")

            label = f"🎬 **Video Controls** (#{valid_bucket[0][0]}–#{valid_bucket[-1][0]})"
            await message.reply_text(label, reply_markup=control_panel(valid_bucket))

    # ── Send OTHER files individually ─────────────────────────
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

    # ── Footer ────────────────────────────────────────────────
    footer_nav = []
    if page > 1:
        footer_nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gallery_{page - 1}"))
    if page < total_pages:
        footer_nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"gallery_{page + 1}"))

    footer_rows = []
    if footer_nav:
        footer_rows.append(footer_nav)
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
    """
    Same grid logic as show_gallery_page but scoped to one category.
    Images/videos → media group album + control panel.
    Others → individual messages.
    """
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

    # ── Header ────────────────────────────────────────────────
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

    # ── Bucket by type ────────────────────────────────────────
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

    # ── Image album ───────────────────────────────────────────
    if image_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in image_bucket:
            if not info.get('tg_file_id', ''):
                continue
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            src_media = tg_id
            date_str = format_date(info.get('date', 0))
            media_group.append(InputMediaPhoto(src_media, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n📤 {date_str}"))
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

    # ── Video album ───────────────────────────────────────────
    if video_bucket:
        media_group, valid_bucket = [], []
        for idx, fid, info in video_bucket:
            if not info.get('tg_file_id', ''):
                continue
            tg_id = info.get('tg_file_id', '')
            if not tg_id:
                continue
            src_media = tg_id
            date_str = format_date(info.get('date', 0))
            media_group.append(InputMediaVideo(src_media, caption=f"#{idx} {os.path.basename(info.get('name', ''))}\n📤 {date_str}", supports_streaming=True))
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

    # ── Others individually ───────────────────────────────────
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

    # ── Footer ────────────────────────────────────────────────
    footer_nav = []
    if page > 1:
        footer_nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"catpage_{category}_{page - 1}"))
    if page < total_pages:
        footer_nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"catpage_{category}_{page + 1}"))

    footer_rows = []
    if footer_nav:
        footer_rows.append(footer_nav)
    footer_rows.append([
        InlineKeyboardButton("🗑️ Bulk Delete", callback_data="bulk_delete_start"),
        InlineKeyboardButton("🔙 Categories",  callback_data="categories"),
        InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
    ])
    await message.reply_text(f"─── End of page {page}/{total_pages} ───", reply_markup=InlineKeyboardMarkup(footer_rows))


# ─────────────────────────────────────────────
# FILE VIEW HELPER
# ─────────────────────────────────────────────

async def send_file_smart(client, chat_id, info, caption, buttons):
    """Send file using Telegram file_id — instant, no local disk used."""
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
    tag    = info.get('tag', '')
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

async def show_bulk_delete_menu(message, client, user_id, page=1):
    """Show a paginated checkbox-style list for bulk deletion."""
    all_files = get_sorted_files()
    total = len(all_files)
    if total == 0:
        await message.reply_text("📁 No files to delete.")
        return

    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total)
    page_files = all_files[start:end]

    selected = pending_deletes.get(user_id, [])

    buttons = []
    for fid, info in page_files:
        name = os.path.basename(info.get('name', 'Unknown'))
        is_sel = fid in selected
        tick = "✅ " if is_sel else "☐ "
        cb = f"bulk_toggle_{fid}_{page}"
        buttons.append([InlineKeyboardButton(f"{tick}{name}", callback_data=cb)])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"bulk_page_{page - 1}"))
    nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"bulk_page_{page + 1}"))
    if nav:
        buttons.append(nav)

    sel_count = len(selected)
    buttons.append([
        InlineKeyboardButton(f"🗑️ Delete {sel_count} selected", callback_data="bulk_delete_confirm") if sel_count else InlineKeyboardButton("☐ Select files above", callback_data="noop"),
        InlineKeyboardButton("❌ Cancel", callback_data="bulk_delete_cancel")
    ])

    header = (
        f"🗑️ **BULK DELETE** — Page {page}/{total_pages}\n"
        f"✅ Selected: {sel_count} file{'s' if sel_count != 1 else ''}\n\n"
        f"_Tap a file to select/deselect it._"
    )
    await message.reply_text(header, reply_markup=InlineKeyboardMarkup(buttons))


# ─────────────────────────────────────────────
# UPLOAD PROCESSING
# ─────────────────────────────────────────────

async def process_upload(client, message, file_data):
    try:
        file_name      = file_data['name']
        file_size      = file_data['size']
        fid            = file_data['file_id']
        tg_file_id     = file_data.get('tg_file_id', '')
        file_unique_id = file_data.get('file_unique_id', '')
        ext            = os.path.splitext(file_name)[1].lower()

        stored_files[fid] = {
            'name': file_name, 'size': file_size,
            'date': datetime.now().timestamp(), 'ext': ext,
            'tg_file_id': tg_file_id,
            'file_unique_id': file_unique_id
        }
        save_storage(stored_files)

        link = f"https://t.me/{client.me.username}?start=file_{fid}"
        icon = get_file_icon(ext)

        await message.edit_text(
            f"✅ **Stored!**\n\n{icon} `{file_name}`\n"
            f"📏 {format_file_size(file_size)}\n\n"
            f"🔗 `{link}`\n💾 Total: `{len(stored_files)}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👁️ View",    callback_data=f"view_{fid}"),
                 InlineKeyboardButton("🖼️ Gallery", callback_data="gallery_1")],
                [InlineKeyboardButton("🔗 Open Link", url=link)],
                [InlineKeyboardButton("📤 Upload More", callback_data="upload"),
                 InlineKeyboardButton("🏠 Main Menu",  callback_data="main_menu")],
            ])
        )

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")




# ─────────────────────────────────────────────
# BOT HANDLERS
# ─────────────────────────────────────────────

@app.on_message(filters.command("start"))
async def start(client, message):
    if not is_owner(message.from_user.id):
        await message.reply_text("🔒 This bot is private.")
        return

    # Handle deep-link: /start file_<id>  or  /start el_<token>
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


@app.on_message(filters.document | filters.photo | filters.video | filters.audio)
async def store_file(client, message):
    if not is_owner(message.from_user.id):
        await message.reply_text("🔒 Private bot.")
        return

    processing = await message.reply_text("📥 **Uploading…**\n⬜⬜⬜ Step 1/3 — Receiving file…")

    try:
        if message.document:
            file           = message.document
            file_name      = file.file_name or f"doc_{file.file_unique_id}"
            file_size      = file.file_size or 0
            tg_file_id     = file.file_id
            file_unique_id = file.file_unique_id
        elif message.photo:
            file           = message.photo
            file_name      = f"photo_{file.file_unique_id}.jpg"
            file_size      = getattr(file, 'file_size', 0)
            tg_file_id     = file.file_id
            file_unique_id = file.file_unique_id
        elif message.video:
            file           = message.video
            file_name      = getattr(file, 'file_name', None) or f"video_{file.file_unique_id}.mp4"
            file_size      = file.file_size or 0
            tg_file_id     = file.file_id
            file_unique_id = file.file_unique_id
        elif message.audio:
            file           = message.audio
            file_name      = getattr(file, 'file_name', None) or f"audio_{file.file_unique_id}.mp3"
            file_size      = file.file_size or 0
            tg_file_id     = file.file_id
            file_unique_id = file.file_unique_id
        else:
            await processing.delete()
            await message.reply_text("Unsupported file type.")
            return

        await asyncio.sleep(0.8)
        await processing.edit_text("🔍 **Uploading…**\n🟩⬜⬜ Step 2/3 — Checking for duplicates…")

        # Security check
        is_bad, bad_reason = check_malicious(file_name, file_size)
        if is_bad:
            await processing.delete()
            await message.reply_text(
                f"🛡️ **Security Block!**\n\n{bad_reason}\n\n"
                f"_Rename the file and try again if this is a false positive._"
            )
            logging.warning(f'Blocked: {message.from_user.id}: {file_name} — {bad_reason}')
            return

        # Duplicate check
        is_dup, dup_type = is_duplicate(file_name, tg_file_id, file_unique_id)
        if is_dup:
            existing_info = None
            for info in stored_files.values():
                if (tg_file_id and info.get('tg_file_id') == tg_file_id) or \
                   (file_unique_id and info.get('file_unique_id') == file_unique_id) or \
                   os.path.basename(info.get('name', '')).lower() == os.path.basename(file_name).lower():
                    existing_info = info
                    break

            existing_detail = ""
            if existing_info:
                existing_detail = (
                    f"\n\n📄 **Existing file:**\n"
                    f"`{os.path.basename(existing_info.get('name', ''))}`\n"
                    f"📏 {format_file_size(existing_info.get('size', 0))}\n"
                    f"📅 {format_date(existing_info.get('date', 0))}"
                )

            dup_label = "🔴 **Same file content**" if dup_type == 'content' else "🟡 **Same filename**"
            pending_id = str(message.id)
            pending_uploads[pending_id] = {
                'name': file_name, 'size': file_size,
                'file_id': pending_id, 'tg_file_id': tg_file_id,
                'file_unique_id': file_unique_id
            }
            await processing.edit_text(
                f"⚠️ **Duplicate Detected!**\n\n"
                f"{dup_label} already exists.{existing_detail}\n\n"
                f"Upload anyway?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes, upload anyway", callback_data=f"confirm_{pending_id}"),
                    InlineKeyboardButton("❌ No, cancel",         callback_data=f"cancel_{pending_id}")
                ]])
            )
            return

        await asyncio.sleep(0.8)
        await processing.edit_text("💾 **Uploading…**\n🟩🟩⬜ Step 3/3 — Saving to storage…")
        await asyncio.sleep(0.6)

        fid = str(message.id)
        ext = os.path.splitext(file_name)[1].lower()
        stored_files[fid] = {
            'name': file_name, 'size': file_size,
            'date': datetime.now().timestamp(), 'ext': ext,
            'tg_file_id': tg_file_id, 'file_unique_id': file_unique_id
        }
        await processing.delete()
        save_storage(stored_files)

        link = f"https://t.me/{client.me.username}?start=file_{fid}"
        icon = get_file_icon(ext)
        await message.reply_text(
            f"✅ **Stored!**\n\n{icon} `{file_name}`\n"
            f"📏 {format_file_size(file_size)}\n\n"
            f"🔗 `{link}`\n💾 Total: `{len(stored_files)}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👁️ View",    callback_data=f"view_{fid}"),
                 InlineKeyboardButton("🖼️ Gallery", callback_data="gallery_1")],
                [InlineKeyboardButton("🔗 Open Link", url=link)],
                [InlineKeyboardButton("📤 Upload More", callback_data="upload"),
                 InlineKeyboardButton("🏠 Main Menu",  callback_data="main_menu")],
            ])
        )

    except Exception as e:
        try:
            await processing.delete()
        except Exception:
            pass
        await message.reply_text(f"❌ Error: {e}")


@app.on_message(filters.text & filters.private)
async def handle_text(client, message):
    if not is_owner(message.from_user.id):
        await message.reply_text("🔒 Private bot.")
        return

    text = message.text

    # Ignore commands — they have their own handlers
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
        now = datetime.now().timestamp()
        total_size   = sum(i.get('size', 0) for i in stored_files.values())
        rb_size      = sum(i.get('size', 0) for i in recycle_bin.values())
        cats         = group_files_by_category()
        missing      = [fid for fid, i in stored_files.items() if not i.get('tg_file_id')]

        # Recycle bin expiry info
        if recycle_bin:
            oldest      = min(recycle_bin.values(), key=lambda i: i.get('deleted_at', now))
            days_left   = max(0, RECYCLE_TTL_DAYS - int((now - oldest.get('deleted_at', now)) / 86400))
            expiry_note = f"🔜 Next auto-delete in **{days_left} day(s)**"
        else:
            expiry_note = "✅ Recycle bin is empty"

        # Local JSON sizes
        storage_json_size = os.path.getsize(STORAGE_FILE) if os.path.exists(STORAGE_FILE) else 0
        recycle_json_size = os.path.getsize(RECYCLE_FILE) if os.path.exists(RECYCLE_FILE) else 0
        local_size        = storage_json_size + recycle_json_size

        # Extra stats
        favs_count   = sum(1 for i in stored_files.values() if i.get('favourite'))
        tagged_count = sum(1 for i in stored_files.values() if i.get('tag'))
        # Duplicate count
        uid_seen = {}; name_seen = {}; dup_count = 0
        for info in stored_files.values():
            uid = info.get('file_unique_id',''); nm = os.path.basename(info.get('name','')).lower()
            if uid: uid_seen[uid] = uid_seen.get(uid,0)+1
            name_seen[nm] = name_seen.get(nm,0)+1
        dup_count = sum(v-1 for v in uid_seen.values() if v>1) + sum(v-1 for v in name_seen.values() if v>1)

        stats = (
            f"📊 **BOT STATISTICS**\n"
            f"{'─' * 35}\n\n"
            f"☁️ **CLOUD STORAGE** · Telegram Servers\n"
            f"📁 Active files:      `{len(stored_files)}`\n"
            f"💾 Cloud size:        `{format_file_size(total_size)}`\n"
            f"⭐ Favourites:        `{favs_count}`\n"
            f"🏷️ Tagged files:      `{tagged_count}`\n"
            f"🔁 Duplicate copies:  `{dup_count}`\n"
            f"{'─' * 35}\n"
            f"♻️ **RECYCLE BIN**\n"
            f"🗑️ Files in bin:      `{len(recycle_bin)}`\n"
            f"💾 Bin size:          `{format_file_size(rb_size)}`\n"
            f"⏳ Auto-purge:        `{RECYCLE_TTL_DAYS} days`\n"
            f"{expiry_note}\n"
            f"{'─' * 35}\n"
            f"💻 **LOCAL STORAGE** · This Device\n"
            f"_All files on Telegram cloud — zero local disk usage_\n"
            f"📄 storage.json:      `{format_file_size(storage_json_size)}`\n"
            f"📄 recycle_bin.json:  `{format_file_size(recycle_json_size)}`\n"
            f"📦 Total local:       `{format_file_size(local_size)}`\n"
            f"{'─' * 35}\n"
            f"🛠️ **FEATURES ACTIVE**\n"
            f"✅ Gallery · 10/page album grid · sort date/name/size\n"
            f"✅ Categories with numbered page buttons\n"
            f"✅ Bulk Delete (all or per category)\n"
            f"✅ Bulk Restore (with Select All per page)\n"
            f"✅ Favourites · Duplicates · Expiring Links\n"
            f"✅ Rename · Tag · Malicious file detection\n"
            f"✅ Auto-purge recycle bin every {RECYCLE_TTL_DAYS} days\n"
            f"{'─' * 35}\n"
            f"📂 **FILES BY CATEGORY**\n"
        )
        for cat, files in sorted(cats.items()):
            cat_size = sum(stored_files[f["id"]].get("size", 0) for f in files if f["id"] in stored_files)
            stats += f"  {cat}: `{len(files)}` · `{format_file_size(cat_size)}`\n"
        if missing:
            stats += f"\n⚠️ `{len(missing)}` file(s) missing Telegram ID — use /cleanup"

        # Generate pie chart
        chart_buf = generate_stats_pie_chart(cats, stored_files)
        if chart_buf:
            await message.reply_photo(chart_buf, caption=stats, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
        else:
            await message.reply_text(stats, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))
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
        # Search
        term = text.lower()
        results = [(fid, info) for fid, info in stored_files.items() if term in info.get('name', '').lower()]
        if not results:
            await message.reply_text(f"🔍 No files found matching `{text}`")
            return
        resp = f"🔍 **Results for:** `{text}`\n" + "─" * 30 + "\n\n"
        bot_username = client.me.username
        for fid, info in results[:20]:
            name = os.path.basename(info.get('name', ''))
            size = format_file_size(info.get('size', 0))
            resp += f"• {name} — {size}\n"
        if len(results) > 20:
            resp += f"\n_Showing 20 of {len(results)} results._"
        await message.reply_text(resp, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]))


@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    if not is_owner(callback_query.from_user.id):
        await callback_query.answer("🔒 Private bot.", show_alert=True)
        return

    data = callback_query.data
    msg = callback_query.message
    user_id = callback_query.from_user.id

    # tag_cancel gets a visible popup alert — all others get silent ack below
    if data.startswith("tag_cancel_"):
        await callback_query.answer("❌ Tag cancelled", show_alert=True)
    elif data.startswith("tag_clear_"):
        await callback_query.answer("🏷️ Tag cleared", show_alert=True)
    else:
        await callback_query.answer()

    # ── No-op ──────────────────────────────────────────────
    if data == "noop":
        return

    # ── Main menu ──────────────────────────────────────────
    if data == "main_menu":
        await msg.reply_text(
            f"🏠 **Main Menu**\n📁 Total: `{len(stored_files)}` files",
            reply_markup=get_inline_menu()
        )

    # ── Upload prompt ──────────────────────────────────────
    elif data == "upload":
        await msg.reply_text(f"📤 Send me any file!\n💾 Stored: `{len(stored_files)}`")

    # ── Gallery (paginated) ────────────────────────────────
    elif data.startswith("gallery_"):
        page = int(data.split("_")[1])
        await show_gallery_page(msg, client, page)

    # ── Categories list ────────────────────────────────────
    elif data == "categories":
        await show_categories(msg, client)

    # ── Category page ──────────────────────────────────────
    elif data.startswith("category_"):
        category = data[len("category_"):]
        await show_category_page(msg, client, category, page=1)

    elif data.startswith("catpage_"):
        # catpage_<category>_<page>
        parts = data.split("_")
        page = int(parts[-1])
        category = "_".join(parts[1:-1])
        await show_category_page(msg, client, category, page)

    # ── Search prompt ──────────────────────────────────────
    elif data == "search":
        await msg.reply_text("🔍 **Search**\n\nType a filename or keyword.")

    # ── Help ───────────────────────────────────────────────
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
    elif data.startswith("view_"):
        fid = data[5:]
        await view_file(client, msg, fid)

    # ── Link (preview + shareable link caption + copy button) ─
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
        # Caption shown on the preview itself
        caption = (
            f"{icon} **{clean_name}**\n"
            f"📏 {size_str}\n\n"
            f"🔗 `{link_url}`"
        )
        # Buttons attached to the preview
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Copy Link", url=link_url),
             InlineKeyboardButton("🗑️ Delete",    callback_data=f"delete_{fid}")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")],
        ])

        try:
            await send_file_smart(client, msg.chat.id, info, caption, buttons)
        except Exception as e:
            await msg.reply_text(f"⚠️ Preview failed: {e}\n\n🔗 `{link_url}`", reply_markup=buttons)

    # ── Single delete — ask confirmation ──────────────────
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

    # ── Bulk delete — start ────────────────────────────────
    elif data == "bulk_delete_start":
        pending_deletes[user_id] = []
        await show_bulk_delete_menu(msg, client, user_id, page=1)

    elif data.startswith("bulk_page_"):
        page = int(data.split("_")[-1])
        await show_bulk_delete_menu(msg, client, user_id, page)

    elif data.startswith("bulk_toggle_"):
        # bulk_toggle_<fid>_<page>
        parts = data.split("_")
        page = int(parts[-1])
        fid = "_".join(parts[2:-1])
        selected = pending_deletes.setdefault(user_id, [])
        if fid in selected:
            selected.remove(fid)
        else:
            selected.append(fid)
        pending_deletes[user_id] = selected
        await show_bulk_delete_menu(msg, client, user_id, page)

    elif data == "bulk_delete_confirm":
        selected = pending_deletes.get(user_id, [])
        if not selected:
            await msg.reply_text("☐ No files selected!")
            return
        names = [os.path.basename(stored_files[fid].get('name', fid)) for fid in selected if fid in stored_files]
        name_list = "\n".join(f"• {n}" for n in names)
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
        await msg.reply_text(
            f"🗑️ **Moved to Recycle Bin!**\n\n"
            f"♻️ Moved: `{moved}` file(s)\n"
            f"💾 Remaining: `{len(stored_files)}` files",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ View Recycle Bin", callback_data="recycle_bin_1"),
                InlineKeyboardButton("🏠 Main Menu",        callback_data="main_menu"),
            ]])
        )

    elif data == "bulk_delete_cancel":
        pending_deletes.pop(user_id, None)
        await msg.reply_text("❌ Bulk delete cancelled.")

    # ── Recycle Bin ───────────────────────────────────────
    elif data.startswith("recycle_bin_"):
        page = int(data.split("_")[-1])
        await show_recycle_categories(msg, client, page)

    elif data.startswith("rb_cat_"):
        # rb_cat_<category>_1
        parts = data.split("_")
        page = int(parts[-1])
        category = "_".join(parts[2:-1])
        await show_recycle_category_page(msg, client, category, page)

    elif data.startswith("rb_view_"):
        fid = data[8:]
        await view_recycle_file(client, msg, fid)

    # ── IMPORTANT: check longer prefixes FIRST to avoid startswith collision ──

    elif data.startswith("rb_restore_ask_"):
        fid = data[len("rb_restore_ask_"):]
        info = recycle_bin.get(fid)
        if not info:
            await msg.edit_text("❌ File not found in recycle bin.")
            return
        name = os.path.basename(info.get('name', 'Unknown'))
        # Edit the control panel message in-place to show inline confirmation
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
            await msg.edit_text(
                f"✅ **Restored!**\n\n📄 `{name}` is back in your storage.\n"
                f"♻️ Restored at: {restored_str}\n"
                f"💾 Total files: `{len(stored_files)}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("♻️ Recycle Bin", callback_data="recycle_bin_1"),
                    InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
                ]])
            )
        else:
            await msg.edit_text("❌ File not found.")

    elif data.startswith("rb_restore_cancel_"):
        fid = data[len("rb_restore_cancel_"):]
        info = recycle_bin.get(fid)
        name = os.path.basename(info.get('name', 'Unknown')) if info else "Unknown"
        await msg.edit_text(
            f"❌ Restore cancelled.\n\n📄 `{name}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ Restore",       callback_data=f"rb_restore_ask_{fid}"),
                InlineKeyboardButton("🗑️ Del Forever",   callback_data=f"rb_permdelete_{fid}"),
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

    # ── Cleanup ghost records ──────────────────────────────
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


    # ── Sort preference ────────────────────────────────────
    elif data.startswith("sort_"):
        pref = data[5:]  # name / size / date
        sort_pref[user_id] = pref
        label = {'name': '🔤 Name', 'size': '💾 Size', 'date': '🕐 Date'}.get(pref, '🕐 Date')
        await msg.reply_text(
            f"✅ Gallery sorted by **{label}**",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🖼️ Gallery", callback_data="gallery_1"),
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
        )

    # ── Favourites page ────────────────────────────────────
    elif data.startswith("favourites_"):
        page = int(data.split("_")[1])
        await show_favourites_page(msg, client, page, user_id)

    # ── Toggle favourite ───────────────────────────────────
    elif data.startswith("fav_"):
        fid = data[4:]
        info = stored_files.get(fid)
        if info:
            info['favourite'] = not info.get('favourite', False)
            save_storage(stored_files)
            state = "⭐ Added to Favourites!" if info['favourite'] else "☆ Removed from Favourites"
            name  = os.path.basename(info.get('name',''))
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

    # ── Rename ─────────────────────────────────────────────
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

    # ── Tag / custom category ──────────────────────────────
    elif data.startswith("tag_clear_"):
        fid = data[len("tag_clear_"):]
        user_state.pop(str(user_id), None)
        user_state.pop(str(user_id) + '_fid', None)
        user_state.pop(user_id, None)
        user_state.pop(user_id + '_fid', None)
        if fid in stored_files:
            stored_files[fid].pop('tag', None)
            save_storage(stored_files)
            clear_text = "🏷️ **Tag cleared!**\n\nThe tag has been removed from this file."
            clear_buttons = InlineKeyboardMarkup([[
                InlineKeyboardButton("👁️ View",      callback_data=f"view_{fid}"),
                InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
            ]])
            try:
                await msg.edit_text(clear_text, reply_markup=clear_buttons)
            except Exception:
                await msg.reply_text(clear_text, reply_markup=clear_buttons)
        else:
            await msg.reply_text("❌ File not found.")

    elif data.startswith("tag_cancel_"):
        fid = data[len("tag_cancel_"):]
        user_state.pop(str(user_id), None)
        user_state.pop(str(user_id) + '_fid', None)
        user_state.pop(user_id, None)
        user_state.pop(user_id + '_fid', None)
        logging.info(f"tag_cancel triggered: fid={fid}, user={user_id}")
        name = os.path.basename(stored_files.get(fid, {}).get('name', ''))
        cancel_text = f"❌ **Tag cancelled**\n\n📄 `{name}` — no changes made."
        cancel_buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁️ View",      callback_data=f"view_{fid}"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"),
        ]])
        try:
            await msg.edit_text(cancel_text, reply_markup=cancel_buttons)
        except Exception as e:
            logging.info(f"tag_cancel edit_text failed ({e}), using reply_text")
            await msg.reply_text(cancel_text, reply_markup=cancel_buttons)

    elif data.startswith("tag_"):
        fid = data[4:]
        info = stored_files.get(fid)
        if not info:
            await msg.reply_text("❌ File not found.")
            return
        user_state[str(user_id)] = 'awaiting_tag'
        user_state[str(user_id) + '_fid'] = fid
        current_tag = info.get('tag') or 'None'
        name = os.path.basename(info.get('name', ''))
        await msg.reply_text(
            f"🏷️ **Set Tag**\n\n"
            f"📄 File: `{name}`\n"
            f"🏷️ Current tag: `{current_tag}`\n\n"
            f"Type your new tag and send it\n"
            f"_(e.g. Work, Personal, Holiday, Project X)_",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑️ Clear Tag", callback_data=f"tag_clear_{fid}"),
                InlineKeyboardButton("❌ Cancel",    callback_data=f"tag_cancel_{fid}"),
            ]])
        )

    # ── Duplicates ─────────────────────────────────────────
    elif data == "duplicates":
        await show_duplicates(msg, client)

    elif data.startswith("dedup_"):
        # Keep the newest copy, delete the rest
        name_key = data[6:]
        entries = [
            (fid, info) for fid, info in stored_files.items()
            if os.path.basename(info.get('name','')).lower() == name_key
        ]
        if len(entries) < 2:
            await msg.reply_text("✅ No duplicates found for this file.")
            return
        entries.sort(key=lambda x: x[1].get('date', 0), reverse=True)
        kept = entries[0]
        removed = entries[1:]
        for fid, info in removed:
            stored_files.pop(fid, None)
        save_storage(stored_files)
        await msg.reply_text(
            f"✅ **Deduplicated!**\n\n"
            f"📄 Kept: `{os.path.basename(kept[1].get('name',''))}` (newest)\n"
            f"🗑️ Removed: `{len(removed)}` older copy/copies",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔁 Check Again", callback_data="duplicates"),
                InlineKeyboardButton("🏠 Main Menu",   callback_data="main_menu"),
            ]])
        )

    # ── Link expiry ────────────────────────────────────────
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
        fid   = parts[1]
        dur   = parts[2]
        info  = stored_files.get(fid)
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

    # ── Upload confirmation ────────────────────────────────
    elif data.startswith("confirm_"):
        pid = data[8:]
        if pid in pending_uploads:
            # Edit the confirmation message to show "uploading..." in-place
            await msg.edit_text(
                "💾 **Uploading…** (3/3) Saving to storage…",
                reply_markup=None
            )
            await process_upload(client, msg, pending_uploads.pop(pid))
        else:
            await msg.edit_text("⚠️ Upload session expired. Please send the file again.")

    elif data.startswith("cancel_"):
        pid = data[7:]
        pending_uploads.pop(pid, None)
        await msg.edit_text("❌ Upload cancelled.")


# ─────────────────────────────────────────────
# AUTO-PURGE TASK  (runs every 24 h)
# ─────────────────────────────────────────────

async def auto_purge_recycle_bin():
    """Background task: delete recycle bin entries older than RECYCLE_TTL_DAYS."""
    while True:
        await asyncio.sleep(24 * 60 * 60)   # check once a day
        now = datetime.now().timestamp()
        cutoff = RECYCLE_TTL_DAYS * 24 * 60 * 60
        expired = [
            fid for fid, info in list(recycle_bin.items())
            if now - info.get('deleted_at', now) >= cutoff
        ]
        if expired:
            for fid in expired:
                recycle_bin.pop(fid, None)
            save_recycle_bin(recycle_bin)
            print(f"♻️  Auto-purged {len(expired)} file(s) older than {RECYCLE_TTL_DAYS} days")
            # Notify owner
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
recycle_bin   = load_recycle_bin()
print(f"📁 Loaded {len(stored_files)} files from storage")
print(f"♻️  Loaded {len(recycle_bin)} files from recycle bin")
print("🤖 Bot starting…")
print("✅ Features: Gallery (10/page) | View | Link+Preview | Delete | Bulk Delete | Categories")


async def main():
    await app.start()
    print("✅ Bot online — auto-purge task started (checks every 24h)")
    asyncio.create_task(auto_purge_recycle_bin())
    await idle()
    await app.stop()


app.run(main())