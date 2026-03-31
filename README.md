# Telegram--Cloud-Storage-Bot
🧰 Premium Telegram Simple Cloud Storage Bot

## A personal Telegram bot that turns your Telegram account into a private cloud storage.  
Upload any file – photos, videos, audio, documents – and manage them with an intuitive gallery, categories, favourites, recycle bin, and duplicate detection. All files are stored on Telegram’s servers, using zero local disk space.

---

# Dashboard Overview
*** Help & Guide

<img width="538" height="1043" alt="Help   Guide Cloud Bot Storage Telegram" src="https://github.com/user-attachments/assets/dbda7b4e-599b-4585-bbf0-44defb56097c" />

*** Bot Statistic

<img width="530" height="1059" alt="Bot Statistic Cloud Bot Storage Telegram" src="https://github.com/user-attachments/assets/b77fc334-62c6-4f6e-bc36-63c8805ecf14" />

---

## ✨ Features

### 📤 Uploading
- Send any file (photo, video, audio, document) directly to the bot.  
- Files are stored on **Telegram's cloud** – no storage on your device.

### 🖼️ Gallery View
- Browse all files in a clean 10‑item grid.  
- Sort by **date**, **name**, or **size**.  
- Numbered pagination for easy navigation.

### 📂 Categories
- Files are automatically grouped by type: Images, Videos, Audio, Documents, Archives, Code, etc.  
- Each category has its own paginated view with file previews.

### 🔍 Search
- Find files by filename or keyword.

### ⭐ Favourites
- Mark any file as favourite.  
- Quick access to all starred files.

### 🔁 Duplicate Manager
- Detects duplicate files by **content** (SHA‑256 hash for files ≤10 MB) or **name**.  
- For each duplicate group you can **keep the newest** or **keep the oldest** copy.

### 🔘 File Actions (tap 👁️ View on any file)
| Action | Description |
|--------|-------------|
| **View** | Preview the file (photo, video, audio, etc.). |
| **Link** | Generate a permanent shareable link with inline preview. |
| **Expiring Link** | Create a link that expires after 1h, 6h, 1d, 7d, or 30d. |
| **Fav** | Star/unstar a file. |
| **Rename** | Change the file’s name (extension preserved automatically). |
| **Tag** | Add a custom tag or category to the file. |
| **Delete** | Move the file to the recycle bin. |
| **Bulk Delete** | Select multiple files (by category or all) and move them to the recycle bin in one go. |

### ♻️ Recycle Bin
- Deleted files stay in the bin for **30 days**, then are automatically purged.  
- **Restore** any file back to main storage.  
- **Delete Forever** – permanently remove a file.  
- **Empty Bin** – remove all files at once.  
- **Bulk Restore** – checkbox‑select multiple files and restore them together.  
- **Select All** button per page for easy bulk operations.

### 🛡️ Security
- **Private bot** – only the owner (your Telegram user ID) can interact.  
- **Malicious file detection** – blocks dangerous extensions and suspicious filename patterns.  
- **Expiring links** – control how long shared links remain active.  
- **/cleanup** command – removes orphaned records (files missing Telegram file IDs).

### 📊 Statistics
- Shows total active files, cloud size, favourites count, tagged files, duplicate copies.  
- Recycle bin usage and next auto‑purge date.  
- Local JSON file sizes – zero local disk usage for the actual files.  
- Category breakdown with file counts and total size per category.  
- Optional pie chart (requires `matplotlib`).

---

## 🚀 Quick Setup

### Step 1: Create Your Telegram Bot with BotFather

- 1. Open Telegram and search for @BotFather (look for the verified blue checkmark)

- 2. Start a chat and send /newbot command

- 3. Follow the prompts:

*** Choose a name for your bot (e.g., "MyCloudStorage")

*** Choose a username (must end with "bot", e.g., "MyCloudStorageBot")

- 4. Save your token! BotFather will give you an API token that looks like:
eg:
```python
1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
```

---

### Step 2: Choose Your Storage Location

- Simple Option (No channel needed): Private Chat (Simplest)
Just send a message to your bot directly
No special permissions needed.

### Step 3: Install the Cloud Storage System
- I'll guide you through using a popular, well-maintained solution called "Telegram Cloud Storage"

*** Open CMD ( Run As Administrator)
```python
cd C:\Projects
git clone https://github.com/RizkyFauzi16/Telegram-Storage-Bot.git
cd Telegram-Storage-Bot
```

### 📥 Step 4: Install Python (If Not Installed)
Download Python:

Go to -<a href="https://www.python.org/downloads/">python.org/downloads</a>


### Step 5: Install Microsoft C++ Build Tools (Recommended if not installed)
Download Visual C++ Build Tools:

Go to: -<a href="https://visualstudio.microsoft.com/visual-cpp-build-tools/">https://visualstudio.microsoft.com/visual-cpp-build-tools/</a>

Click "Download Build Tools"
```python
Install:

Run the downloaded installer

Select "Desktop development with C++" workload

Click Install (this may take 10-15 minutes)

Restart your computer (IMPORTANT!)

After restart, install the step below:
```

*** Step 6: Install required software:
- copy & paste this to CMD
```python
pip install -r requirements.txt
pip install python-telegram-bot
pip install pyrogram
pip install tgcrypto
pip install pillow
pip install psutil
pip install matplotlib
```
- Or Install All Packages One by One:
```python
pip install pyrogram pyromod python-dotenv pymongo dnspython aiohttp
```

---

🎯 ### After Installation
In CMD:
```
notepad config.py
```
*** Add your bot token from BotFather
- eg: You'll need to fill in these values (look for lines like these):
-<a href="https://github.com/Donovandonz/Telegram--Cloud-Storage-Bot/blob/main/env">.env</a>
```python
API_ID = ""           # Leave blank or get from my.telegram.org
API_HASH = ""         # Leave blank or get from my.telegram.org
BOT_TOKEN = ""        # Put your bot token here
OWNER_ID = ""         # Put your Telegram user ID here
```
*** After that, to save and close: File → Save, then close Notepad. (Note: save the file name as .env)

### 🔍 How to Use @userinfobot to get your "OWNER ID"
- Search for @userinfobot in Telegram

- Start the bot by clicking "Start" or sending /start

- It will instantly reply with your numeric user ID (e.g., 123456789)

### ⚠️ Important Security Note
-While @userinfobot itself is legitimate, be aware of impostor bots that use similar names. Telegram warns about fake accounts that impersonate famous services . Always double-check that:

- The username is exactly @userinfobot (not @userinfo_bot or similar variations)

- The bot has a verified checkmark

---

### Step 7: Run Your Bot

- In CMD: Run this!
```python
python bot.py
OR
py bot.py
```
- You should see something like:
Bot Started!

---

### Step 8: 
- In CMD: Run this again!
```python
notepad config.py
```
### Copy this code👇 and rename this file as bot.py then save

-<a href="https://github.com/Donovandonz/Telegram--Cloud-Storage-Bot/blob/main/bot.py">bot.py</a>

In this code find:
- Enter your user ID where it says OWNER_ID = "" (remove the quotes and paste the number)

- Enter your bot token from BotFather where it says BOT_TOKEN = ""

- After saving the config file, you're ready to run your bot with:
```python
py bot.py
```
---

### 📱 Testing Your Bot
- Open Telegram

- Find your bot by its username

- Send /start to your bot

- You should get a response!

### 📁 File Storage Feature
- This bot creates a file sharing system where:

- Users send files to the bot

- The bot stores them with a shareable link

- You can manage files through the bot

# 🎉 Congratulations!
You've successfully set up a Telegram cloud storage bot! Your bot can now store and share files through Telegram.

---

# Setup to Auto-run your cloud bot storage everytime you're log on PC/Laptop

*** Step 1: In CMD 
```python
notepad config.py
```
*** Save this file code👇
-<a href="https://github.com/Donovandonz/Telegram--Cloud-Storage-Bot/blob/main/start_bot_silent.vbs">start_bot_silent.vbs</a>

Go-to:
- Task Scheduler (recommended, works plugged in or on battery)
- This is the most reliable way. Windows will auto-start the bot every time you log in, and restart it if it crashes.

- Press Win + R, type taskschd.msc, press Enter
- Click Create Basic Task on the right
- Name it CloudStorageBot, click Next
- Trigger: When I log on, click Next
- Action: Start a program, click Next
- Program: browse to your python.exe (usually C:\Users\YourName\AppData\Local\Programs\Python\Python3xx\python.exe)
- Arguments: bot.py
- Start in: C:\Projects\SimpleStorageBot (your bot folder path)
- Finish → right-click the task → Properties → check Run whether user is logged on or not if you want it to run even on lock screen

---

# IMPORTANT NOTE
- Save your all files (.env, bot.py etc in the folder) to your own backup drive.
- Because when you buy a new PC/Laptop your gonna need this file to paste on your new device.
- Just follow the first step guide installation Python, C++ installation provide.
- Then just paste the files to your new file folder and run the bot cloud storage like usuall.
- That's all have a nice day and have your own cloud storage bot safe in your own hands!
- BYE ! 👋



















