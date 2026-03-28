Set WshShell = CreateObject("WScript.Shell")
Do
    WshShell.Run "cmd /c cd /d C:\Projects\SimpleStorageBot && py bot.py", 0, True
    WshShell.Run "cmd /c timeout /t 5", 0, True
Loop
