"""
Entry point that launches the Aiogram v3 app by default.
Use LEGACY_PTB=1 to run the old PTB engine.
"""
import os
import asyncio
from importlib import import_module

def run_aiogram():
    mod = import_module("telegram_meeting_bot.aiogram_app.app")
    return asyncio.run(mod.main())

def run_ptb():
    mod = import_module("telegram_meeting_bot.bot.main")
    return mod.main()

if __name__ == "__main__":
    if os.environ.get("LEGACY_PTB") == "1":
        run_ptb()
    else:
        run_aiogram()
