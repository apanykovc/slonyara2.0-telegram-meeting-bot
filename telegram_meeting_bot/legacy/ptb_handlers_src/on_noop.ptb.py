async def on_noop(update: Update, _context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    try:
        await q.answer("⏳ Уже выполняю…", cache_time=1)
    except Exception:
        pass

