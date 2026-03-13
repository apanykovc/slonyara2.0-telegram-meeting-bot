async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    user = update.effective_user
    text = show_help_text(update)
    try:
        await reply_text_safe(update.message, 
            text, reply_markup=main_menu_kb(is_admin(user)), parse_mode="Markdown"
        )
    except Exception:
        await reply_text_safe(update.message, 
            text, reply_markup=main_menu_kb(is_admin(user))
        )
