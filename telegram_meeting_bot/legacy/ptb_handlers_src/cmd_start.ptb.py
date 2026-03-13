async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _set_log_user(update)
    user = update.effective_user
    text = menu_text_for(update.effective_chat.id)
    await reply_text_safe(update.message, 
        text, reply_markup=main_menu_kb(is_admin(user)), parse_mode="Markdown"
    )
