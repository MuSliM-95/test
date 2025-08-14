def format_contragent_text_notifications(action: str, segment_name: str, name: str, phone: str):
    if action == "new_contragent":
        header = "Новый пользователь добавлен в сегмент!"
    else:
        header = "Пользователь исключен из сегмента!"
    return f"{header}\nСегмент: {segment_name}.\nКлиент:\n{name}\nТелефон: {phone}"