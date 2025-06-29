import re

def replace_masks(text: str, replacements: dict) -> str:
    def replacer(match):
        key = match.group(1).strip()  # вытаскиваем имя переменной без {{ }}
        return str(replacements.get(key, match.group(0)))  # если нет значения — оставить маску

    return re.sub(r"\{\{\s*(.*?)\s*\}\}", replacer, text)