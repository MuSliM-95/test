from pathlib import Path

# Каталог для загрузки изображений категорий
UPLOAD_DIR = Path("/uploads/categories")
# Разрешённые расширения файлов изображений
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
# Максимальный размер файла (5 МБ)
MAX_UPLOAD_SIZE = 5 * 1024 * 1024
