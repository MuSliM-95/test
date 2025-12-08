from typing import Union
import phonenumbers


class RuPhone(str):
    """Кастомный тип для российских телефонных номеров"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value: Union[str, 'RuPhone', None]) -> Union[str, None]:
        # Обработка None
        if value is None:
            return None

        # Если уже RuPhone, возвращаем как строку
        if isinstance(value, RuPhone):
            return str(value)

        # Проверка типа
        if not isinstance(value, str):
            raise TypeError('string required')

        # Пустая строка
        if not value:
            return None

        try:
            # Очистка номера от лишних символов
            cleaned = ''.join(filter(str.isdigit, value))

            # Если пусто после очистки
            if not cleaned:
                return None

            # Нормализация российских номеров
            if cleaned.startswith('8'):
                cleaned = '7' + cleaned[1:]
            elif not cleaned.startswith('7') and len(cleaned) == 10:
                # 10 цифр без кода страны - считаем российским
                cleaned = '7' + cleaned
            elif not cleaned.startswith('7'):
                # Другие варианты оставляем как есть
                pass

            # Форматируем с +
            formatted = f"+{cleaned}" if not cleaned.startswith('+') else cleaned

            # Парсим номер
            try:
                parsed = phonenumbers.parse(formatted, "RU")
            except phonenumbers.NumberParseException:
                # Попробуем автоопределение региона
                parsed = phonenumbers.parse(formatted, None)

            # Проверяем валидность
            if not phonenumbers.is_valid_number(parsed):
                raise ValueError("Invalid phone number")

            # Возвращаем в формате E164
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)

        except phonenumbers.NumberParseException:
            raise ValueError("Invalid phone number format")
        except Exception:
            raise ValueError("Invalid phone number format")
