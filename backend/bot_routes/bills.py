
import re
import logging
import os
import pytz
import json
from datetime import datetime
import requests


from aiogram import  Router, types, F
from aiogram.client.session import aiohttp

from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.fsm.context import FSMContext
from typing import Dict, Any
from database.db import database,  users
from typing import Dict, Any

from common.s3_service.impl.S3ServiceFactory import S3ServiceFactory
from common.s3_service.models.S3SettingsModel import S3SettingsModel
from bot_routes.pdf_reader import extract_text_from_pdf_images
from bot_routes.bills_model import CreateBillData, UpdateBillData, CreateBillApproverData, UpdateBillApproverData, BillApproveStatus, BillStatus, get_approve_by_id_and_approver, get_approvers_by_bill, check_user_permissions, create_bill, format_bill_notification, get_bill, update_bill_status, update_bill, create_bill_approver, update_bill_approve

timezone = pytz.timezone("Europe/Moscow")

# Определение состояний для FSM
class BillDateForm(StatesGroup):
    start = State()
    waiting_for_date = State()


def convert_unicode_to_text(text):
    """
    Converts Unicode escape sequences (like \\u0418) in a string to human-readable text.
    Handles different input types (string, bytes).

    Args:
        text: The string or bytes containing Unicode escape sequences.

    Returns:
        The string with Unicode escape sequences converted to text.
    """
    if isinstance(text, bytes):
        text = text.decode('utf-8')  # Decode bytes to string

    try:
        return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        # If the text is not a valid JSON string, return it as is
        return text
    


def replace_newlines_with_spaces(text):
    """Replaces all newline characters in a string with spaces.

    Args:
        text: The input string.

    Returns:
        The string with all newline characters replaced by spaces.
    """
    return text.replace('\n', ' ')

def validate_inn(inn: str) -> bool:
    inn = ''.join(filter(str.isdigit, inn))
    if len(inn) not in (10, 12):
        return False
    
    weights_10 = [2, 4, 10, 3, 5, 9, 4, 6, 8]
    weights_12 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
    
    try:
        if len(inn) == 10:
            check = sum(int(c) * w for c, w in zip(inn[:9], weights_10)) % 11 % 10
            return check == int(inn[9])
        else:
            # Проверка 11-й цифры
            check11 = sum(int(c) * w for c, w in zip(inn[:10], weights_12)) % 11 % 10
            # Проверка 12-й цифры
            weights_12[-1] = 8
            check12 = sum(int(c) * w for c, w in zip(inn[:11], weights_12)) % 11 % 10
            return check11 == int(inn[10]) and check12 == int(inn[11])
    except:
        return False

def validate_bic_with_corr_account(bic: str, corr_account: str) -> bool:
    bic_digits = ''.join(filter(str.isdigit, bic)).zfill(9)
    corr_digits = ''.join(filter(str.isdigit, corr_account)).zfill(20)
    
    # Последние 3 цифры БИК должны совпадать с 9-11 цифрами коррсчета
    if len(bic_digits) != 9 or len(corr_digits) != 20:
        return False
    
    return bic_digits[-3:] == corr_digits[9:12]

def validate_bic_region(bic: str) -> bool:
    region_code = bic[4:6]
    return region_code in []#VALID_REGION_CODES  # Загрузить справочник регионов

def normalize_number(raw: str) -> str:
    return ''.join(filter(str.isdigit, raw))

def find_corr_account(text: str, control_number) -> str:
    # Найти строку, содержащую "Корр. счет"
    match = re.search(r"(\d{20})", text)
    if match:
        text = text.replace(match.group(1), '')
        if match.group(1)[-3:] == control_number:
            return match.group(1), text
        else:
            return find_corr_account(text, control_number)
    else:
        
        return None, text

def process_text_test(text):
    text = re.sub(r'(\d)\s+(\d)', r'\1\2', text)
    text = text.replace('|', ' ')
    patterns = {
        "bic": r"\s(\d{9})(?!\d)",              # БИК
        "corr_account": r"\s(\d{20})(?!\d)",  # Корр. счет
        "pc": r"\s(\d{20})(?!\d)",
        "inn": r"\s(\d{10,12})(?!\d)",
    }

    result = {}
   
    result["bic"] = re.search(patterns["bic"], text).group(1) if re.search(patterns["bic"], text) else None
    if result["bic"]:
        text = text.replace(result["bic"], '')
    corr_account, text = find_corr_account(text, result["bic"][-3:])

    if corr_account:
        result["corr_account"] = corr_account
    else:
        result["corr_account"] = None

    result["pc"] = re.search(patterns["pc"], text).group(1) if re.search(patterns["pc"], text) else None
    if result["pc"]:
        text = text.replace(result["pc"], '')

    result["inn_seller"] = re.search(patterns["inn"], text).group(1) if re.search(patterns["inn"], text) else None
    if result["inn_seller"]:
        text = text.replace(result["inn_seller"], '')
    result["inn_buyer"] = re.search(patterns["inn"], text).group(1) if re.search(patterns["inn"], text) else None
    if result["inn_buyer"]:
        text = text.replace(result["inn_buyer"], '')

    return result

    

def process_text(text):
    """
    Извлечение данных из текста с использованием регулярных выражений.
    
    Аргументы:
        text (str): Входной текст
    
    Возврат:
        dict: Структурированные извлеченные данные
    """
    # Регулярные выражения для поиска данных
    patterns = {
        "seller": r"Продавец\s+(.*?ИНН \d+)",  # Продавец (ФИО) + ИНН
        "seller_inn": r"ИНН\s+(\d{10,12})",   # ИНН продавца
        "bank": r"АО\s+\"(.+?)\"",            # Название банка
        "bic": r"БИК\s+(\d{9})",              # БИК
        "corr_account": r"(\d{20})",  # Корр. счет
        "payment_account": r"Расчетный счет\s+(\d{20})",  # Расчетный счет
        "buyer": r"Индивидуальный предприниматель (.+?)\s+ИНН",  # Покупатель (ФИО)
        "buyer_inn": r"ИНН\s+(\d{10,12})",    # ИНН покупателя
        "invoice_number": r"Счёт на оплату №(\d+)",  # Номер счета
        "invoice_date": r"от\s+(\d{1,2} \w+ \d{4})",  # Дата счета
        "items": r"(\d+)\s+(.+?)\s+(\d+)\s+([\d\s]+,\d{2})\s+([\d\s]+,\d{2})", # Таблица товаров
        "total": r"Итого к оплате:\s+([\d\s]+,\d{2})",  # Итоговая сумма
        "total_words": r"([\w\s]+рублей \d{2} копеек)"  # Сумма прописью
    }
    
    # Извлечение данных по регулярным выражениям
    result = {}
    result["seller"] = re.search(patterns["seller"], text, re.DOTALL).group(1).strip() if re.search(patterns["seller"], text, re.DOTALL) else None
    if result["seller"]:
        text = text.replace(result["seller"], '')
    result["seller_inn"] = re.search(patterns["seller_inn"], text).group(1) if re.search(patterns["seller_inn"], text) else None
    if result["seller_inn"]:
        text = text.replace(result["seller_inn"], '')
    result["bank_name"] = re.search(patterns["bank"], text).group(1) if re.search(patterns["bank"], text) else None
    if result["bank_name"]:
        text = text.replace(result["bank_name"], '')
    result["bic"] = re.search(patterns["bic"], text).group(1) if re.search(patterns["bic"], text) else None
    if result["bic"]:
        text = text.replace(result["bic"], '')
    result["corr_account"] = re.search(patterns["corr_account"], text).group(1) if re.search(patterns["corr_account"], text) else None
    if result["corr_account"]:
        text = text.replace(result["corr_account"], '')
    result["payment_account"] = re.search(patterns["payment_account"], text).group(1) if re.search(patterns["payment_account"], text) else None
    if result["payment_account"]:
        text = text.replace(result["payment_account"], '')
    result["buyer"] = re.search(patterns["buyer"], text, re.DOTALL).group(1).strip() if re.search(patterns["buyer"], text, re.DOTALL) else None
    if result["buyer"]:
        text = text.replace(result["buyer"], '')
    result["buyer_inn"] = re.search(patterns["buyer_inn"], text).group(1) if re.search(patterns["buyer_inn"], text) else None
    if result["buyer_inn"]:
        text = text.replace(result["buyer_inn"], '')
    result["invoice_number"] = re.search(patterns["invoice_number"], text).group(1) if re.search(patterns["invoice_number"], text) else None
    if result["invoice_number"]:
        text = text.replace(result["invoice_number"], '')    
    result["invoice_date"] = re.search(patterns["invoice_date"], text).group(1) if re.search(patterns["invoice_date"], text) else None
    result["total"] = re.search(patterns["total"], text).group(1).replace(' ', '') if re.search(patterns["total"], text) else None
    result["total_words"] = re.search(patterns["total_words"], text).group(1) if re.search(patterns["total_words"], text) else None

    # Извлечение данных из таблицы товаров
    items_pattern = re.findall(patterns["items"], text, re.DOTALL)
    result["items"] = []
    if items_pattern:
        for item in items_pattern:
            result["items"].append({
                "number": int(item[0]),
                "description": item[1].strip(),
                "quantity": int(item[2]),
                "price": float(item[3].replace(' ', '').replace(',', '.')),
                "amount": float(item[4].replace(' ', '').replace(',', '.'))
            })

    return result

def process_text2(text: str) -> Dict[str, Any]:
    patterns = {
        "bik": r"БИК\s+(\d{9})",
        "seller_inn": r"ИНН[:\s_]+(\d{10,12})",
        "account_number": r"[Сс]ч[.\s№]+\s*(\d{20})",
        "total_amount": r"Всего к оплате:\s+([\d\s]+,\d{2})",
        "date": r"от (\d{1,2} \w+ \d{4}) г",
        "contract_number": r"Договор заявка\s+([\w-]+)",
        "seller": r"Поставщик[^\n]+\nИП ([^\n]+)",
        "buyer": r"Покупатель[^\n]+\nИП ([^\n]+)",
    }

    result = {}
    
    # Основные поля
    for field, pattern in patterns.items():
        if isinstance(pattern, dict):
            result[field] = {}
            for sub_field, sub_pattern in pattern.items():
                match = re.search(sub_pattern, text, re.IGNORECASE)
                result[field][sub_field] = match.group(1).strip() if match else None
        else:
            match = re.search(pattern, text, re.IGNORECASE)
            result[field] = match.group(1).strip() if match else None

    # Обработка сумм
    if result.get('total_amount'):
        result['total_amount'] = float(
            result['total_amount']
            .replace(' ', '')
            .replace(',', '.')
        )

    return result



# Функция для создания клавиатуры
def create_bill_action_keyboard(bill):
    error_keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="Сместить дату", callback_data=f'{{"action": "change_date", "bill_id": {bill.id}}}'),
            ],
            [
                types.InlineKeyboardButton(text="Добавить сегодняшним числом", callback_data=f'{{"action": "update_bill_payment_date", "bill_id": {bill.id}}}'),
            ],
            [
                types.InlineKeyboardButton(text="Отмена", callback_data=f'{{"action": "cancel_bill", "bill_id": {bill.id}}}'),
            ]
        ]
    )
    return error_keyboard

def create_like_dislike_keyboard(bill_id: int):
    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="👍 Like", callback_data=f'{{"action": "like", "bill_id": {bill_id}}}'),
                types.InlineKeyboardButton(text="👎 Dislike", callback_data=f'{{"action": "dislike", "bill_id": {bill_id}}}'),
            ]
        ]
    )
    return keyboard

async def get_user_from_db(user_id: str):
    """Fetches a user from the database based on user_id."""
    query = users.select().where(users.c.chat_id == user_id)
    return await database.fetch_one(query)


async def get_bill_approvers_data(bill_id: int):
    """Retrieves and formats bill approvers data."""
    approvers_db = await get_approvers_by_bill(bill_id)
    approvers = []
    for approver in approvers_db:
        query = users.select().where(users.c.id == approver.approver_id)
        result = await database.fetch_one(query)
        approvers.append({
            "id": approver.id,
            "approver_id": approver.approver_id,
            "bill_id": approver.bill_id,
            "status": approver.status,
            "username": result.username,
        })
    return approvers

async def update_bill_status_based_on_approvals(bill_id: int):
    """
    Updates the bill status based on the approval statuses and payment date.
    """
    bill = await get_bill(bill_id)
    if not bill:
        logging.warning(f"Bill with id {bill_id} not found.")
        return

    approvers = await get_approvers_by_bill(bill_id)
    if not approvers:
        # If there are no approvers, the bill is considered approved.
        await update_bill_status(bill_id, BillStatus.approved)
        return

    all_approved = all(approver.status == BillApproveStatus.approved for approver in approvers)
    any_rejected = any(approver.status == BillApproveStatus.canceled for approver in approvers)

    if any_rejected:
        await update_bill_status(bill_id, BillStatus.rejected)
    elif all_approved:
        await update_bill_status(bill_id, BillStatus.approved)
    else:
        await update_bill_status(bill_id, BillStatus.waiting_for_approval)

# Функция для получения маршрута
def get_bill_route(bot):
    pdf_router = Router()

    async def check_user_registration(callback_query: types.CallbackQuery):
        """Checks if the user is registered in the system."""
        user = await get_user_from_db(str(callback_query.from_user.id))
        if not user:
            await bot.send_message(chat_id=callback_query.message.chat.id, text="Вы не зарегистрированы в системе. Пожалуйста, зарегистрируйтесь, чтобы продолжить.")
            return False
        return user


    async def send_bill_notification(chat_id: int, notification_string: str, bill_id: int = None):
        """Sends a bill notification message with optional like/dislike keyboard."""
        if bill_id:
            await bot.send_message(chat_id=chat_id, text=notification_string,
                                reply_markup=create_like_dislike_keyboard(bill_id),
                                parse_mode="html")
        else:
            await bot.send_message(chat_id=chat_id, text=notification_string, parse_mode="html")


    @pdf_router.callback_query(lambda call: True)
    async def callback_query(callback_query: types.CallbackQuery, state: FSMContext):
        user_id = str(callback_query.from_user.id)
        await bot.answer_callback_query(callback_query.id)
        data = json.loads(callback_query.data)
        bill_id = data['bill_id']

        user_permissions = await check_user_permissions(user_id, bill_id)
        if not user_permissions:
            user = await get_user_from_db(user_id)
            await bot.answer_callback_query(callback_query.id, text=f"У {user.first_name} нет прав на действие {data['action']}, Номер счета: {bill_id}")
            return

        await state.update_data(bill_id=bill_id)
        old_bill = await get_bill(bill_id)

        if data['action'] == 'change_date':
            await bot.answer_callback_query(callback_query.id)
            await callback_query.message.reply("Введите новую дату в формате ГГГГ-ММ-ДД:")
            await state.set_state(BillDateForm.waiting_for_date)

        elif data['action'] == 'update_bill_payment_date':
            await bot.answer_callback_query(callback_query.id)  # Подтверждаем нажатие кнопки
            state_data = await state.get_data()
            naive_date =datetime.now()
            localized_date = timezone.localize(naive_date)
            await update_bill(state_data['bill_id'],{"payment_date": localized_date, "status": BillStatus.waiting_for_approval})
            
            new_bill = await get_bill(bill_id)

            approvers = await get_bill_approvers_data(new_bill.id)
            await update_bill_status_based_on_approvals(new_bill.id)
            notification_string = await format_bill_notification(
                bill_id=old_bill.id,
                created_by=old_bill.created_by,
                s3_url=old_bill.s3_url,
                file_name=old_bill.file_name,
                approvers=approvers,
                new_payment_date=new_bill.payment_date,
                old_payment_date=old_bill.payment_date,
                old_status=old_bill.old_status,
                new_status=new_bill.status,
                updated_by=user_id
            )
            await state.set_state(None)
            await send_bill_notification(callback_query.message.chat.id, notification_string, new_bill.id)

        elif data['action'] == 'cancel_bill':
            await bot.answer_callback_query(callback_query.id)  # Подтверждаем нажатие кнопки
            state_data = await state.get_data()
            await update_bill_status(state_data['bill_id'], "canceled")
            
            new_bill = await get_bill(bill_id)

            approvers = await get_bill_approvers_data(new_bill.id)
            
            notification_string = await format_bill_notification(
                bill_id=old_bill.id,
                created_by=old_bill.created_by,
                s3_url=old_bill.s3_url,
                file_name=old_bill.file_name,
                approvers=approvers,
                new_payment_date=new_bill.payment_date,
                old_payment_date=old_bill.payment_date,
                old_status=old_bill.old_status,
                new_status=new_bill.status,
                updated_by=user_id
            )
            await state.set_state(None)
            await send_bill_notification(callback_query.message.chat.id, notification_string)

        elif data['action'] == 'like':
            user = await check_user_registration(callback_query)
            if not user:
                return

            approve = await get_approve_by_id_and_approver(user.id, bill_id)
            if not approve:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Не достпно для {user.username}, так как не является утверждающим.")
                return

            if approve.status != BillApproveStatus.approved:
                await update_bill_approve(approve.id, {'status':  BillApproveStatus.approved})
                await update_bill_status_based_on_approvals(bill_id)
                new_bill = await get_bill(bill_id)

                approvers = await get_bill_approvers_data(new_bill.id)

                notification_string = await format_bill_notification(
                    bill_id=old_bill.id,
                    created_by=old_bill.created_by,
                    s3_url=old_bill.s3_url,
                    file_name=old_bill.file_name,
                    approvers=approvers,
                    new_payment_date=new_bill.payment_date,
                    old_payment_date=old_bill.payment_date,
                    old_status=old_bill.old_status,
                    new_status=new_bill.status,
                    updated_by=user_id
                )

                await send_bill_notification(callback_query.message.chat.id, notification_string)

                if new_bill.status:
                    await bot.send_message(chat_id=callback_query.message.chat.id, text="Счет утвержден.")
                await state.set_state(None)

        elif data['action'] == 'dislike':
            user = await check_user_registration(callback_query)
            if not user:
                return

            approve = await get_approve_by_id_and_approver(user.id, bill_id)
            if not approve:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Не достпно для {user.username}, так как не является утверждающим.")
                return

            await bot.answer_callback_query(callback_query.id)
            await callback_query.message.reply("Введите новую дату в формате ГГГГ-ММ-ДД:")
            await state.set_state(BillDateForm.waiting_for_date)

    @pdf_router.message(state=BillDateForm.waiting_for_date)
    async def process_payment_date(message: types.Message, state: FSMContext):
        user_date = message.text
        user_id = message.from_user.id
        try:
            state_data = await state.get_data()
            old_bill = await get_bill(state_data['bill_id'])
            datetime.strptime(user_date, "%Y-%m-%d")
            state_data = await state.get_data()
            naive_date = datetime.strptime(user_date, "%Y-%m-%d")
            localized_date = timezone.localize(naive_date)
            await update_bill(state_data['bill_id'],{"payment_date": localized_date, "status": BillStatus.waiting_for_approval})
            await update_bill_status_based_on_approvals(old_bill.id)
            new_bill = await get_bill(state_data['bill_id'])

            approvers = await get_bill_approvers_data(new_bill.id)

            notification_string = await format_bill_notification(
                bill_id=old_bill.id,
                created_by=old_bill.created_by,
                s3_url=old_bill.s3_url,
                file_name=old_bill.file_name,
                approvers=approvers,
                new_payment_date=new_bill.payment_date,
                old_payment_date=old_bill.payment_date,
                old_status=old_bill.old_status,
                new_status=new_bill.status,
                updated_by=user_id
            )
            await state.set_state(None)
            await send_bill_notification(message.chat.id, notification_string, new_bill.id)

        except ValueError:
            await message.reply("Пожалуйста, введите дату в правильном формате (ГГГГ-ММ-ДД):")
            return

        await state.set_state(None)

    @pdf_router.message(lambda message: message.text.isdigit(), state=BillDateForm.waiting_for_date)
    async def process_invalid_date(message: types.Message):
        await message.reply("Пожалуйста, введите дату в правильном формате (ГГГГ-ММ-ДД):")

    @pdf_router.message(F.document.mime_type == "application/pdf")
    async def handle_pdf(message: types.Message, state: FSMContext):
        chat_id = message.chat.id
        user_id = message.from_user.id
        try:
            await state.set_data({})
            file_id = message.document.file_id
            file_name = message.document.file_name
            file = await bot.get_file(file_id)

            # Получаем URL для скачивания файла
            file_url = f'https://api.telegram.org/file/bot{bot.token}/{file.file_path}'
            s3_factory = S3ServiceFactory(
                s3_settings=S3SettingsModel(
                    aws_access_key_id=os.getenv('S3_ACCESS'),
                    aws_secret_access_key=os.getenv('S3_SECRET'),
                    endpoint_url=os.getenv('S3_URL')
                )
            )

            # Скачиваем файл
            async with aiohttp.ClientSession() as session:
                async with session.get(file_url) as response:
                    if response.status == 200:
                        file_bytes = await response.read()
                        s3_client = s3_factory()
                        await s3_client.upload_file_object(file_bytes=file_bytes, bucket_name='tg-bills', file_key=file_id)
                        file_url=f'{os.getenv("S3_URL")}/tg-bills/{file_id}'
                        await state.set_data({'file_url': file_url})
                        bill_text = extract_text_from_pdf_images(file_bytes)
                        if bill_text:
                            await message.reply(bill_text)
                            extracted_data = process_text_test(bill_text)              
                            if extracted_data:
                                print("Extracted Data:")
                                for key, value in extracted_data.items():
                                    print(f"{key}: {value}")
                            else:
                                print("Failed to extract data from the invoice.")
                          
                            #await message.reply(extracted_data[])
                            #for admin_id in admin_list:
                                #await bot.send_message(admin_id, response_text)
                            await state.set_state(BillDateForm.start)
                            query = users.select().where(users.c.chat_id == str(user_id))
                            user = await database.fetch_one(query)
                            bill_data: CreateBillData = dict(
                                created_by=user.id,
                                status='new',
                                s3_url=file_url,
                                file_name=file_name,
                                plain_text=bill_text
                            )
                            try:
                                bill = await create_bill(bill_data)
                                bill = await get_bill(bill)
                                bill_approvers = []
                                if message.caption_entities:
                                    for entity in message.caption_entities:
                                        if entity.type == "mention":
                                            user_id = message.caption[entity.offset+1:entity.offset+entity.length]
                                            query = users.select().where(users.c.username == str(user_id) and users.c.chat_id == users.c.owner_id)
                                            user = await database.fetch_one(query)
                                            if user:
                                                bill_approver = CreateBillApproverData(
                                                    bill_id=bill.id,
                                                    approver_id=user.id,
                                                    status='new'
                                                )
                                                approve_id = await create_bill_approver(bill_approver)
                                                bill_approvers.append({
                                                    'approver_id': user.id,
                                                    'username': user.username,
                                                    'id': approve_id,
                                                    'status': 'new'
                                                })
                                        
                                notification_string = await format_bill_notification(
                                    bill_id=bill.id,
                                    created_by=bill.created_by,
                                    s3_url=bill.s3_url,
                                    file_name=bill.file_name,
                                    approvers=bill_approvers,
                                    new_payment_date=bill.payment_date,
                                    old_payment_date=bill.payment_date,
                                    old_status=bill.old_status,
                                    new_status=bill.status,
                                    updated_by=user_id,
                                    new_bill=True
                                )
                                await message.reply(notification_string, 
                                    reply_markup=create_bill_action_keyboard(bill), 
                                    parse_mode="HTML")            
                            except Exception as e:
                                await message.reply(f"Произошла ошибка: {e}")
                                return
                           
                            
                        else:
                            await message.reply("Не удалось извлечь текст из файла.")
                    else:
                        await message.reply("Не удалось скачать файл.")
                        return

        except Exception as e:
            await message.reply(f"Произошла ошибка: {e}")

    return pdf_router
