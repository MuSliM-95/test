
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
from bot_routes.bills_model import *
from bot_routes.keyboards import *



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

def find_counterparty_account_number(text: str, control_number) -> str:
    # Найти строку, содержащую "Корр. счет"
    match = re.search(r"(\d{20})", text)
    if match:
        text = text.replace(match.group(1), '')
        if match.group(1)[-3:] != control_number:
            return match.group(1), text
        else:
            return find_counterparty_account_number(text, control_number)
    else:
        
        return None, text


def process_text_rus(text):
    text = re.sub(r'(\d)\s+(\d)', r'\1\2', text)
    text = text.replace(' — ', ': ')
    text = text.replace('|', ' ')

    print(text)
    patterns = {
        "reason_2": r"([^\n]+?)\s*№(?!\d)",
        "reason": r"Основание:\s*([^\n]+)",
        "sum": r"(?:на сумму|(?:итог )?к оплате:)\s*([\d\s]+[.,]\d{2})",
        "supplier": r"(?:руб\.|копеек)\s*\n\s*([А-ЯЁ\s]+)\s*\n",
        "supplier_2": r"Поставщик:?\s*([^,\n]+?)(?:,|\s+ИНН)"
    }
   
    result = {}
   
 
    result["payment_amount"] = re.search(patterns["sum"], text).group(1) if re.search(patterns["sum"], text) else None
    if result["payment_amount"]:
        text = text.replace(result["payment_amount"], '')
        result["payment_amount"] = result["payment_amount"].replace(' ', '').replace(',', '.')
        # Will return 16000.00 as float
        result["payment_amount"] = float(result["payment_amount"])
    result["payment_purpose"] = re.search(patterns["reason"], text).group(1) if re.search(patterns["reason"], text) else None
    if result["payment_purpose"]:
        text = text.replace(result["payment_purpose"], '')
    else:
        result["payment_purpose"] = re.search(patterns["reason_2"], text).group(1) if re.search(patterns["reason_2"], text) else None
        if result["payment_purpose"]:
            text = text.replace(result["payment_purpose"], '')
        else:
            result["payment_purpose"] = None
    supplier_match = re.search(patterns["supplier"], text)
    if supplier_match:
        result["counterparty_name"] = supplier_match.group(1).strip()
    else:
        supplier_match = re.search(patterns["supplier_2"], text)
        if supplier_match:
            result["counterparty_name"] = supplier_match.group(1).strip()

    return result

    

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
   
    result["counterparty_bank_bic"] = re.search(patterns["bic"], text).group(1) if re.search(patterns["bic"], text) else None
    if result["counterparty_bank_bic"]:
        text = text.replace(result["counterparty_bank_bic"], '')
    counterparty_account_number, text = find_counterparty_account_number(text, result["counterparty_bank_bic"][-3:])

    if counterparty_account_number:
        result["counterparty_account_number"] = counterparty_account_number
    else:
        result["counterparty_account_number"] = None

    #result["inn_seller"] = re.search(patterns["inn"], text).group(1) if re.search(patterns["inn"], text) else None
    #if result["inn_seller"]:
    #    text = text.replace(result["inn_seller"], '')
    #result["inn_buyer"] = re.search(patterns["inn"], text).group(1) if re.search(patterns["inn"], text) else None
    #if result["inn_buyer"]:
    #    text = text.replace(result["inn_buyer"], '')
    return result
 

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


    


    @pdf_router.callback_query(lambda call: True)
    async def callback_q(callback_query: types.CallbackQuery, state: FSMContext):
        user_id = str(callback_query.from_user.id)
        await bot.answer_callback_query(callback_query.id)
        data = json.loads(callback_query.data)
        bill_id = int(data['bill_id'])

        new_bill = await get_bill(bill_id)

        if new_bill['status'] == BillStatus.canceled:
            await bot.answer_callback_query(callback_query.id, text=f"Счет {bill_id} отменен")
            return
        old_bill = dict(new_bill)
        approvers = await get_bill_approvers_data(new_bill.id)

        user_permissions = await check_user_permissions(user_id, bill_id)
        if not user_permissions:
            user = await get_user_from_db(user_id)
            await bot.answer_callback_query(callback_query.id, text=f"У {user.first_name} нет прав на действие {data['action']}, Номер счета: {bill_id}")
            return

        await state.update_data(bill_id=bill_id)


        if data['action'] == 'select_tb_account':
            await update_bill(bill_id, {"tochka_bank_account_id":data['account_id']})
        elif data['action'] == 'change_date':
            if 'data' in data:
                naive_date = datetime.strptime(data['data'], "%Y-%m-%d")
                localized_date = timezone.localize(naive_date)
                await update_bill(bill_id, {"payment_date":localized_date, "status": BillStatus.waiting_for_approval})
            else:
                await bot.answer_callback_query(callback_query.id)
                await callback_query.message.reply("Введите новую дату в формате ГГГГ-ММ-ДД:")
                await state.set_state(BillDateForm.waiting_for_date)
                return

        elif data['action'] == 'cancel_bill':
            
            await update_bill_status(bill_id, BillStatus.canceled)

          
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

                approvers = await get_bill_approvers_data(new_bill.id)


        elif data['action'] == 'dislike':
            user = await check_user_registration(callback_query)
            if not user:
                return

            approve = await get_approve_by_id_and_approver(user.id, bill_id)
            if not approve:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Не достпно для {user.username}, так как не является утверждающим.")
                return
            
            await update_bill_approve(approve.id, {'status':  BillApproveStatus.canceled})
            await update_bill_status_based_on_approvals(bill_id)
            
            approvers = await get_bill_approvers_data(new_bill.id)
            
            await callback_query.message.reply("Введите новую дату в формате ГГГГ-ММ-ДД:")
        
        elif data['action'] == 'send_bill':
            await send_bill(new_bill, callback_query, bot)
 
        new_bill = await get_bill(bill_id)
        notification_string = await format_bill_notification(
            created_by=new_bill.created_by,
            approvers=approvers,
            updated_by=user_id,
            new_bill=new_bill,
            old_bill=old_bill,
        )
        await callback_query.message.reply(notification_string, 
                                    reply_markup=create_main_menu(new_bill["id"], new_bill['status']), 
                                    parse_mode="HTML")    

    @pdf_router.message(state=BillDateForm.waiting_for_date)
    async def process_payment_date(message: types.Message, state: FSMContext):
        user_date = message.text
        user_id = message.from_user.id
        
        try:
            state_data = await state.get_data()
            old_bill = await get_bill(state_data['bill_id'])
            datetime.strptime(user_date, "%Y-%m-%d")
            naive_date = datetime.strptime(user_date, "%Y-%m-%d")
            localized_date = timezone.localize(naive_date)
            await update_bill(state_data['bill_id'],{"payment_date": localized_date, "status": BillStatus.waiting_for_approval})
            await update_bill_status_based_on_approvals(old_bill.id)
            new_bill = await get_bill(state_data['bill_id'])

            approvers = await get_bill_approvers_data(new_bill.id)

            notification_string = await format_bill_notification(
                created_by=new_bill.created_by,
                approvers=approvers,
                updated_by=user_id,
                new_bill=new_bill,
                old_bill=old_bill,
            )
            await state.set_state(None)
            await message.reply(notification_string, 
                                    reply_markup=create_main_menu(new_bill["id"], new_bill['status']), 
                                    parse_mode="HTML")    

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
                            message_str = 'Invoice data:\n'
                            bill_data = {}
                            extracted_data = process_text_test(bill_text)              
                            if extracted_data:

                                for key, value in extracted_data.items():
                                    bill_data[key] = value
                                    message_str += f"{key}: {value}\n"
                                    print(f"{key}: {value}")
                            else:
                                print("Failed to extract data from the invoice. eng")
                            bill_text_rus = extract_text_from_pdf_images(file_bytes, lang='rus')
                            if bill_text_rus:
                                await message.reply(bill_text_rus)
                                extracted_data = process_text_rus(bill_text_rus)              
                                if extracted_data:

                                    for key, value in extracted_data.items():
                                        bill_data[key] = value
                                        message_str += f"{key}: {value}\n"
                                        print(f"{key}: {value}")
                                else:
                                    print("Failed to extract data from the invoice. rus")   
                            await message.reply(message_str)
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
                                plain_text=bill_text_rus,
                                **bill_data
                            )
                            try:
                                bill_id = await create_bill(bill_data)
                                bill = await get_bill(bill_id)
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
                                    created_by=bill.created_by,
                                    approvers=bill_approvers,
                                    updated_by=user_id,
                                    new_bill=bill
                                )
                                accounts = await get_tochka_bank_accounts_by_chat_id(str(chat_id))
                                keyboard = await create_select_account_payment_keyboard(bill.id, accounts)
                                await message.reply(notification_string, 
                                    reply_markup=keyboard,
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
