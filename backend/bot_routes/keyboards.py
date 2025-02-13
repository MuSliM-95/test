from datetime import datetime
from aiogram import  Router, types, F

from bot_routes.bills import BillStatus

# Функция для создания клавиатуры
async def create_select_account_payment_keyboard(bill_id, accounts):
    keyboard_keys = []
    for account in accounts:
        keyboard_keys.append([
            types.InlineKeyboardButton(
                text=str(account.accountId), 
                callback_data=f'{{"action": "select_tb_account", "account_id": {account.id}, "bill_id": {bill_id}}}'
            )
        ])
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=keyboard_keys)
    return keyboard


def create_main_menu(bill_id: int, status: BillStatus):
    today = datetime.now()
    naive_date = today.strftime("%Y-%m-%d")
    inline_keyboard = [  [
                types.InlineKeyboardButton(text="Отменить платёж", callback_data=f'{{"action": "cancel_bill", "bill_id": {bill_id}}}')
            ]]
    if status == BillStatus.canceled:
        inline_keyboard = [
            [
            
            ]
        ]
    if status == BillStatus.new:
        inline_keyboard = [
            [
                types.InlineKeyboardButton(text="Сместить дату", callback_data=f'{{"action": "change_date", "bill_id": {bill_id}}}')
            ],
            [
                types.InlineKeyboardButton(text="Добавить сегодняшним числом", callback_data=f'{{"action": "change_date", "data": "{naive_date}",  "bill_id": {bill_id}}}')
            ],
            [
                types.InlineKeyboardButton(text="Отменить платёж", callback_data=f'{{"action": "cancel_bill", "bill_id": {bill_id}}}')
            ]
        ]
    if status == BillStatus.waiting_for_approval:
         inline_keyboard = [
            [
                types.InlineKeyboardButton(text="👍 Like", callback_data=f'{{"action": "like", "bill_id": {bill_id}}}'),
                types.InlineKeyboardButton(text="👎 Dislike", callback_data=f'{{"action": "dislike", "bill_id": {bill_id}}}')
            ],
            [
                types.InlineKeyboardButton(text="Отменить платёж", callback_data=f'{{"action": "cancel_bill", "bill_id": {bill_id}}}')
            ]
        ]
    if status == BillStatus.approved:
        inline_keyboard = [
            [types.InlineKeyboardButton(text="Отправить в банк", callback_data=f'{{"action": "send_bill", "bill_id": {bill_id}}}')],
            [types.InlineKeyboardButton(text="Отменить платёж", callback_data=f'{{"action": "cancel_bill", "bill_id": {bill_id}}}')]
        ]
    if status == BillStatus.error:
         inline_keyboard = [
         
           [types.InlineKeyboardButton(text="Редактировать", callback_data=f'{{"action": "edit_bill", "bill_id": {bill_id}}}')],
            [types.InlineKeyboardButton(text="Отправить", callback_data=f'{{"action": "send_bill", "bill_id": {bill_id}}}')],
            [types.InlineKeyboardButton(text="Отменить платёж", callback_data=f'{{"action": "cancel_bill", "bill_id": {bill_id}}}')]
        ]
    if status == BillStatus.requested:
        inline_keyboard = [
            [types.InlineKeyboardButton(text="Проверить статус (неактивно)", callback_data=f'{{"action": "check_bill", "bill_id": {bill_id}}}')]
        ]
    keyboard = types.InlineKeyboardMarkup(
        inline_keyboard=inline_keyboard
    )
    return keyboard