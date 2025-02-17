
import logging
import os
from fastapi.params import Depends
import pytz
import json
from datetime import datetime

from aiogram import  Router, types, F
from aiogram.client.session import aiohttp

from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.fsm.context import FSMContext
from typing import Dict, Any
from database.db import database,  users,  bills, tochka_bank_accounts, bill_approvers

from bot_routes.core.functions.keyboards import *

from bot_routes.core.functions.callbacks import (
    bills_callback,
    change_payment_date_bill_callback,
    create_select_account_payment_callback,
)
from bot_routes.core.services.TgBillsService import ITgBillsUpdate, TgBillsService, get_tochka_bank_accounts_by_chat_id
from bot_routes.core.services.TgBillApproversService import  TgBillApproversService
from bot_routes.core.repositories.impl.TgBillsRepository import TgBillsRepository
from bot_routes.core.repositories.impl.TgBillApproversRepository import TgBillApproversRepository




timezone = pytz.timezone("Europe/Moscow")

# Определение состояний для FSM
class BillDateForm(StatesGroup):
    start = State()
    waiting_for_date = State()

# Функция для получения маршрута
def get_bill_route(bot, s3_client):

    pdf_router = Router()
    tg_bill_repository = TgBillsRepository()
    tg_bill_approvers_repository = TgBillApproversRepository(database, bill_approvers, users)
    tg_bill_service = TgBillsService(tg_bill_repository, tg_bill_approvers_repository, s3_client, s3_bucket_name='tg-bills')
    
    tg_bill_approvers_service = TgBillApproversService(tg_bill_approvers_repository)


    @pdf_router.callback_query(lambda c: create_select_account_payment_callback.filter()(c))
    async def select_account_payment_handler(callback_query: types.CallbackQuery):
        data = create_select_account_payment_callback.parse(callback_query.data)
        tg_id_updated_by = str(callback_query.from_user.id)
        account_id = data['account_id']
        bill_id = data['bill_id']
        bill, msg = await tg_bill_service.update_bill(bill_id, ITgBillsUpdate(tochka_bank_account_id=account_id), tg_id_updated_by)
        if not bill:
            await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
            await bot.answer_callback_query(callback_query.id)
            return
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Выбран счет {account_id}", reply_markup=create_main_menu(bill_id, bill['status']))

    @pdf_router.callback_query(lambda c: change_payment_date_bill_callback.filter()(c))
    async def change_payment_date_handler(callback_query: types.CallbackQuery, callback_data: dict = Depends(change_payment_date_bill_callback.parse), state: FSMContext = None):
        bill_id = int(callback_data['bill_id'])
        tg_id_updated_by = str(callback_query.from_user.id)
        if 'data' in callback_data and callback_data["data"]:
            datetime.strptime(callback_data["data"], "%Y-%m-%d")
            bill, msg = await tg_bill_service.change_bill_date(bill_id, callback_data["data"], str(tg_id_updated_by))
            if not bill:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
            await bot.send_message(chat_id=callback_query.message.chat.id, text=msg, reply_markup=create_main_menu(bill_id, bill['status']))
        else:

            await bot.answer_callback_query(callback_query.id)
            await callback_query.message.reply("Введите новую дату в формате ГГГГ-ММ-ДД:")
            await state.set_state(BillDateForm.waiting_for_date)


    @pdf_router.callback_query(lambda c: bills_callback.filter()(c))
    async def bills_callback_handler(callback_query: types.CallbackQuery, callback_data: dict = Depends(bills_callback.parse)):
        action = callback_data['action']
        bill_id = callback_data['bill_id']
        tg_id_updated_by = str(callback_query.from_user.id)
        user, message = tg_bill_service.check_user_registration(tg_id_updated_by)
        if not user:
            await bot.answer_callback_query(callback_query.id, text=message)
            return
        res, message  = await tg_bill_service.check_user_permissions(bill_id, tg_id_updated_by)
        if not res:
            await bot.answer_callback_query(callback_query.id, text=message)
            return
        msg = ''

        if action == 'cancel_bill':
            bill, msg = await tg_bill_service.update_bill(bill_id, {"status": TgBillStatus.CANCELLED})
            if not bill:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
                return
        if action == 'send_bill':
            bill, msg = await tg_bill_service.send_bill(bill_id)
            if not bill:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
                return
        if action == 'like':
            user_permissions, msg = await tg_bill_service.check_user_permissions(bill_id, tg_id_updated_by)
            if user_permissions:
              
                approve = await tg_bill_approvers_service.get_approve_by_bill_id_and_approver_id(user.id, bill_id)
                if not approve:
                    await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Не достпно для {user.username}, так как не является утверждающим.")
                    return
                res, msg = await tg_bill_approvers_service.like(approve.id, tg_id_updated_by)
                if not res:
                    await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
                    return
                approvers = await tg_bill_approvers_service.get_bill_approvers(bill_id)
                bill = await tg_bill_service.update_bill_status_based_on_approvals(bill_id, approvers)


        if action == 'dislike':
            user_permissions, msg = await tg_bill_service.check_user_permissions(bill_id, tg_id_updated_by)
            if user_permissions:
                approve = await tg_bill_approvers_service.get_approve_by_bill_id_and_approver_id(user.id, bill_id)
                if not approve:
                    await bot.send_message(chat_id=callback_query.message.chat.id, text=f"Не достпно для {user.username}, так как не является утверждающим.")
                    return
                res, msg = await tg_bill_approvers_service.dislike(approve.id, tg_id_updated_by)
                if not res:
                    await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
                    return
                approvers = await tg_bill_approvers_service.get_bill_approvers(bill_id)
                bill = await tg_bill_service.update_bill_status_based_on_approvals(bill_id, approvers)

        
        
        await bot.send_message(chat_id=callback_query.message.chat.id, text=msg, reply_markup=create_main_menu(bill_id, bill['status']))
    

    @pdf_router.message(state=BillDateForm.waiting_for_date)
    async def process_payment_date(message: types.Message, state: FSMContext):
        user_date = message.text
        tg_id_updated_by = message.from_user.id
        try:
            state_data = await state.get_data()
            datetime.strptime(user_date, "%Y-%m-%d")

            bill, msg = await tg_bill_service.change_bill_date(state_data['bill_id'], user_date, str(tg_id_updated_by))
            if not bill:
                await message.reply(msg)
        
            await state.set_state(None)
            await message.reply(msg, reply_markup=create_main_menu(state_data["bill_id"], bill['status']), 
                                    parse_mode="HTML")    

        except ValueError:
            await message.reply("Пожалуйста, введите дату в правильном формате (ГГГГ-ММ-ДД):")
            return


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
            file_info = await bot.get_file(file_id)
            bill, msg = await tg_bill_service.process_and_save_bill(file_id, file_name, str(user_id), bot.token, file_info.file_path)
            if not bill:
                await bot.send_message(chat_id=chat_id, text=msg)
                return
            
            bill_approvers, msg = await tg_bill_approvers_service.create_bill_approvers(message, bill.id)
            if not bill_approvers:
                await bot.send_message(chat_id=chat_id, text=msg)
                return
            new_bill = await tg_bill_service.get_bill(bill.id)
            notification_string = await tg_bill_service.format_bill_notification(tg_id_updated_by=str(user_id), old_bill=bill,new_bill=new_bill)
            accounts = await get_tochka_bank_accounts_by_chat_id(str(message.chat.id))
            keyboard = create_select_account_payment_keyboard(bill.id, accounts)
            await message.reply(notification_string,
                                reply_markup=keyboard,
                                parse_mode="HTML")
                 
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    return pdf_router
