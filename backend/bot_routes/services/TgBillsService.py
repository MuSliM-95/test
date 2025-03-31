import json
import os
import re
from datetime import datetime
import time
from aiogram import  Router, types, F
import aiohttp
import logging
from bot_routes.repositories.impl.TgBillsRepository import TgBillsCreateModel, TgBillsUpdateModel, TgBillsRepository
from bot_routes.repositories.impl.TgBillApproversRepository import TgBillApproversRepository
from bot_routes.functions.tochka_api import TochkaBankError, get_access_token, send_payment_to_tochka
from database.db import TgBillApproveStatus, database, users,TgBillStatus
from bot_routes.functions.pdf_reader import extract_text_from_pdf_images
from bot_routes.functions.TgBillsFuncions import get_user_from_db

logging.basicConfig(level=logging.ERROR)

class TgBillsService:

    def __init__(self, tg_bills_repository: TgBillsRepository, tg_bill_approvers_repository: TgBillApproversRepository, s3_client, s3_bucket_name):
        self.tg_bills_repository = tg_bills_repository
        self.tg_bill_approvers_repository = tg_bill_approvers_repository
        self.s3_client = s3_client
        self.s3_bucket_name = s3_bucket_name

    async def check_user_registration(self, user_id):
        query = users.select().where(users.c.chat_id == user_id)
        user = await database.fetch_one(query)
        if not user:
            return False, "Вы не зарегистрированы"
        return user, ''
    
    async def check_user_permissions(self, bill_id: int, tg_id_updated_by: str) -> tuple[dict, str]:
        user = await get_user_from_db(tg_id_updated_by)
        user_id = user.chat_id
        bill = await self.tg_bills_repository.get_by_id(bill_id)
        if not bill:
            return False, "Счет не найден"
        approvers = await self.tg_bill_approvers_repository.get_approvers_by_bill_id(bill_id)
        if not approvers:
            return False, "Нет подтверждающих пользователей"
        query = users.select().where(users.c.id == bill.created_by)    
        created_user = await database.fetch_one(query)

        if created_user.chat_id == user_id:
            return True, "Вы создатель счета"

        approvers = [approver.approver_id for approver in approvers]
        for approver in approvers:
            if str(approver) == str(user_id):
                return True
        return False,  "У вас нет прав на этот счет"
     

    def convert_unicode_to_text(self, text):
        """Converts Unicode escape sequences to text."""
        if isinstance(text, bytes):
            text = text.decode('utf-8')
        try:
            return json.loads(f'"{text}"')
        except json.JSONDecodeError:
            return text

    def replace_newlines_with_spaces(self, text):
        """Replaces newlines with spaces."""
        return text.replace('\n', ' ')

    def validate_inn(self, inn: str) -> bool:
        """Validates INN."""
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
                check11 = sum(int(c) * w for c, w in zip(inn[:10], weights_12)) % 11 % 10
                weights_12[-1] = 8
                check12 = sum(int(c) * w for c, w in zip(inn[:11], weights_12)) % 11 % 10
                return check11 == int(inn[10]) and check12 == int(inn[11])
        except:
            return False

    def validate_bic_with_corr_account(self, bic: str, corr_account: str) -> bool:
        """Validates BIC with corresponding account."""
        bic_digits = ''.join(filter(str.isdigit, bic)).zfill(9)
        corr_digits = ''.join(filter(str.isdigit, corr_account)).zfill(20)
        if len(bic_digits) != 9 or len(corr_digits) != 20:
            return False
        return bic_digits[-3:] == corr_digits[9:12]


    def normalize_number(self, raw: str) -> str:
        """Normalizes a number by removing non-digit characters."""
        return ''.join(filter(str.isdigit, raw))

    def find_counterparty_account_number(self, text: str, control_number:str) -> tuple[str, str]:
        """Finds counterparty account number."""
        match = re.search(r"(\d{20})", text)
        if match:
            text = text.replace(match.group(1), '')
            if match.group(1)[-3:] != control_number:
                return match.group(1), text
            else:
                return self.find_counterparty_account_number(text, control_number)
        else:
            return None, text

    def process_text_test(self, text: str) -> dict:
        """Processes text to extract BIC, corresponding account, etc."""
        text = re.sub(r'(\d)\s+(\d)', r'\1\2', text)
        text = text.replace('|', ' ')
        patterns = {
            "bic": r"\s(\d{9})(?!\d)",
            "corr_account": r"\s(\d{20})(?!\d)",
            "pc": r"\s(\d{20})(?!\d)",
            "inn": r"\s(\d{10,12})(?!\d)",
        }
        result = {}
        result["counterparty_bank_bic"] = re.search(patterns["bic"], text).group(1) if re.search(patterns["bic"], text) else None
        if result["counterparty_bank_bic"]:
            text = text.replace(result["counterparty_bank_bic"], '')
        counterparty_account_number, text = self.find_counterparty_account_number(text, result.get("counterparty_bank_bic", "")[-3:])
        result["counterparty_account_number"] = counterparty_account_number
        return result

    def process_text_rus(self, text: str) -> dict:
        """Processes Russian text to extract payment amount, purpose, etc."""
        text = re.sub(r'(\d)\s+(\d)', r'\1\2', text)
        text = text.replace(' — ', ': ')
        text = text.replace('|', ' ')
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

    async def check_bill_can_action(self, bill_id: int, action_type: str = None) -> bool:

        new_bill = await self.tg_bills_repository.get_by_id(bill_id)

        if new_bill['status'] == TgBillStatus.CANCELED:
            return False, "Счет отменен"
        return True
    
    async def update_bill(self, bill_id, data: TgBillsUpdateModel, tg_id_updated_by: str) -> tuple[dict, str]:
        """Updates a bill."""
        check_user_permissions, message = await self.check_user_permissions(bill_id, tg_id_updated_by)
        if not check_user_permissions:
            return False, message
        old_bill = await self.tg_bills_repository.get_by_id(bill_id)
        if old_bill['status'] == TgBillStatus.CANCELED:
            return False, "Счет отменен"
        await self.tg_bills_repository.update(old_bill.id, data)
        new_bill = await self.tg_bills_repository.get_by_id(bill_id)
        message = await self.format_bill_notification(
            tg_id_updated_by=tg_id_updated_by,
            new_bill=new_bill,
            old_bill=old_bill,
        )
        return new_bill, message
    
    async def change_bill_date(self, bill_id: int, new_date: str, tg_id_updated_by: str) -> bool:
        check_user_permissions, message = await self.check_user_permissions(bill_id, tg_id_updated_by)
        if not check_user_permissions:
            return False, message
        """Changes the payment date of a bill."""
        old_bill = await self.tg_bills_repository.get_by_id(bill_id)
        if old_bill['status'] == TgBillStatus.CANCELED:
            return False, "Счет отменен"
        payment_date = datetime.strptime(new_date, "%Y-%m-%d")

        data = {
            "status": TgBillStatus.WAITING_FOR_APPROVAL,
            "payment_date": payment_date,
        }
        await self.tg_bills_repository.update(old_bill.id, TgBillsUpdateModel(**data))
        new_bill = await self.tg_bills_repository.get_by_id(bill_id)
        message = await self.format_bill_notification(
            tg_id_updated_by=tg_id_updated_by,
            new_bill=new_bill,
            old_bill=new_bill,
        )
        return new_bill, message
    
    async def get_bill(self, bill_id: int) -> dict:
        """Gets a bill by ID."""
        bill = await self.tg_bills_repository.get_by_id(bill_id)
        return bill
        
    async def format_bill_notification(
        self, 
        tg_id_updated_by: str,
        new_bill: dict,
        old_bill: dict = {},
    ) -> str:
        
        def get_bill_changes(old_bill, new_bill):
            changes = {}
            for key, value in new_bill.items():
                if key in ['created_at', 'updated_at', 'deleted_at']:
                    continue
                else:
                    if old_bill.get(key, None) != new_bill.get(key, None):
                        changes[key] = f"{old_bill.get(key, None)} -> {new_bill.get(key, None)}"
            return changes
        attribute_translation = {
            "id": "id",
            "payment_date": "Дата платежа",
            "accountId": "Аккаунт в точка банке",
            "s3_url": "Ссылка S3",
            "file_name": "Имя файла",
            "payment_amount": "Сумма платежа",
            "counterparty_account_number": "Номер счёта контрагента",
            "payment_purpose": "Назначение платежа",
            "counterparty_bank_bic": "БИК банка контрагента",
            "counterparty_name": "Наименование контрагента",
            "corr_account": "Корреспондентский счет",
            "status": "Статус",
            "request_id": "ID запроса",
            "tochka_bank_account_id": "accountId",
        }
        changes = get_bill_changes(old_bill, new_bill)
        message_parts = [f"Создан новый счёт №{new_bill['id']}:"]

        query = users.select().where(users.c.id == new_bill['created_by'])
        user = await database.fetch_one(query)
        query = users.select().where(users.c.chat_id == tg_id_updated_by)
        updated_by = await database.fetch_one(query)
        if old_bill.get('id', None) is  None:
            message_parts = [f"Создан новый счёт №{new_bill.id}:"]
        else:
            message_parts = [f"Счёт №{new_bill.id} был обновлён:"]
        approvers = await self.tg_bill_approvers_repository.get_approvers_extended_by_bill_id(new_bill['id'])
        for approver in approvers:
            if approver['status'] == TgBillApproveStatus.APPROVED:
                message_parts.append(f"  - Одобрен пользователем: {approver['username']}")
            elif approver['status'] == TgBillApproveStatus.CANCELED:
                message_parts.append(f"  - Отклонён пользователем: {approver['username']}")
            elif approver['status'] == TgBillApproveStatus.NEW:
                message_parts.append(f"  - Необходимо одобрение пользователя: {approver['username']}")
        for key, value in new_bill.items():
            # Skip certain keys that are not needed in the notification
            if key in ['created_by','created_at', 'updated_at', 'deleted_at', 'plain_text', 'accountId']:
                continue
            else:
                # Use translation if defined; otherwise fall back to the key name.
                translated_key = attribute_translation.get(key, key)
                message_parts.append(f"  - {translated_key}: {value}")

        if changes:
            message_parts.append("Изменения:")
            for key, value in changes.items():
                message_parts.append(f"  - {key}: {value}")
        message_parts.append(f"  - Создан пользователем: {user.first_name}")
        message_parts.append(f"  - Обновлён пользователем: {updated_by.first_name if updated_by else 'Система'}")

        return "\n".join(message_parts)

    
    async def send_bill(self, bill_id, tg_id_updated_by) -> tuple[bool, str]:
        bill = await self.tg_bills_repository.get_by_id(bill_id)

        check_user_permissions, message = await self.check_user_permissions(bill_id, tg_id_updated_by)
        if not check_user_permissions:
            return False, message
        if bill.status == TgBillStatus.CANCELED:
            return False, "Счет отменен"
        if bill.status == TgBillStatus.WAITING_FOR_APPROVAL:
            return False, "Счет находится на одобрении"
        
        if bill.status == TgBillStatus.APPROVED or bill.status == TgBillStatus.ERROR:

            account_arr = bill.accountId.split('/')
            try:
                auth = None
                try:
                    tokens = await get_access_token(bill['tochka_bank_account_id'])
                    if tokens:
                        auth = tokens['access_token']
                    else:
                        return False, "Ошибка при отправке счёта в банк. Не найден токен аccess_token."
                except Exception as e:
                    logging.exception("Ошибка при отправке счёта в банк. Не найден токен аccess_token.")
                    return False, "Ошибка при отправке счёта в банк. Не найден токен аccess_token."
    

                try:
                    await self.tg_bills_repository.update(bill['id'],  TgBillsUpdateModel(status=TgBillStatus.ERROR))
                except Exception as e:
                    logging.exception("Ошибка при отправке счёта в банк.")
                    return False, "Ошибка при отправке счёта в банк."
                
                result = await send_payment_to_tochka(
                    account_code=account_arr[0],
                    bank_code=account_arr[1],
                    counterparty_bank_bic=bill.counterparty_bank_bic,
                    counterparty_account_number=bill.counterparty_account_number,
                    counterparty_name=bill.counterparty_name,
                    payment_date=bill.payment_date.strftime("%Y-%m-%d"),
                    payment_amount=bill.payment_amount,
                    payment_purpose=bill.payment_purpose,
                    auth=auth
                )
                if result["success"]:
                    await self.tg_bills_repository.update(bill['id'], TgBillsUpdateModel(request_id=result["request_id"], status=TgBillStatus.REQUESTED ))
                    return True, "Счёт отправлен в банк. Запрос на оплату: " + result["request_id"]
                else:
                    await self.tg_bills_repository.update(bill['id'], TgBillsUpdateModel(status=TgBillStatus.ERROR))
                    return False, "Ошибка при отправке счёта в банк. " + result["message"]
            except TochkaBankError as e:
                logging.exception("Bank API Error ")
                detailed_errors = ''
                if e.errors is not None:
                    detailed_errors = [f"{err['errorCode']}: {err['message']}" for err in e.errors]
                error_message = f"Bank API Error {e.code}: {e.message}\nDetailed errors: {detailed_errors}"
                await self.tg_bills_repository.update(bill['id'],  TgBillsUpdateModel(status=TgBillStatus.ERROR))
                return False, error_message
            except Exception as e:
                logging.exception("Ошибка при отправке счёта в банк. ")
                await self.tg_bills_repository.update(bill['id'], TgBillsUpdateModel(status=TgBillStatus.ERROR))
                return False, "Ошибка при отправке счёта в банк. " + str(e)
        else:
            return False, "Счет не может быть отправлен в банк, так как его статус: " + bill.status
            

    async def update_bill_status_based_on_approvals(self, bill_id: int, approvers = []):

        all_approved = all(approver.status == TgBillApproveStatus.APPROVED for approver in approvers)
        any_canceled = any(approver.status == TgBillApproveStatus.CANCELED for approver in approvers)

        if any_canceled:
            await self.tg_bills_repository.update(bill_id, TgBillsUpdateModel(status=TgBillStatus.WAITING_FOR_APPROVAL))
        elif all_approved:
            await self.tg_bills_repository.update(bill_id,  TgBillsUpdateModel(status=TgBillStatus.APPROVED))
        else:
            await self.tg_bills_repository.update(bill_id, TgBillsUpdateModel(status=TgBillStatus.WAITING_FOR_APPROVAL))
        bill = await self.tg_bills_repository.get_by_id(bill_id)
        return bill
    
    async def download_telegram_file(self, file_path: str,  bot_token) -> bytes:
        """Downloads file from Telegram."""
        file_url = f'https://api.telegram.org/file/bot{bot_token}/{file_path}'

        async with aiohttp.ClientSession() as session:
            async with session.get(file_url) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    raise Exception(f"Failed to download file. Status code: {response.status}")
    
    def extract_bill_data(self, bill_text, bill_text_rus):
        bill_data = {}
        def remove_spaces_between_digits(text: str) -> str:
            return re.sub(r'(?<=\d)\s+(?=\d)', '', text)
        if bill_text:
            bill_text = remove_spaces_between_digits(bill_text)
            extracted_data = self.process_text_test(bill_text)
            bill_data.update(extracted_data)
        if bill_text_rus:
            extracted_data_rus = self.process_text_rus(bill_text_rus)
            bill_data.update(extracted_data_rus)
        return bill_data
    
    async def process_and_save_bill(self, file_id, file_name, tg_id_updated_by, bot_token, file_path):
        """Processes the uploaded PDF bill and saves it."""
        try:
            file_bytes = await self.download_telegram_file( file_path, bot_token)
            await self.s3_client.upload_file_object(file_bytes=file_bytes, bucket_name=self.s3_bucket_name, file_key=f"tg-bills/{file_id}.pdf")
            file_url = f'{os.getenv("S3_URL")}/{self.s3_bucket_name}/tg-bills/{file_id}.pdf'

            bill_text = extract_text_from_pdf_images(file_bytes)
            bill_text_rus = extract_text_from_pdf_images(file_bytes, lang='rus')

            bill_data = self.extract_bill_data(bill_text, bill_text_rus)

            user = await get_user_from_db(str(tg_id_updated_by))
            bill_data["created_by"] = user.id
            bill_data["status"] = TgBillStatus.NEW
            bill_data["s3_url"] = file_url
            bill_data["file_name"] = file_name
            bill_data["plain_text"] = bill_text_rus

            bill_id = await self.tg_bills_repository.insert(TgBillsCreateModel(**bill_data))
            bill = await self.tg_bills_repository.get_by_id(bill_id)
            msg = await self.format_bill_notification(
                tg_id_updated_by=tg_id_updated_by,
                new_bill=bill
            )
            return bill, msg

        except Exception as e:
            logging.exception("Ошибка при обработке счёта.")
            return None, "Ошибка при обработке счёта."