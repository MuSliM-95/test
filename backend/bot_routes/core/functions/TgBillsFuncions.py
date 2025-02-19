from sqlalchemy import select
from database.db import database, users
from database.db import database,  users,  cboxes,pboxes,   tochka_bank_accounts
from aiogram.dispatcher.fsm.context import FSMContext

async def get_tochka_bank_accounts_by_chat_id(chat_id: str):
    chat_owner= await get_chat_owner(chat_id)
    query = (
        select([tochka_bank_accounts])
        .select_from(tochka_bank_accounts)
        .join(pboxes, tochka_bank_accounts.c.payboxes_id == pboxes.c.id)
        .join(cboxes, pboxes.c.cashbox == cboxes.c.id)
        .join(users, cboxes.c.admin == users.c.id)
        .where( cboxes.c.admin == chat_owner["id"])

    )
    return await database.fetch_all(query)

async def get_user_from_db_by_username(username: str):
    query = users.select().where(users.c.username == username)
    return await database.fetch_one(query)

async def get_chat_owner(chat_id: str):
    query = users.select().where(users.c.chat_id == chat_id)
    res =  await database.fetch_one(query)
    query = users.select().where(users.c.owner_id == res["owner_id"])
    res =  await database.fetch_one(query)
    return res

async def get_user_from_db(user_id: str):
    """Fetches a user from the database based on user_id."""
    query = users.select().where(users.c.chat_id == user_id)
    return await database.fetch_one(query)


async def delete_previous_bill_message(bot, chat_id, state: FSMContext):
    """Deletes the previous message related to a bill, if it exists."""
    data = await state.get_data()
    last_message_id = data.get("last_bill_message_id")

    if last_message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=last_message_id)
        except Exception as e:
            print(f"Error deleting message: {e}")