import asyncio

from sqlalchemy import select

from ws_manager import manager

from database.db import database, pboxes, users, users_cboxes_relation, projects

from datetime import datetime
import time


async def get_user_id_cashbox_id_by_token(token: str):
    user_cbox = await database.fetch_one(
        users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    )

    if user_cbox:
        user = await database.fetch_one(users.select().where(users.c.id == user_cbox.user))
        return user.id, user_cbox.cashbox_id
    else:
        return None, None


async def get_user_by_token(token: str):
    user_cbox = await database.fetch_one(
        users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    )

    user_dict = None
    if user_cbox:
        user = await database.fetch_one(users.select().where(users.c.id == user_cbox.user))
        user_dict = {
            "id": user.id, "external_id": user.external_id, "photo": user.photo,
            "phone_number": user.phone_number,
            "first_name": user.first_name,
            "last_name": user.last_name, "username": user.username, "status": user_cbox.status,
            "is_admin": user_cbox.is_owner, "created_at": user.created_at, "updated_at": user.updated_at,
            "tags": user_cbox.tags, "timezone": user_cbox.timezone, "payment_past_edit_days": user_cbox.payment_past_edit_days,
            "shift_work_enabled": user_cbox.shift_work_enabled,
        }

    return user_dict


async def raschet(user, token):
    update = await database.fetch_all("SELECT * FROM rachet(:token, :today)",
                                      {"token": token, "today": int(datetime.utcnow().timestamp())})

    payboxes_list_q = select([pboxes.c.id, pboxes.c.start_balance]).where(
        pboxes.c.cashbox == user.cashbox_id)
    projects_list_q = select([projects.c.id]).where(
        projects.c.cashbox == user.cashbox_id)

    payboxes_ids = await database.fetch_all(payboxes_list_q)
    projects_ids = await database.fetch_all(projects_list_q)

    z_pbs = [dict(pb) for pb in payboxes_ids if pb.id not in [
        pb.pb_id for pb in update if pb.pb_id]]
    z_proj = [dict(pr) for pr in projects_ids if pr.id not in [
        pr.pr_id for pr in update if pr.pr_id]]

    for i in update:
        if i.pb_id:
            q = pboxes.select().where(pboxes.c.id == i.pb_id)
            paybox = await database.fetch_one(q)
            q = pboxes.update().where(pboxes.c.id == i.pb_id).values(
                {"balance": round(float(i.balance) + paybox.start_balance, 2),
                 "update_start_balance": int(datetime.utcnow().timestamp())})
            await database.execute(q)
            q = pboxes.select().where(pboxes.c.id == i.pb_id)
            paybox = await database.fetch_one(q)
            await manager.send_message(token, {"action": "edit", "target": "payboxes", "result": dict(paybox)})
        if i.pr_id:
            update_proj = {"incoming": i.incoming, "outgoing": i.outgoing,
                           "updated_at": int(datetime.utcnow().timestamp())}

            if update_proj["outgoing"] == 0 and update_proj["incoming"] != 0:
                update_proj['profitability'] = 100
            elif update_proj["outgoing"] == 0 and update_proj["incoming"] == 0:
                update_proj['profitability'] = 0
            else:
                update_proj['profitability'] = round(
                    ((update_proj["incoming"] - update_proj["outgoing"]) / update_proj["outgoing"]) * 100, 2)

            q = projects.update().where(projects.c.id == i.pr_id).values(update_proj)
            await database.execute(q)
            q = projects.select().where(projects.c.id == i.pr_id)
            project = await database.fetch_one(q)
            await manager.send_message(token, {"action": "edit", "target": "projects", "result": dict(project)})

    for z_paybox in z_pbs:
        q = pboxes.update().where(pboxes.c.id == z_paybox['id']).values(
            {"balance": z_paybox['start_balance'], "update_start_balance": int(datetime.utcnow().timestamp())})
        await database.execute(q)
        q = pboxes.select().where(pboxes.c.id == z_paybox['id'])
        paybox = await database.fetch_one(q)
        await manager.send_message(token, {"action": "edit", "target": "payboxes", "result": dict(paybox)})

    for z_project in z_proj:
        q = projects.update().where(projects.c.id == z_project['id']).values(
            {"incoming": 0, "outgoing": 0, 'profitability': 0,
             "updated_at": int(datetime.utcnow().timestamp())}
        )
        await database.execute(q)
        q = projects.select().where(projects.c.id == z_project['id'])
        project = await database.fetch_one(q)
        await manager.send_message(token, {"action": "edit", "target": "projects", "result": dict(project)})
