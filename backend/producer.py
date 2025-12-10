import json
import os
import uuid
from datetime import datetime

from api.apple_wallet.messages.AppleWalletCardUpdateMessage import AppleWalletCardUpdateMessage
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.utils.ioc.ioc import ioc
from database.db import database, users
import aio_pika
from const import marketplace_orders_queue_name


async def produce_message(body: dict) -> None:
    connection = await aio_pika.connect_robust(
        host=os.getenv("RABBITMQ_HOST"),
        port=os.getenv("RABBITMQ_PORT"),
        login=os.getenv("RABBITMQ_USER"),
        password=os.getenv("RABBITMQ_PASS"),
        virtualhost=os.getenv("RABBITMQ_VHOST"),
        timeout=10,
    )

    async with connection:
        routing_key = "message_queue"

        channel = await connection.channel()
        query = users.select().where(
            users.c.is_blocked == False, users.c.chat_id != body["tg_user_or_chat"]
        )
        live_users = await database.fetch_all(query=query)
        for i in live_users:
            body.update({"from_or_to": str(i.chat_id)})
            body.update({"is_blocked": i.is_blocked})
            body.update({"size": len(live_users)})
            message = aio_pika.Message(body=json.dumps(body).encode())
            await channel.default_exchange.publish(
                message=message, routing_key=routing_key
            )


async def queue_notification(notification_data: dict) -> bool:
    """
    Добавляет уведомление в очередь RabbitMQ для последующей обработки.

    Args:
        notification_data: Данные для уведомления (тип, получатели, содержимое и т.д.)
        
    Returns:
        bool: Успешно ли добавлено уведомление в очередь
    """
    try:
        connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST"),
            port=os.getenv("RABBITMQ_PORT"),
            login=os.getenv("RABBITMQ_USER"),
            password=os.getenv("RABBITMQ_PASS"),
            virtualhost=os.getenv("RABBITMQ_VHOST"),
            timeout=10,
        )

        async with connection:
            routing_key = "notification_queue"

            channel = await connection.channel()

            await channel.declare_queue(routing_key, durable=True)

            message = aio_pika.Message(
                body=json.dumps(notification_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(message, routing_key=routing_key)

        return True
    except Exception as e:
        print(f"Error adding notification to queue: {e}")
        return False
        
        
async def send_order_assignment_notification(order_id: int, role: str, user_id: int, user_name: str, links: dict = None) -> bool:
    """
    Отправляет уведомление о назначении исполнителя для заказа
    
    Args:
        order_id: ID заказа
        role: Роль исполнителя (picker, courier)
        user_id: ID назначенного пользователя
        user_name: Имя назначенного пользователя
        links: Ссылки на заказ для разных ролей
        
    Returns:
        bool: Успешно ли добавлено уведомление в очередь
    """
    notification_data = {
        "type": "assignment",
        "order_id": order_id,
        "role": role,
        "user_id": user_id,
        "user_name": user_name,
        "links": links or {}
    }
    
    # Здесь можно получить chat_id других участников процесса и добавить их в recipients
    # Например, менеджеров, которым нужно знать о назначении
    # notification_data["recipients"] = ["chat_id1", "chat_id2", ...]
    
    return await queue_notification(notification_data)


async def send_new_chat_notification(
    cashbox_id: int,
    chat_id: int,
    contact_name: str = None,
    channel_name: str = None,
    ad_title: str = None
) -> bool:
    """
    Отправляет уведомление о новом чате владельцам кассы
    
    Args:
        cashbox_id: ID кассы
        chat_id: ID созданного чата
        contact_name: Имя контакта
        channel_name: Название канала
        ad_title: Название объявления
        
    Returns:
        bool: Успешно ли добавлено уведомление в очередь
    """
    try:
        print(f"=== send_new_chat_notification called ===")
        print(f"cashbox_id: {cashbox_id}, chat_id: {chat_id}")
        print(f"contact_name: {contact_name}, channel_name: {channel_name}, ad_title: {ad_title}")
        
        from sqlalchemy import select, and_
        from database.db import users, users_cboxes_relation
        
        # Получаем всех владельцев кассы
        owner_query = select([users.c.chat_id]).select_from(
            users.join(
                users_cboxes_relation,
                users.c.id == users_cboxes_relation.c.user
            )
        ).where(
            and_(
                users_cboxes_relation.c.cashbox_id == cashbox_id,
                users_cboxes_relation.c.is_owner == True,
                users_cboxes_relation.c.status == True,
                users.c.chat_id.isnot(None)
            )
        )
        
        owners = await database.fetch_all(owner_query)
        recipients = [str(owner.chat_id) for owner in owners if owner.chat_id]
        
        print(f"Found {len(recipients)} owners: {recipients}")
        
        if not recipients:
            print(f"No owners found for cashbox {cashbox_id} to send chat notification")
            return False
        
        # Формируем текст уведомления
        text = "💬 <b>Новый чат</b>\n\n"
        
        if contact_name:
            text += f"Контакт: {contact_name}\n"
        
        if channel_name:
            text += f"Канал: {channel_name}\n"
        
        if ad_title:
            text += f"Объявление: {ad_title}\n"
        
        text += f"\nID чата: {chat_id}"
        
        print(f"Notification text: {text}")
        
        notification_data = {
            "type": "segment_notification",
            "recipients": recipients,
            "text": text,
            "timestamp": datetime.now().timestamp(),
        }
        
        print(f"Sending notification to queue: {notification_data}")
        result = await queue_notification(notification_data)
        print(f"Notification queued: {result}")
        return result
        
    except Exception as e:
        print(f"Error sending new chat notification: {e}")
        import traceback
        traceback.print_exc()
        return False


async def publish_apple_wallet_pass_update(card_ids: list[int]):
    rabbitmq_messaging: IRabbitMessaging = await ioc.get(IRabbitFactory)()

    for card_id in card_ids:
        await rabbitmq_messaging.publish(
            AppleWalletCardUpdateMessage(
                message_id=uuid.uuid4(),
                loyality_card_id=card_id,
            ),
            routing_key="teach_card_operation"
        )
