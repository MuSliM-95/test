import json
import os
from datetime import datetime
from typing import List

import aio_pika


async def send_segment_notification(
    recipient_ids: List[str] = None,
    notification_text: str = None,
    segment_id: int = None,
) -> bool:
    """
    Отправляет уведомление из сегментов RabbitMQ

    Args:
        recipient_ids: Список ID получателей (telegram chat_id)
        notification_text: Предварительно форматированный текст уведомления (опционально)
        segment_id: ID сегмента (опционально)

    Returns:
        bool: Успешно ли добавлено уведомление в очередь
    """
    try:
        notification_data = {
            "type": "segment_notification",
            "recipients": recipient_ids or [],
            "text": notification_text,
            "timestamp": datetime.now().timestamp(),
        }

        connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST"),
            port=os.getenv("RABBITMQ_PORT"),
            login=os.getenv("RABBITMQ_USER"),
            password=os.getenv("RABBITMQ_PASS"),
            virtualhost=os.getenv("RABBITMQ_VHOST"),
            timeout=10,
        )

        async with connection:
            channel = await connection.channel()

            queue = await channel.declare_queue("notification_queue", durable=True)

            message = aio_pika.Message(
                body=json.dumps(notification_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(
                message, routing_key="notification_queue"
            )

        print(
            f"Notification 'segment_notification' for segment {segment_id} queued successfully. {recipient_ids}"
        )
        return True

    except Exception as e:
        print(f"Failed to send notification for for segment {segment_id}: {str(e)}")
        return False