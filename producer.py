import json
import pika
import random
from faker import Faker
from mongoengine import connect, Document, StringField, BooleanField

# Налаштування підключення до MongoDB
connect("contacts_db", host="mongodb+srv://username:password@cluster.mongodb.net/contacts_db")

# Модель контакту
class Contact(Document):
    fullname = StringField(required=True)
    email = StringField(required=True)
    phone = StringField(required=True)
    is_sent = BooleanField(default=False)
    preferred_method = StringField(choices=["email", "sms"], required=True)

fake = Faker()

# Підключення до RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

# Оголошення черг
channel.queue_declare(queue="email_queue")
channel.queue_declare(queue="sms_queue")

# Генерація контактів і надсилання у RabbitMQ
for _ in range(10):
    contact = Contact(
        fullname=fake.name(),
        email=fake.email(),
        phone=fake.phone_number(),
        preferred_method=random.choice(["email", "sms"])
    ).save()

    message = {"contact_id": str(contact.id)}
    queue_name = "email_queue" if contact.preferred_method == "email" else "sms_queue"
    
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Зберігає повідомлення при падінні RabbitMQ
        ),
    )

    print(f"Sent to {queue_name}: {message}")

connection.close()
