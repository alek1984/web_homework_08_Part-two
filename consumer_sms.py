import json
import pika
from mongoengine import connect
from models import Contact

# Підключення до MongoDB
connect("contacts_db", host="mongodb+srv://username:password@cluster.mongodb.net/contacts_db")

# Функція-заглушка для надсилання SMS
def send_sms(contact):
    print(f"Sending SMS to {contact.phone}")
    return True

# Функція обробки повідомлення з черги
def callback(ch, method, properties, body):
    message = json.loads(body)
    contact = Contact.objects(id=message["contact_id"]).first()

    if contact and send_sms(contact):
        contact.is_sent = True
        contact.save()
        print(f"SMS sent to {contact.fullname}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# Підключення до RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue="sms_queue")
channel.basic_consume(queue="sms_queue", on_message_callback=callback)

print("Waiting for SMS messages...")
channel.start_consuming()
