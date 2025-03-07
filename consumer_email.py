import json
import pika
from mongoengine import connect
from models import Contact

# Підключення до MongoDB
connect("contacts_db", host="mongodb+srv://username:password@cluster.mongodb.net/contacts_db")

# Функція-заглушка для надсилання email
def send_email(contact):
    print(f"Sending email to {contact.email}")
    return True

# Функція обробки повідомлення з черги
def callback(ch, method, properties, body):
    message = json.loads(body)
    contact = Contact.objects(id=message["contact_id"]).first()

    if contact and send_email(contact):
        contact.is_sent = True
        contact.save()
        print(f"Email sent to {contact.fullname}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# Підключення до RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue="email_queue")
channel.basic_consume(queue="email_queue", on_message_callback=callback)

print("Waiting for email messages...")
channel.start_consuming()
