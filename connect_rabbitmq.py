import ssl
import pika
from dotenv import load_dotenv
import os
import json
from messages_to_hubspot import send_data_to_hubspot
from daemonize import Daemonize

def connect_and_consume():
    # Load variables from .env file
    load_dotenv()

    # Access variables
    rabbitmq_host = os.getenv("RABBITMQ_HOST")
    rabbitmq_port = os.getenv("RABBITMQ_PORT")
    rabbitmq_username = os.getenv("RABBITMQ_USERNAME")
    rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
    rabbitmq_output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE")

    ca_cert_path = os.getenv("CA_CERT_PATH")
    client_cert_path = os.getenv("CLIENT_CERT_PATH")
    client_key_path = os.getenv("CLIENT_KEY_PATH")

    # Set the heartbeat interval in seconds
    heartbeat_interval = 60  # 1 minute

    # This is the function that takes care of the callback of the queue
    def call_back(ch, method, properties, body):
        print(f"\n\n\n [x] Received the queue data {body}")
        response = send_data_to_hubspot(body, ch)
        if response == True:
            # print(f"Line 159 Positively Ack: {body}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # SSL configuration
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile=client_cert_path,
                            keyfile=client_key_path)
    ssl_context.load_verify_locations(cafile=ca_cert_path)
# ca_certificate.pem  client_certificate.pem  client_key.pem

    # Connect to RabbitMQ
    credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
    rabbitmq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_host, port=rabbitmq_port, credentials=credentials, heartbeat=heartbeat_interval,    
        ssl_options=pika.SSLOptions(context=ssl_context)
        )
    )
    rabbitmq_channel = rabbitmq_connection.channel()

    # Declare output queue name
    rabbitmq_channel.queue_declare(queue=rabbitmq_output_queue, durable=True)

    # Now consume the message with a callback function we created earlier
    rabbitmq_channel.basic_consume(rabbitmq_output_queue, on_message_callback=call_back, auto_ack=False)

    rabbitmq_channel.start_consuming()

# connect_and_consume()

def start_connect_and_consuming_daemon():
    connect_and_consume()

if __name__ == "__main__":
    # Daemonize the script
    daemon = Daemonize(app="rabbitmq_hubspot_gateway", pid="/tmp/rabbitmq_hubspot_gateway.pid", action=start_connect_and_consuming_daemon)
    daemon.start()
