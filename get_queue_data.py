import pika
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Access variables
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_port = os.getenv("RABBITMQ_PORT")
rabbitmq_queue = os.getenv("RABBITMQ_QUEUE")

#print(rabbitmq_host)
#print(rabbitmq_port)
#print(rabbitmq_queue)

# this is the function that takes callback of queue
def callbackIt(ch, method, properties, body) :
    print(f" [x] Received the queue data {body}")

# connect to rabbitmq
rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))
rabbitmq_channel = rabbitmq_connection.channel()

# declare queue name
rabbitmq_channel.queue_declare(queue=rabbitmq_queue)

# now consume the message with a callback function we created earlier 
rabbitmq_channel.basic_consume(rabbitmq_queue, on_message_callback=callbackIt, auto_ack=True)


rabbitmq_channel.start_consuming()
