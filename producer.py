from datetime import date
from os import environ

import pika as pika
from dotenv import load_dotenv


def publish_message_to_rabbitmq(username: str, password: str, index_name: str, exchange: str, doc: bytes):
    """
    Function to connect user to RabbitMQ
    :param username: User's username
    :param password: User's password
    :param index_name: User's elasticsearch index name
    :param exchange: The exchange which sends the message
    :param doc: The message that been sends to the queue
    :return: None
    """
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange, durable=True, exchange_type='topic')
    channel.queue_declare(queue=index_name)
    channel.queue_bind(exchange=exchange, queue=index_name, routing_key=index_name)
    channel.basic_publish(exchange=exchange, routing_key=index_name, body=doc)
    channel.close()


def main():
    load_dotenv()
    username = environ['RABBITMQ_USERNAME']
    password = environ['RABBITMQ_PASSWORD']
    exchange = environ['RABBITMQ_EXCHANGE']
    index_name = environ['ELASTIC_INDEX']
    doc = b'{"Author": "Tester 22", "Date": "%b", "Severity": "3", "Status": "New"}' % str(date.today()).encode()
    publish_message_to_rabbitmq(username, password, index_name, exchange, doc)
    print("Message been sent!")


if __name__ == '__main__':
    main()
