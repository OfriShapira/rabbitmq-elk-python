from datetime import date
from os import environ

import pika as pika
from dotenv import load_dotenv


def publish_message_to_rabbitmq(username: str, password: str, index_name: str, exchange: str, doc: bytes, hostname: str,
                                exchange_type: str):
    """
    Function to connect user to RabbitMQ
    :param username: User's username
    :param password: User's password
    :param index_name: User's elasticsearch index name
    :param exchange: The exchange which sends the message
    :param doc: The message that been sends to the queue
    :param hostname: User's hostname
    :param exchange_type: Exchange type (direct, topic, fanout etc.)
    :return: None
    """
    credentials = pika.PlainCredentials(username=username, password=password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, durable=True, exchange_type=exchange_type)
    channel.queue_declare(queue=index_name)
    channel.queue_bind(exchange=exchange, queue=index_name, routing_key=index_name)
    channel.basic_publish(exchange=exchange, routing_key=index_name, body=doc)
    channel.close()
    print("Message been sent!")


def main():
    load_dotenv()

    username = environ['RABBITMQ_USERNAME']
    password = environ['RABBITMQ_PASSWORD']
    exchange = environ['RABBITMQ_EXCHANGE']
    index_name = environ['ELASTIC_INDEX']

    # Declaring the message body according to the index data types and scheme
    body = b'{"Author": "Example Author", "Date": "%b", "Severity": "5", "Status": "New"}' % str(date.today()).encode()

    publish_message_to_rabbitmq(username=username, password=password, index_name=index_name, exchange=exchange,
                                doc=body, hostname='localhost', exchange_type='topic')


if __name__ == '__main__':
    main()
