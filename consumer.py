import os
import sys
from os import environ

import pika as pika
from dotenv import load_dotenv
from elasticsearch import Elasticsearch


def rabbitmq_to_elastic(password: str, username: str, port: int, index_name: str, host_name: str) -> None:
    """
    Function to establish connection between elasticsearch and rabbitmq, will send the RabbitMQ message to elasticsearch
    index
    :param password: User's password
    :param username: User's username
    :param port: User's server port
    :param index_name: User's elasticsearch index name
    :param host_name: User's host name
    :return: None
    """

    channel = connect_to_rabbitmq(host_name, index_name)

    def callback(ch, method, properties, body):
        print(f"Received this message: {body}")
        send_message_to_elastic(port, username, password, body, index_name, host_name)

    channel.basic_consume(queue=index_name, on_message_callback=callback, auto_ack=True)

    print('Waiting for messages from RabbitMQ. To exit press CTRL+C')
    channel.start_consuming()


def send_message_to_elastic(port: int, username: str, password: str, doc: [str, any], index_name: str,
                            host_name: str) -> None:
    """
    Function to send message to elastic from RabbitMQ designated queue
    :param port: User's server port
    :param username: User's username
    :param password: User's password
    :param doc: The message body
    :param index_name: User's elasticsearch index name
    :param host_name: User's host name
    :return: None
    """
    # Create an Elasticsearch client with authentication
    client = Elasticsearch(hosts=[f"http://{host_name}:{port}"], basic_auth=(username, password))

    # Use the index() method to add the document
    client.index(index=index_name, document=doc)


def connect_to_rabbitmq(host_name: str, index_name: str):
    """
    Function that connect client to rabbitmq
    :param host_name: User's hostname
    :param index_name: User's elasticsearch designated index name
    :return: BlockingChannel
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host_name))
    channel = connection.channel()
    channel.queue_declare(queue=index_name)
    return channel


def main():
    load_dotenv()
    rabbitmq_to_elastic(environ['ELASTIC_PASSWORD'], environ['ELASTIC_USERNAME'],
                        int(environ['ELASTIC_PORT']), environ['ELASTIC_INDEX'],
                        environ['ELASTIC_HOST_NAME'])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
