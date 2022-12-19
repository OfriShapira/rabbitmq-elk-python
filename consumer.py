import os
import sys
from os import environ

import pika as pika
from dotenv import load_dotenv
from elasticsearch import Elasticsearch


def connect_to_elastic():
    load_dotenv()
    password = environ['ELASTIC_PASSWORD']
    username = environ['ELASTIC_USERNAME']
    print()
    print()
    port = 9200
    index_name = "ness_qa_queue"
    host_name = "localhost"

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host_name))
    channel = connection.channel()

    channel.queue_declare(queue=index_name)

    def callback(ch, method, properties, body):
        print("Received %r" % body)
        send_doc_to_elastic(port, username, password, body, index_name)

    channel.basic_consume(queue=index_name, on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def send_doc_to_elastic(port, username, password, doc, index_name):
    # Create an Elasticsearch client with authentication
    client = Elasticsearch(hosts=[f"http://localhost:{port}"], basic_auth=(username, password))

    # Use the index() method to add the document
    client.index(index=index_name, document=doc)


if __name__ == '__main__':
    try:
        connect_to_elastic()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
