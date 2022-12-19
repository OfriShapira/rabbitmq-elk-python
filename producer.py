import pika as pika

# producer
# declaring the credentials needed for connection like host, port, userme, password, exchange etc
credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
channel = connection.channel()
channel.exchange_declare('test_exchange', durable=True, exchange_type='topic')
channel.queue_declare(queue='ness_qa_queue')
channel.queue_bind(exchange='test_exchange', queue='ness_qa_queue', routing_key='ness_qa_queue')

# messaging to queue named C
message = b'{"Author": "Reut Koh 3","Date": "2022-12-19","Severity": "5","Status": "New"}'

channel.basic_publish(exchange='test_exchange', routing_key='ness_qa_queue', body=message)
channel.close()
