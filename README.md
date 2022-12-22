# RabbitMQ to Elasticsearch
Two scripts that simulate communication between RabbitMQ and Elasticsearch using Python. <br/>
In order to run the project, you must create an .env file that will contain the Elasticsearch and RabbitMQ login details and put it in your project directory.
The .env file should include the following format:
```
ELASTIC_PASSWORD=<example>
ELASTIC_USERNAME=<example>
ELASTIC_PORT=<example>
ELASTIC_INDEX=<example>
ELASTIC_HOST_NAME=<example>

RABBITMQ_USERNAME=<example>
RABBITMQ_PASSWORD=<example>
```