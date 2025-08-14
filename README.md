This project demonstrates a real-time data pipeline using Docker to orchestrate several services: a Kafka message broker, a Spark cluster, and a Python application that streams data from Kafka into a Delta Lake table.

The pipeline consists of three main components:

  1.A Kafka Producer that generates sample JSON messages and sends them to a Kafka topic.
  2.A Spark Streaming job (the consumer) that reads messages from the Kafka topic.
  3.A Delta Lake table where the Spark job continuously appends the streaming data.

The setup is managed by docker-compose.yml, which handles service dependencies and network configuration.

Make sure your project directory contains the docker-compose.yml, kafka_producer.py, and kafka_consumer.py files.

Open a terminal in the project's root directory.
Run the following command to start all the services in detached mode:
  docker-compose up -d

To view the logs from all services in a single stream, run:
  docker-compose logs -f

To stop and remove all services, run:
  docker-compose down
