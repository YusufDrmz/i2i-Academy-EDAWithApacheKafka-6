
# i2i Academy - EDA with Apache Kafka

This repository contains a simple implementation of an Event-Driven Architecture (EDA) using Apache Kafka and Java. 

## Project Structure
* **docker-compose.yml**: Configuration file to spin up Apache Kafka and Zookeeper instances in a containerized environment.
* **producer/**: A Java applications that generates and publishes custom message objects to a specific Kafka topic.
* **consumer/**: A Java application that listens to the designated Kafka topic, reads the incoming objects, and prints them to the console.

## How to Run Locally

1. **Start Kafka & Zookeeper:**
   ```bash
   docker compose up -d
Run the Consumer:
Execute the KafkaConsumerApp.java main method to start listening for messages.

Run the Producer:
Execute the KafkaProducerApp.java main method to publish sample events to the topic.
