# Message Broker in Scala

## Table of Contents
1. [General Info](#general-info)
2. [Technologies](#technologies)
3. [Implemeted features](#implemeted-features)
***

### General Info
This project contains a message broker and example of producer and consumer. The message broker can accept conections from multiple producers and send them to the conected consumers. It alows them to publish messages with topics which aren't known beforehand. It also contains a mechanism for resending of the messages that haven't been confirmed to arrive at the consumer.<br>
In the given example of published data, the topics are related to border checkpoints of a country, each being represented by a producer. It generates data about the number of people have antered and heva leaved the country. The consumers represent state authrities that are receiving the information about immigration and emingration. 
***

## Technologies
The technologies used within the project, with the dependencies managed by sbt.
* [Akka 2.6.18](https://akka.io/) - for creating and managing actors
* [LogBack 1.2.11](https://search.maven.org/artifact/ch.qos.logback/logback-classic/1.2.11/jar) - for printing logs.
* [ReactiveMongo 1.1.0](https://search.maven.org/artifact/org.reactivemongo/play2-reactivemongo_3/1.1.0-play28-RC4/jar) - for MongoDB connection.
* [MUnit 0.7.26](https://scalameta.org/munit/) - testing library.
***

## Implemeted features
- [x] The message broker is a dedicated async TCP server.
- [x] [Dynamic connection](#dynamic-connection) - the publishers and consumers can connect / subscribe and disconnect / unsubscribe at any time.
- [x] [Multiple topics support](#multiple-topics-support) - Producers / Consumers are able to publish / subscribe to zero, one or many topics.
- [x] [Serialized messages](#serialized-messages) - the messages are represented as classes which are serialized to be sent through TCP connection.
- [x] [Delivery Guarantee](#delivery-guarantee) - the subscriber sends an acknowledgment for every received message.
- [x] [Message priority](#message-priority) - the publisher sets the priority of each message.
- [x] Persistent messages - saved in a file, with an interval specified in broker.properties file.
- [x] Docker Compose configuration - the containers are connecting in a bridge network, and communicating through TCP sockets.

## How to run
### Build docker images:
To build 3 distinct images, each one running either hte broker,producer or consumer, change the following line in the build.sbt file:<br>
```Compile / run / mainClass := Some("<main_file_location>")```<br>
The main_file_location values are:
- for messsage broker: Broker.MessageBroker
- for producer: Producer.Producer
- for consumer: Consumer.Consumer 
For each main_file_location value, load the sbt changes and compile the project. Then build the image with commands: 
- ```docker build -t broker:v1 .```
- ```docker build -t producer:v1 .```
- ```docker build -t consumer:v1 .```
### Run containers:
```docker compose up```
