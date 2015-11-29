# kafka-clients

##A Node.js module for [Apache Kafka](http://kafka.apache.org/documentation.html) 0.9.0 and later

This module provides clients for consuming from (kafka-clients.KafkaConsumer) and publishing to (kafka-clients.KafkaProducer) Apache Kafka.  It is loosely modeled after the [Java clients API](http://kafka.apache.org/090/javadoc/index.html) distributed with Apache Kafka.

##Documentation
[Kafka Clients API](https://github.com/mastersingh24/kafka-clients/wiki/API-Documentation)

##Motivation

While there are several very good Kafka clients for Node.js, most of them were initially written against earlier versions of the Kafka API.  There have been significant changes in the Kafka 0.8.2 and 0.9.0 releases and since the Kafka project itself provided major rewrites of the Java consumer and producer clients, it seemed like it was time to create new Node.js clients as well.

##Installation

TBD

##Prerequisite

Requires Apache Kafka 0.9.0 or later

##Acknowledgements

Parts of the architecture and design were inspired by the following projects:
* [Kafkaesque](https://github.com/pelger/Kafkaesque)
* [node-amqp](https://github.com/postwait/node-amqp)
