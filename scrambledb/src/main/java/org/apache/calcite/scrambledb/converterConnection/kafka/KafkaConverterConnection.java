/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.scrambledb.converterConnection.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.tools.ConverterConnection;
import org.apache.calcite.scrambledb.converterConnection.model.Payload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaConverterConnection implements ConverterConnection {

  private final String kafkaTopic;
  private final String kafkaBootstrapServers;
  private final UUID identifier;

  private final static String CONVERTER_RESPONSE_IDENTIFIER = "response";
  private final static String MESSAGE_KEY_SEPARATOR = "_";
  private final static Duration KAFKA_RESPONSE_TIMEOUT = Duration.ofSeconds(5);

  public KafkaConverterConnection(CalcitePrepare.Context context, UUID identifier) {
    this.kafkaBootstrapServers = context.config().converterKafkaBootstrapServers();
    // identifier for kafka messaging
    this.identifier = identifier;
    // the kafka topic to produce and subscribe
    this.kafkaTopic = context.config().converterKafkaTopic();
  }

  @Override public Type getType() {
    return Type.KAFKA;
  }

  @Override public List<String> getPseudonyms(List<String> input) {
    return interaction(input,"pseudonym");
  }

  @Override public List<String> convert(List<String> pseudonyms) {
    return interaction(pseudonyms, "convert");
  }


  public List<String> interaction(List<String> payload, String taskIdentifier) {
    final KafkaConsumer<String, JsonNode> consumer = getConsumer();

    final String responseIdentifier =
        this.identifier + MESSAGE_KEY_SEPARATOR + CONVERTER_RESPONSE_IDENTIFIER;
    final Payload[] response = {null};
    final CountDownLatch latch = new CountDownLatch(1);
    Thread consumerThread = new Thread(() -> {
      consumer.subscribe(Collections.singletonList(this.kafkaTopic));

      while (response[0] == null){
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, JsonNode> record : records) {
          if (record.key().contains(responseIdentifier)) {
            response[0] = new ObjectMapper()
                .convertValue(record.value(), new TypeReference<Payload>(){});
            break;
          }
        }
      }
      latch.countDown(); // Release await() in the test thread.
      consumer.close();
    });

    consumerThread.start();

    final KafkaProducer<String, Payload> producer = getProducer();
    final String key = this.identifier + MESSAGE_KEY_SEPARATOR + taskIdentifier;

    Payload request = new Payload(payload);

    ProducerRecord<String, Payload> record = new ProducerRecord<>(this.kafkaTopic, key, request);
    producer.send(record);
    producer.close();

    try {
      if (latch.await(KAFKA_RESPONSE_TIMEOUT.get(ChronoUnit.SECONDS), TimeUnit.SECONDS)) {
        return response[0].getData();
      } else {
        throw new Exception("[Error] could not get response from converter. Session timeout.");
      }
    } catch (Exception e) {
      e.printStackTrace();
      return new ArrayList<>();
    }

  }

  private KafkaProducer<String, Payload> getProducer() {
    // Kafka config
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "2147483647");

    return new KafkaProducer<String, Payload>(properties);
  }

  private KafkaConsumer<String, JsonNode> getConsumer() {
    // Kafka config
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.GROUP_ID_CONFIG, this.identifier.toString());
    properties.setProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    return new KafkaConsumer<String, JsonNode>(properties);
  }

}
