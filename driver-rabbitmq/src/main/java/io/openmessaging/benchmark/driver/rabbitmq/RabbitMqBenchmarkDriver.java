/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.rabbitmq;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.BaseEncoding;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.rabbitmq.RabbitMqConfig.QueueType;

import org.apache.bookkeeper.stats.StatsLogger;

// Based on the KeyDistributor from the framework
// Generates routing keys to "channel" messages to specific queues
// Common to all producers for a workload
class RoutingKeyGenerator {
    private AtomicInteger idx = new AtomicInteger(0);
    private final int KEY_COUNT;
    private final String[] keys;

    RoutingKeyGenerator(int partitions, int keyLength) {
        KEY_COUNT = partitions;
        keys = new String[KEY_COUNT];

        byte[] buffer = new byte[keyLength];
        Random random = new Random();
        for (int i = 0; i < keys.length; i++) {
            random.nextBytes(buffer);
            keys[i] = BaseEncoding.base64Url().omitPadding().encode(buffer);
        }
    }

    public String next() {
        return keys[idx.getAndIncrement() % KEY_COUNT];
    }
}

class ConnectionManager {
    private List<ConnectionFactory> connectionFactory = new ArrayList<>();
    private List<Connection> connections = new ArrayList<>();
    private AtomicInteger idx = new AtomicInteger(0);

    ConnectionManager(String[] brokers) {
        for (String broker : brokers) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setAutomaticRecoveryEnabled(true);
            factory.setHost(broker);
            factory.setUsername("admin");
            factory.setPassword("admin");
            connectionFactory.add(factory);
        }
    }

    // Round robins across all brokers
    public Connection connectAny() throws IOException, TimeoutException {
        Connection connection = connectionFactory.get(idx.getAndIncrement() % connectionFactory.size()).newConnection();
        connections.add(connection);

        return connection;
    }

    public void close() {
        connections.forEach(connection -> {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}

public class RabbitMqBenchmarkDriver implements BenchmarkDriver {

    private RabbitMqConfig config;

    private ConnectionManager connectionManager;

    private BuiltinExchangeType exchangeType;

    private Map<String, RoutingKeyGenerator> topicRoutingKeyGenerator = new HashMap<>();

    private int routingKeyLength;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, RabbitMqConfig.class);
        connectionManager = new ConnectionManager(config.brokers);
        routingKeyLength = config.routingKeyLength;
    }

    @Override
    public void close() throws Exception {
        connectionManager.close();
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-topic";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        topicRoutingKeyGenerator.put(topic, new RoutingKeyGenerator(partitions, routingKeyLength));
        // Use routing keys to simulate multiple partitions, if no. of partitions > 1
        if (partitions > 1) {
            exchangeType = BuiltinExchangeType.TOPIC;
        } else {
            exchangeType = BuiltinExchangeType.FANOUT;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Connection connection = connectionManager.connectAny();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(topic, exchangeType, config.messagePersistence);
            future.complete(null);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();

        try {
            Connection connection = connectionManager.connectAny();
            Channel channel = connection.createChannel();
            channel.confirmSelect();
            future = CompletableFuture.completedFuture(new RabbitMqBenchmarkProducer(channel, topic,
                    config.messagePersistence, topicRoutingKeyGenerator.get(topic)));
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
            ConsumerCallback consumerCallback) {

        CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> {
            try {
                String queueName = topic + "-" + subscriptionName;
                Connection connection = connectionManager.connectAny();
                Channel channel = connection.createChannel();

                // Consume everything (all "partitions")
                // channel.exchangeDeclare(topic, exchangeType, config.messagePersistence);

                Map<String, Object> args = new HashMap<>();
                args.put("x-queue-type", config.queueType.toString().toLowerCase());
                channel.queueDeclare(queueName, config.messagePersistence, false, false, args);
                channel.queueBind(queueName, topic, "#");
                future.complete(new RabbitMqBenchmarkConsumer(channel, queueName, consumerCallback));
            } catch (IOException | TimeoutException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
}
