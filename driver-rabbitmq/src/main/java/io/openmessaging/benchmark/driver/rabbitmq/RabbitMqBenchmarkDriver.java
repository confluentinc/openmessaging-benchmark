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
    private final int KEY_BYTE_SIZE = 7;

    private AtomicInteger idx = new AtomicInteger(0);
    private final int KEY_COUNT;
    private final String[] keys;

    RoutingKeyGenerator(int partitions) {
        KEY_COUNT = partitions;
        keys = new String[KEY_COUNT];

        byte[] buffer = new byte[KEY_BYTE_SIZE];
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

public class RabbitMqBenchmarkDriver implements BenchmarkDriver {

    private RabbitMqConfig config;

    private List<Connection> connections = new ArrayList<>();

    private ConnectionFactory connectionFactory;

    private BuiltinExchangeType exchangeType;

    private Map<String, RoutingKeyGenerator> topicRoutingKeyGenerator = new HashMap<>();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, RabbitMqConfig.class);
        connectionFactory = new ConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setHost(config.brokerAddress);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");

        if (!config.connectionPerChannel) {
            try {
                Connection connection = connectionFactory.newConnection();
                connections.add(connection);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        connections.forEach(connection -> {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-topic";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        topicRoutingKeyGenerator.put(topic, new RoutingKeyGenerator(partitions));
        // Use routing keys to simulate multiple partitions, if no. of partitions > 1
        if (partitions > 1) {
            exchangeType = BuiltinExchangeType.TOPIC;
        } else {
            exchangeType = BuiltinExchangeType.FANOUT;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Connection connection = null;
            try {
                connection = connectionFactory.newConnection();
                connections.add(connection);
            } catch (TimeoutException e) {
                e.printStackTrace();
                future.completeExceptionally(e);
                return future;
            }

            Channel channel = connection.createChannel();
            channel.exchangeDeclare(topic, exchangeType);
        } catch (IOException e) {
            e.printStackTrace();
            future.completeExceptionally(e);
            return future;
        }

        future.complete(null);

        return future;
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
        Channel channel = null;
        try {
            Connection connection = null;
            if (config.connectionPerChannel) {
                try {
                    connection = connectionFactory.newConnection();
                    connections.add(connection);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    future.completeExceptionally(e);
                    return future;
                }
            } else {
                connection = connections.get(0);
            }

            channel = connection.createChannel();
            channel.confirmSelect();
        } catch (IOException e) {
            e.printStackTrace();
            future.completeExceptionally(e);
            return future;
        }

        future = CompletableFuture.completedFuture(new RabbitMqBenchmarkProducer(channel, topic,
                config.messagePersistence, topicRoutingKeyGenerator.get(topic)));

        return future;
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
            ConsumerCallback consumerCallback) {

        CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> {
            try {
                String queueName = topic + "-" + subscriptionName;
                Channel channel = null;
                Connection connection = null;
                if (config.connectionPerChannel) {
                    try {
                        connection = connectionFactory.newConnection();
                        connections.add(connection);
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    }
                } else {
                    connection = connections.get(0);
                }

                // Create queue
                try {
                    channel = connection.createChannel();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Map<String, Object> args = new HashMap<>();
                args.put("x-queue-type", config.queueType.toString().toLowerCase());
                channel.queueDeclare(queueName, true, false, false, args);
                // Consume everything (all "partitions")
                channel.queueBind(queueName, topic, "#");
                future.complete(new RabbitMqBenchmarkConsumer(channel, queueName, consumerCallback));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
}
