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
package io.openmessaging.benchmark.driver.artemis;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import javax.jms.Connection;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;

public class ArtemisBenchmarkDriver implements BenchmarkDriver {
	private ArtemisConfig config;

	private InitialContext context;
	private Connection connection;
	private Session session;

	@Override
	public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {

		this.config = readConfig(configurationFile);
		log.info("ActiveMQ Artemis driver configuration: {}", writer.writeValueAsString(config));
		try {
			Hashtable<String, Object> jndi_env = new Hashtable<String, Object>();
			jndi_env.put(InitialContext.INITIAL_CONTEXT_FACTORY,
					//"org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
//					"org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
					"org.apache.qpid.jms.jndi.JmsInitialContextFactory");
			jndi_env.put("connectionFactory.myFactoryLookup", config.brokerAddress);

			context = new InitialContext(jndi_env);
			JmsConnectionFactory cf = (JmsConnectionFactory) context.lookup("myFactoryLookup");
			connection = cf.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public String getTopicNamePrefix() {
		return "test";
	}

	@Override
	public CompletableFuture<Void> createTopic(String topic, int partitions) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		if (partitions != 1) {
			future.completeExceptionally(new IllegalArgumentException("Partitions are not supported in Artemis"));
			return future;
		}

		ForkJoinPool.commonPool().submit(() -> {
			try {
				log.info("Creating queue: "+topic);
				session.createQueue(topic);
				log.info("Create queue task complete");
				future.complete(null);
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		});

		return future;
	}

	@Override
	public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
		try {
			return CompletableFuture.completedFuture(new ArtemisBenchmarkProducer(topic, connection));
		} catch (Exception e) {
			CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
			future.completeExceptionally(e);
			return future;
		}
	}

	@Override
	public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
			ConsumerCallback consumerCallback) {
		CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
		ForkJoinPool.commonPool().submit(() -> {
			try {
				String queueName = topic + "-" + subscriptionName;
				BenchmarkConsumer consumer = new ArtemisBenchmarkConsumer(topic, queueName, connection,
						consumerCallback);
				future.complete(consumer);
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		});

		return future;
	}

	@Override
	public void close() throws Exception {
		log.info("Shutting down ActiveMQ Artemis benchmark driver");
		
		if (session!=null) {
			session.close();
		}
		
		if (connection != null) {
			connection.close();
		}
		log.info("ActiveMQ Artemis benchmark driver successfully shut down");
	}

	private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static ArtemisConfig readConfig(File configurationFile) throws IOException {
		return mapper.readValue(configurationFile, ArtemisConfig.class);
	}

	private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
	private static final Logger log = LoggerFactory.getLogger(ArtemisBenchmarkProducer.class);
}
