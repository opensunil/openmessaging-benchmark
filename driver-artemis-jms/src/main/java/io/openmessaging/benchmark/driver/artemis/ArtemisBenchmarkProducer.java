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

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.benchmark.driver.BenchmarkProducer;

public class ArtemisBenchmarkProducer implements BenchmarkProducer {
	private static final Logger log = LoggerFactory.getLogger(ArtemisBenchmarkProducer.class);

	private Session session;
	private MessageProducer producer;

	public ArtemisBenchmarkProducer(String address, javax.jms.ConnectionFactory cf) {
		session = null;
		producer = null;

		try {
			Connection conn = cf.createConnection();
			conn.start();
			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = session.createQueue(address);
			log.info("Create queue addressed: "+address+" queue: "+queue);
			producer = session.createProducer(queue);
		} catch (Exception e) {
			log.warn("Queue creation error: " + e);
		}

	}

	@Override
	public void close() throws Exception {
		producer.close();
		session.close();
	}

	@Override
	public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {

		try {
			
			BytesMessage msg = session.createBytesMessage();
			msg.setJMSTimestamp(System.currentTimeMillis());

			msg.writeBytes(payload);
			Runnable produceMessage = () -> {
				try {
					producer.send(msg);
					log.debug("Sending message");
				} catch (Exception e) {
					log.error("Error sending message: " + e);
				}
			};
			return CompletableFuture.runAsync(produceMessage);
		} catch (Exception e) {
			log.error("Error sending message");
			return CompletableFuture.runAsync(null);
		}
	}

}
