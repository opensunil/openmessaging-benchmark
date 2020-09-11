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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;

public class ArtemisBenchmarkConsumer implements BenchmarkConsumer {
	private static final Logger log = LoggerFactory.getLogger(ArtemisBenchmarkConsumer.class);

	private final Session session;
	private MessageConsumer consumer = null;

	public ArtemisBenchmarkConsumer(String topic, String queueName, Connection connection, ConsumerCallback callback)
			throws JMSException {
		log.info("Creating session with connection: "+connection);
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		try {
			log.info("Get Queue: "+topic);
			Queue queue = session.createQueue(topic);
			log.info("Get Consumer from queue: "+queue);
			consumer = session.createConsumer(queue);
			log.info("Got Consumer: "+consumer);
		} catch (Exception e) {
			log.warn("Queue creation error: " + e);
		}

		if (consumer != null) {
			log.info("Setting listener on consumer");
			consumer.setMessageListener(message -> {
				byte[] payload = null;
				try {
					log.debug("Retrieve payload");
					payload = message.getBody(byte[].class);
					callback.messageReceived(payload, message.getJMSTimestamp());
				} catch (JMSException e) {
					log.warn("Failed to acknowledge message", e);
				}
			});
			log.info("Added message listener");
		}
		
	}

	@Override
	public void close() throws Exception {
		if (consumer != null)
			consumer.close();
		if (session != null)
			session.close();
	}

}
