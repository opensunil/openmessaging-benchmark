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
package io.openmessaging.benchmark.worker.commands;

import java.util.List;

public class TopicsInfo {
    public int numberOfTopics;
    public int numberOfPartitionsPerTopic;

    public boolean createDestinations;
    public String queueName;
    public List<String> topicNames;

    public TopicsInfo() {
    }

    public TopicsInfo(int numberOfTopics, int numberOfPartitionsPerTopic) {
        this.numberOfTopics = numberOfTopics;
        this.numberOfPartitionsPerTopic = numberOfPartitionsPerTopic;
    }

    public boolean shouldCreateDestinations() {
        return createDestinations;
    }

    public void setCreateDestinations(boolean createDestinations) {
        this.createDestinations = createDestinations;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public List<String> getTopicNames() {
        return topicNames;
    }

    public void setTopicNames(List<String> topicNames) {
        this.topicNames = topicNames;
        if (this.queueName==null) {
            this.numberOfTopics = topicNames.size();
        }
    }
}
