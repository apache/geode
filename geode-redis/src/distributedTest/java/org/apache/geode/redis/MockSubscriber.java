/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Client;
import redis.clients.jedis.JedisPubSub;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class MockSubscriber extends JedisPubSub {
  private CountDownLatch latch;
  private List<String> receivedMessages = new ArrayList<String>();
  private Client client;

  private static final Logger logger = LogService.getLogger();

  public MockSubscriber(CountDownLatch latch) {
    this.latch = latch;
  }

  public List<String> getReceivedMessages() {
    return receivedMessages;
  }

  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
    // logger.info("--->>> Received subscription for " + client.getSocket());
    latch.countDown();
  }

  @Override
  public void onMessage(String channel, String message) {
    receivedMessages.add(message);
  }

  @Override
  public void proceed(Client client, String... channels) {
    this.client = client;
    // logger.info("--->>> Before for " + client.getSocket());
    super.proceed(client, channels);
    // logger.info("--->>> After for " + client.getSocket());
  }
}
