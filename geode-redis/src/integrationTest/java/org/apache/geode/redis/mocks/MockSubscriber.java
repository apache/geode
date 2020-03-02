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

package org.apache.geode.redis.mocks;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.JedisPubSub;

public class MockSubscriber extends JedisPubSub {
  private List<String> receivedMessages = new ArrayList<>();
  private List<String> receivedPMessages = new ArrayList<>();

  public List<String> getReceivedMessages() {
    return receivedMessages;
  }

  public List<String> getReceivedPMessages() {
    return receivedPMessages;
  }

  @Override
  public void onMessage(String channel, String message) {
    receivedMessages.add(message);
  }

  @Override
  public void onPMessage(String pattern, String channel, String message) {
    receivedPMessages.add(message);
  }
}
