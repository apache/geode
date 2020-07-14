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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Client;
import redis.clients.jedis.JedisPubSub;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class MockSubscriber extends JedisPubSub {
  private static final Logger logger = LogService.getLogger();
  private Client client;
  private final CountDownLatch subscriptionLatch;
  private final CountDownLatch unsubscriptionLatch;
  private final List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
  private final List<String> receivedPMessages = Collections.synchronizedList(new ArrayList<>());
  public final List<UnsubscribeInfo> unsubscribeInfos =
      Collections.synchronizedList(new ArrayList<>());
  public final List<UnsubscribeInfo> punsubscribeInfos =
      Collections.synchronizedList(new ArrayList<>());

  public MockSubscriber() {
    this(new CountDownLatch(1));
  }

  public MockSubscriber(CountDownLatch subscriptionLatch) {
    this(subscriptionLatch, new CountDownLatch(1));
  }

  public MockSubscriber(CountDownLatch subscriptionLatch, CountDownLatch unsubscriptionLatch) {
    this.subscriptionLatch = subscriptionLatch;
    this.unsubscriptionLatch = unsubscriptionLatch;
  }

  public List<String> getReceivedMessages() {
    return receivedMessages;
  }

  public List<String> getReceivedPMessages() {
    return new ArrayList<>(receivedPMessages);
  }

  @Override
  public void onMessage(String channel, String message) {
    receivedMessages.add(message);
  }

  @Override
  public void onPMessage(String pattern, String channel, String message) {
    receivedPMessages.add(message);
  }

  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
    subscriptionLatch.countDown();
  }

  public void awaitSubscribe() {
    try {
      subscriptionLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onUnsubscribe(String channel, int subscribedChannels) {
    unsubscribeInfos.add(new UnsubscribeInfo(channel, subscribedChannels));
    unsubscriptionLatch.countDown();
  }

  public void awaitUnsubscribe() {
    try {
      unsubscriptionLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onPUnsubscribe(String pattern, int subscribedChannels) {
    punsubscribeInfos.add(new UnsubscribeInfo(pattern, subscribedChannels));
  }

  public static class UnsubscribeInfo {
    public final String channel;
    public final int count;

    public UnsubscribeInfo(String channel, int count) {
      this.channel = channel;
      this.count = count;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UnsubscribeInfo)) {
        return false;
      }
      UnsubscribeInfo that = (UnsubscribeInfo) o;
      return count == that.count &&
          Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {
      return Objects.hash(channel, count);
    }

    @Override
    public String toString() {
      return "UnsubscribeInfo{" +
          "channel='" + channel + '\'' +
          ", count=" + count +
          '}';
    }
  }

}
