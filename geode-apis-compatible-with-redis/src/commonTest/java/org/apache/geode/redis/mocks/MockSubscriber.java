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
 */

package org.apache.geode.redis.mocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Client;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;


public class MockSubscriber extends JedisPubSub {

  private final CountDownLatch subscriptionLatch;
  private final CountDownLatch psubscriptionLatch;
  private final CountDownLatch unsubscriptionLatch;
  private final CountDownLatch pUnsubscriptionLatch;
  private final List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
  private final List<String> receivedPMessages = Collections.synchronizedList(new ArrayList<>());
  private final List<String> receivedPings = Collections.synchronizedList(new ArrayList<>());
  private final List<String> receivedEvents = Collections.synchronizedList(new ArrayList<>());
  public final List<UnsubscribeInfo> unsubscribeInfos =
      Collections.synchronizedList(new ArrayList<>());
  public final List<UnsubscribeInfo> punsubscribeInfos =
      Collections.synchronizedList(new ArrayList<>());
  private CountDownLatch messageReceivedLatch = new CountDownLatch(0);
  private CountDownLatch pMessageReceivedLatch = new CountDownLatch(0);
  private String localSocketAddress;
  private Client client;

  public MockSubscriber() {
    this(new CountDownLatch(1));
  }

  public MockSubscriber(CountDownLatch subscriptionLatch) {
    this(subscriptionLatch, new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1));
  }

  public MockSubscriber(CountDownLatch subscriptionLatch, CountDownLatch unsubscriptionLatch,
      CountDownLatch psubscriptionLatch,
      CountDownLatch pUnsubscriptionLatch) {
    this.subscriptionLatch = subscriptionLatch;
    this.psubscriptionLatch = psubscriptionLatch;
    this.unsubscriptionLatch = unsubscriptionLatch;
    this.pUnsubscriptionLatch = pUnsubscriptionLatch;
  }

  @Override
  public void proceed(Client client, String... channels) {
    localSocketAddress = client.getSocket().getLocalSocketAddress().toString();
    this.client = client;
    super.proceed(client, channels);
  }

  private void switchThreadName(String suffix) {
    String threadName = Thread.currentThread().getName();
    int suffixIndex = threadName.indexOf(" -- ");
    if (suffixIndex >= 0) {
      threadName = threadName.substring(0, suffixIndex);
    }

    threadName += " -- " + suffix + " [" + localSocketAddress + "]";
    Thread.currentThread().setName(threadName);
  }

  public List<String> getReceivedMessages() {
    return receivedMessages;
  }

  public List<String> getReceivedPMessages() {
    return new ArrayList<>(receivedPMessages);
  }

  public List<String> getReceivedPings() {
    return receivedPings;
  }

  public List<String> getReceivedEvents() {
    return receivedEvents;
  }

  @Override
  public void onMessage(String channel, String message) {
    switchThreadName(String.format("MESSAGE %s %s", channel, message));
    receivedMessages.add(message);
    receivedEvents.add("message");
    messageReceivedLatch.countDown();
  }

  @Override
  public void onPMessage(String pattern, String channel, String message) {
    switchThreadName(String.format("PMESSAGE %s %s %s", pattern, channel, message));
    receivedPMessages.add(message);
    receivedEvents.add("pmessage");
    pMessageReceivedLatch.countDown();
  }

  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
    switchThreadName(String.format("SUBSCRIBE %s", channel));
    subscriptionLatch.countDown();
  }

  @Override
  public void onPSubscribe(String pattern, int subscribedChannels) {
    switchThreadName(String.format("PSUBSCRIBE %s", pattern));
    psubscriptionLatch.countDown();
  }

  @Override
  public void onPong(String pattern) {
    switchThreadName(String.format("PONG %s", pattern));
    receivedPings.add(pattern);
  }

  // JedisPubSub ping with message is not currently possible, will submit a PR
  // (https://github.com/xetorthio/jedis/issues/2049)
  public void ping(String message) {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub is not subscribed to a Jedis instance.");
    } else {
      this.client.ping(message);
    }
  }

  private static final int AWAIT_TIMEOUT_MILLIS = 30000;

  public void awaitSubscribe(String channel) {
    try {
      if (!subscriptionLatch.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("awaitSubscribe timed out for channel: " + channel);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void awaitPSubscribe(String pattern) {
    try {
      if (!psubscriptionLatch.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("awaitSubscribe timed out for channel: " + pattern);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onUnsubscribe(String channel, int subscribedChannels) {
    switchThreadName(String.format("UNSUBSCRIBE %s %d", channel, subscribedChannels));
    unsubscribeInfos.add(new UnsubscribeInfo(channel, subscribedChannels));
    unsubscriptionLatch.countDown();
  }

  public void awaitUnsubscribe(String channel) {
    try {
      if (!unsubscriptionLatch.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("awaitUnsubscribe timed out for channel: " + channel);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onPUnsubscribe(String pattern, int subscribedChannels) {
    switchThreadName(String.format("PUNSUBSCRIBE %s %d", pattern, subscribedChannels));
    punsubscribeInfos.add(new UnsubscribeInfo(pattern, subscribedChannels));
    pUnsubscriptionLatch.countDown();
  }

  public void awaitPunsubscribe(String pChannel) {

    try {
      if (!pUnsubscriptionLatch.await(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("awaitPUnsubscribe timed out for pattern: " + pChannel);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void preparePMessagesReceivedLatch(int expectedMessages) {
    pMessageReceivedLatch = new CountDownLatch(expectedMessages);
  }

  public void awaitPMessagesReceived() {
    try {
      assertThat(pMessageReceivedLatch.await(30, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void prepareMessagesReceivedLatch(int expectedMessages) {
    messageReceivedLatch = new CountDownLatch(expectedMessages);
  }

  public void awaitMessagesReceived() {
    try {
      assertThat(messageReceivedLatch.await(30, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
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
