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

package org.apache.geode.redis.internal.pubsub;

import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPUNSUBSCRIBE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bUNSUBSCRIBE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * Class that manages both channel and pattern subscriptions.
 */
public class Subscriptions {

  private final SubscriptionManager<ChannelSubscription> channelSubscriptions =
      new ChannelSubscriptionManager();
  private final SubscriptionManager<PatternSubscription> patternSubscriptions =
      new PatternSubscriptionManager();

  public int getChannelSubscriptionCount(byte[] channel) {
    return channelSubscriptions.getSubscriptionCount(channel);
  }

  public int getPatternSubscriptionCount(byte[] channel) {
    return patternSubscriptions.getSubscriptionCount(channel);
  }

  public int getAllSubscriptionCount(byte[] channel) {
    return getChannelSubscriptionCount(channel) + getPatternSubscriptionCount(channel);
  }

  public void forEachSubscription(byte[] channel, Consumer<Subscription> action) {
    channelSubscriptions.foreachSubscription(channel, action);
    patternSubscriptions.foreachSubscription(channel, action);
  }

  /**
   * Return a list of all subscribed channel names (not including subscribed patterns).
   */
  public List<byte[]> findChannelNames() {
    return channelSubscriptions.getIds();
  }

  /**
   * Return a list of all subscribed channels that match a pattern. This pattern is only applied to
   * channel names and not to actual subscribed patterns. For example, given that the following
   * subscriptions exist: "foo", "foobar" and "fo*" then calling this method with {@code f*} will
   * return {@code foo} and {@code foobar}.
   *
   * @param pattern the glob pattern to search for
   */
  public List<byte[]> findChannelNames(byte[] pattern) {
    return channelSubscriptions.getIds(pattern);
  }

  /**
   * Return a count of all pattern subscriptions including duplicates.
   */
  public int getPatternSubscriptionCount() {
    return patternSubscriptions.getSubscriptionCount();
  }

  @VisibleForTesting
  int getChannelSubscriptionCount() {
    return channelSubscriptions.getSubscriptionCount();
  }

  void add(ChannelSubscription subscription) {
    channelSubscriptions.add(subscription);
  }

  void add(PatternSubscription subscription) {
    patternSubscriptions.add(subscription);
  }

  /**
   * Remove all subscriptions for a given client
   */
  public void remove(Client client) {
    channelSubscriptions.remove(client);
    patternSubscriptions.remove(client);
    client.clearSubscriptions();
  }

  /**
   * @return the total number of all local subscriptions
   */
  @VisibleForTesting
  int size() {
    // this is only used by tests so performance is not an issue
    return getChannelSubscriptionCount() + getPatternSubscriptionCount();
  }

  public SubscribeResult subscribe(byte[] channel, ExecutionHandlerContext context) {
    final Client client = context.getClient();
    ChannelSubscription createdSubscription = null;
    if (client.addChannelSubscription(channel)) {
      createdSubscription = new ChannelSubscription(channel, context, this);
      add(createdSubscription);
    }
    long channelCount = client.getSubscriptionCount();
    return new SubscribeResult(createdSubscription, channelCount, channel);
  }

  public SubscribeResult psubscribe(byte[] patternBytes, ExecutionHandlerContext context) {
    final Client client = context.getClient();
    PatternSubscription createdSubscription = null;
    if (client.addPatternSubscription(patternBytes)) {
      boolean added = false;
      try {
        createdSubscription = new PatternSubscription(patternBytes, context, this);
        add(createdSubscription);
        added = true;
      } finally {
        if (!added) {
          // Must have had a problem parsing the pattern
          client.removePatternSubscription(patternBytes);
        }
      }
    }
    long channelCount = client.getSubscriptionCount();
    return new SubscribeResult(createdSubscription, channelCount, patternBytes);
  }

  private static final Collection<Collection<?>> EMPTY_UNSUBSCRIBE_RESULT =
      singletonList(createUnsubscribeItem(true, null, 0));
  private static final Collection<Collection<?>> EMPTY_PUNSUBSCRIBE_RESULT =
      singletonList(createUnsubscribeItem(false, null, 0));

  public Collection<Collection<?>> unsubscribe(List<byte[]> channels, Client client) {
    if (channels.isEmpty()) {
      channels = client.getChannelSubscriptions();
      if (channels.isEmpty()) {
        return EMPTY_UNSUBSCRIBE_RESULT;
      }
    }
    Collection<Collection<?>> response = new ArrayList<>(channels.size());
    for (byte[] channel : channels) {
      long subscriptionCount = unsubscribe(channel, client);
      response.add(createUnsubscribeItem(true, channel, subscriptionCount));
    }
    return response;
  }

  public Collection<Collection<?>> punsubscribe(List<byte[]> patterns, Client client) {
    if (patterns.isEmpty()) {
      patterns = client.getPatternSubscriptions();
      if (patterns.isEmpty()) {
        return EMPTY_PUNSUBSCRIBE_RESULT;
      }
    }
    Collection<Collection<?>> response = new ArrayList<>(patterns.size());
    for (byte[] pattern : patterns) {
      long subscriptionCount = punsubscribe(pattern, client);
      response.add(createUnsubscribeItem(false, pattern, subscriptionCount));
    }
    return response;
  }

  private long unsubscribe(byte[] channel, Client client) {
    if (client.removeChannelSubscription(channel)) {
      channelSubscriptions.remove(channel, client);
    }
    return client.getSubscriptionCount();
  }

  private long punsubscribe(byte[] patternBytes, Client client) {
    if (client.removePatternSubscription(patternBytes)) {
      patternSubscriptions.remove(patternBytes, client);
    }
    return client.getSubscriptionCount();
  }

  private static List<Object> createUnsubscribeItem(boolean channel, byte[] channelOrPattern,
      long subscriptionCount) {
    return Arrays.asList(channel ? bUNSUBSCRIBE : bPUNSUBSCRIBE, channelOrPattern,
        subscriptionCount);
  }


}
