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

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * Class that manages both channel and pattern subscriptions.
 */
public class Subscriptions {
  private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

  /**
   * Check whether a given client has already subscribed to a channel or pattern
   *
   * @param channelOrPattern channel or pattern
   * @param client a client connection
   */
  @VisibleForTesting
  boolean exists(Object channelOrPattern, Client client) {
    return subscriptions.stream()
        .anyMatch(subscription -> subscription.isEqualTo(channelOrPattern, client));
  }

  /**
   * Return all subscriptions for a given client
   *
   * @param client the subscribed client
   * @return a list of subscriptions
   */
  public List<Subscription> findSubscriptions(Client client) {
    return subscriptions.stream()
        .filter(subscription -> subscription.matchesClient(client))
        .collect(Collectors.toList());
  }

  /**
   * Return all subscriptions for a given channel or pattern
   *
   * @param channelOrPattern the channel or pattern
   * @return a list of subscriptions
   */
  public List<Subscription> findSubscriptions(byte[] channelOrPattern) {
    return subscriptions.stream()
        .filter(subscription -> subscription.matches(channelOrPattern))
        .collect(Collectors.toList());
  }

  /**
   * Return a list of all subscribed channel names (not including subscribed patterns).
   */
  public List<byte[]> findChannelNames() {
    ObjectOpenCustomHashSet<byte[]> channelNames =
        new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);

    subscriptions.stream()
        .filter(s -> s instanceof ChannelSubscription)
        .forEach(channel -> channelNames.add(channel.getSubscriptionName()));

    return new ArrayList<>(channelNames);
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

    GlobPattern globPattern = new GlobPattern(bytesToString(pattern));

    return findChannelNames()
        .stream()
        .filter(name -> globPattern.matches(bytesToString(name)))
        .collect(Collectors.toList());
  }

  /**
   * Return a list consisting of pairs {@code channelName, subscriptionCount}.
   *
   * @param names a list of the names to consider. This should not include any patterns.
   */
  public List<Object> findNumberOfSubscribersPerChannel(List<byte[]> names) {
    List<Object> result = new ArrayList<>();

    names.forEach(name -> {
      Long subscriptionCount = findSubscriptions(name)
          .stream()
          .filter(subscription -> subscription instanceof ChannelSubscription)
          .count();

      result.add(name);
      result.add(subscriptionCount);
    });

    return result;
  }

  /**
   * Add a new subscription
   */
  @VisibleForTesting
  void add(Subscription subscription) {
    subscriptions.add(subscription);
  }

  /**
   * Remove all subscriptions for a given client
   */
  public void remove(Client client) {
    subscriptions.removeIf(subscription -> subscription.matchesClient(client));
  }

  /**
   * Remove a single subscription
   */
  @VisibleForTesting
  boolean remove(Object channel, Client client) {
    return subscriptions.removeIf(subscription -> subscription.isEqualTo(channel, client));
  }

  /**
   * @return the total number of all local subscriptions
   */
  @VisibleForTesting
  int size() {
    return subscriptions.size();
  }

  public synchronized SubscribeResult subscribe(byte[] channel, ExecutionHandlerContext context,
      Client client) {
    Subscription createdSubscription = null;
    if (!exists(channel, client)) {
      createdSubscription = new ChannelSubscription(client, channel, context, this);
      add(createdSubscription);
    }
    long channelCount = findSubscriptions(client).size();
    return new SubscribeResult(createdSubscription, channelCount, channel);
  }

  public SubscribeResult psubscribe(byte[] patternBytes, ExecutionHandlerContext context,
      Client client) {
    GlobPattern pattern = new GlobPattern(bytesToString(patternBytes));
    Subscription createdSubscription = null;
    synchronized (this) {
      if (!exists(pattern, client)) {
        createdSubscription = new PatternSubscription(client, pattern, context, this);
        add(createdSubscription);
      }
      long channelCount = findSubscriptions(client).size();
      return new SubscribeResult(createdSubscription, channelCount, patternBytes);
    }
  }

  public synchronized long unsubscribe(Object channelOrPattern, Client client) {
    remove(channelOrPattern, client);
    return findSubscriptions(client).size();
  }

}
