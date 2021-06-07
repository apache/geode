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

package org.apache.geode.redis.internal.publishAndSubscribe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.Coder;
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

  public List<byte[]> findChannelNames() {

    ObjectOpenCustomHashSet<byte[]> hashSet =
        new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);

    findChannelSubscriptions()
        .forEach(channel -> hashSet.add(channel.getSubscriptionName()));

    return new ArrayList<>(hashSet);
  }

  public List<byte[]> findChannelNames(byte[] pattern) {

    GlobPattern globPattern = new GlobPattern(Coder.bytesToString(pattern));

    return findChannelNames()
        .stream()
        .filter(name -> globPattern.matches(Coder.bytesToString((byte[]) name)))
        .collect(Collectors.toList());
  }

  public List<Object> findNumberOfSubscribersForChannel(List<byte[]> names) {
    List<Object> result = new ArrayList<>();

    names.forEach(name -> {
      Long subscriptionCount =
          findSubscriptions(name)
              .stream()
              .filter(subscription -> subscription instanceof ChannelSubscription)
              .count();

      result.add(name);
      result.add(subscriptionCount);

    });

    return result;
  }

  private Stream<ChannelSubscription> findChannelSubscriptions() {
    return subscriptions.stream()
        .filter(subscription -> subscription instanceof ChannelSubscription)
        .map(subscription -> (ChannelSubscription) subscription);
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
    GlobPattern pattern = new GlobPattern(new String(patternBytes));
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
