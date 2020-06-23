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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

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
  public List<Subscription> findSubscriptions(String channelOrPattern) {
    return subscriptions.stream()
        .filter(subscription -> subscription.matches(channelOrPattern))
        .collect(Collectors.toList());
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

  public synchronized long subscribe(String channel, ExecutionHandlerContext context,
      Client client) {
    if (!exists(channel, client)) {
      add(new ChannelSubscription(client, channel, context));
    }
    return findSubscriptions(client).size();
  }

  public synchronized long psubscribe(GlobPattern pattern, ExecutionHandlerContext context,
      Client client) {
    if (!exists(pattern, client)) {
      add(new PatternSubscription(client, pattern, context));
    }
    return findSubscriptions(client).size();
  }

  public synchronized long unsubscribe(Object channelOrPattern, Client client) {
    remove(channelOrPattern, client);
    return findSubscriptions(client).size();
  }

}
