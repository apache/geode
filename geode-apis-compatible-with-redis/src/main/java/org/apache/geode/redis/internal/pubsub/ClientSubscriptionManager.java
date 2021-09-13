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

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscriptions.ForEachConsumer;

/**
 * An instance of this interface keeps track of all the clients
 * that have active subscriptions to the channel or pattern that
 * this instance represents.
 * In a given manager all of its subscriptions will have the same name.
 */
interface ClientSubscriptionManager {

  /**
   * Call the given action for each subscription this manager
   * has that matches the given channel.
   * Note that some managers support a pattern
   * that the channel needs to match.
   * For managers without a pattern all subscriptions match.
   *
   * @param channelToMatch if non-null and the manager supports matching
   *        then only invoke action for subscriptions that match this.
   */
  void forEachSubscription(byte[] subscriptionName, String channelToMatch, ForEachConsumer action);

  /**
   * return how many subscriptions this manager has.
   */
  int getSubscriptionCount();

  /**
   * returns how many subscriptions this manager has
   * that match the given channel.
   * Note that some managers support a pattern
   * that the channel needs to match.
   * For managers without a pattern all subscriptions match.
   */
  int getSubscriptionCount(String channel);

  /**
   * Adds the given subscription for the given client to this manager.
   * If this manager already has a subscription
   * for that client then the old subscription is
   * removed and the new one is added.
   *
   * @return true if added or already added; false if caller should retry
   */
  boolean add(Client client, Subscription subscription);

  /**
   * Remove any subscription added for the given client.
   *
   * @return true if removed or already removed; false if caller should remove this manager
   */
  boolean remove(Client client);
}
