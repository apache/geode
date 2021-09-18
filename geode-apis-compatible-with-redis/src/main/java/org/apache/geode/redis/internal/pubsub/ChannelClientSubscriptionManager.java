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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscriptions.ForEachConsumer;

class ChannelClientSubscriptionManager
    implements ClientSubscriptionManager {
  private final Map<Client, Subscription> subscriptionMap = new ConcurrentHashMap<>();
  /**
   * This is used instead of subscriptionMap.size() because we need to
   * make sure that once size goes to zero no further adds
   * will be made to this manager. Instead a new manager
   * needs to be created.
   */
  private final AtomicInteger size = new AtomicInteger(1);

  public ChannelClientSubscriptionManager(Client client, Subscription subscription) {
    subscriptionMap.put(client, subscription);
  }

  @Override
  public int getSubscriptionCount() {
    return size.get();
  }

  @Override
  public int getSubscriptionCount(byte[] channel) {
    return size.get();
  }

  @Override
  public void forEachSubscription(byte[] subscriptionName, byte[] channelToMatch,
      ForEachConsumer action) {
    subscriptionMap
        .forEach((client, subscription) -> action.accept(subscriptionName, channelToMatch, client,
            subscription));
  }

  @Override
  public boolean add(Client client, Subscription subscription) {
    // the client has already confirmed that
    // this is a new subscription.
    // So map.put should always return null.
    // This allows us to increment size first
    // which is needed so we can check it for
    // zero and not allow the add.
    int sizeValue;
    do {
      sizeValue = size.get();
      if (sizeValue == 0) {
        // no adds allowed after it is empty
        return false;
      }
    } while (!size.compareAndSet(sizeValue, sizeValue + 1));
    subscriptionMap.put(client, subscription);
    return true;
  }

  @Override
  public boolean remove(Client client) {
    boolean result = true;
    if (subscriptionMap.remove(client) != null) {
      result = size.decrementAndGet() > 0;
    }
    return result;
  }
}
