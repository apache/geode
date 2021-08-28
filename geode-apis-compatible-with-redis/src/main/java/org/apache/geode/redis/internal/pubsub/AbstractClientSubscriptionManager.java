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
import java.util.function.Consumer;

import org.apache.geode.redis.internal.netty.Client;

abstract class AbstractClientSubscriptionManager<S extends Subscription>
    implements ClientSubscriptionManager<S> {
  private final Map<Client, S> map = new ConcurrentHashMap<>();
  /**
   * This is used instead of map.size() because we need to
   * make sure that once size goes to zero no further adds
   * will be made to this manager. Instead a new manager
   * needs to be created.
   */
  private final AtomicInteger size = new AtomicInteger(1);

  public AbstractClientSubscriptionManager(S subscription) {
    Client client = subscription.getClient();
    map.put(client, subscription);
  }

  @Override
  public int getSubscriptionCount() {
    int result = size.get();
    if (result < 0) {
      result = 0;
    }
    return result;
  }

  @Override
  public int getSubscriptionCount(String channel) {
    return size.get();
  }

  @Override
  public void forEachSubscription(String channel, Consumer<Subscription> action) {
    map.values().forEach(action);
  }

  @Override
  public boolean add(S subscription) {
    // the client has already confirmed that
    // this is a new subscription.
    // So map.put should always return null.
    // This allows us to increment size first
    // which is needed so we can check it for
    // zero and not allow the add.
    int sizeValue = size.get();
    do {
      if (sizeValue <= 0) {
        // no adds allowed after it is empty
        return false;
      }
    } while (size.compareAndSet(sizeValue, sizeValue + 1));
    Client client = subscription.getClient();
    map.put(client, subscription);
    return true;
  }

  @Override
  public boolean remove(Client client) {
    boolean result = true;
    if (map.remove(client) != null) {
      result = size.decrementAndGet() > 0;
    }
    return result;
  }
}
