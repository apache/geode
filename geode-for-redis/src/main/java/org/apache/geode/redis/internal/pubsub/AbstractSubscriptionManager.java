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

import static java.util.Collections.emptyList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.statistics.RedisStats;

abstract class AbstractSubscriptionManager implements SubscriptionManager {
  protected final Map<SubscriptionId, ClientSubscriptionManager> clientManagers =
      new ConcurrentHashMap<>();
  protected final RedisStats redisStats;

  public AbstractSubscriptionManager(RedisStats redisStats) {
    this.redisStats = redisStats;
  }

  protected ClientSubscriptionManager getClientManager(byte[] channelOrPattern) {
    if (isEmpty()) {
      return emptyClientManager();
    }
    SubscriptionId subscriptionId = new SubscriptionId(channelOrPattern);
    return clientManagers.getOrDefault(subscriptionId, emptyClientManager());
  }

  @Override
  public List<byte[]> getIds() {
    if (isEmpty()) {
      return emptyList();
    }
    final ArrayList<byte[]> result = new ArrayList<>(clientManagers.size());
    for (SubscriptionId key : clientManagers.keySet()) {
      result.add(key.getSubscriptionIdBytes());
    }
    return result;
  }

  @Override
  public List<byte[]> getIds(byte[] pattern) {
    if (isEmpty()) {
      return emptyList();
    }
    final GlobPattern globPattern = new GlobPattern(pattern);
    final ArrayList<byte[]> result = new ArrayList<>();
    for (SubscriptionId key : clientManagers.keySet()) {
      byte[] idBytes = key.getSubscriptionIdBytes();
      if (globPattern.matches(idBytes)) {
        result.add(idBytes);
      }
    }
    return result;
  }

  protected boolean isEmpty() {
    return clientManagers.isEmpty();
  }

  @Override
  public int getSubscriptionCount() {
    int sum = 0;
    for (ClientSubscriptionManager manager : clientManagers.values()) {
      sum += manager.getSubscriptionCount();
    }
    return sum;
  }

  public int getUniqueSubscriptionCount() {
    Set<ByteBuffer> uniques = new HashSet<>();
    for (ClientSubscriptionManager manager : clientManagers.values()) {
      for (Subscription s : manager.getSubscriptions()) {
        for (byte[] bytes : s.getClient().getPatternSubscriptions()) {
          uniques.add(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
        }
      }
    }
    return uniques.size();
  }

  @Override
  public Subscription add(byte[] channelOrPattern, Client client) {
    if (!addToClient(client, channelOrPattern)) {
      return null;
    }
    final Subscription subscription = new SubscriptionImpl(client);
    final SubscriptionId subscriptionId = new SubscriptionId(channelOrPattern);
    ClientSubscriptionManager newManager = null;
    ClientSubscriptionManager existingManager = clientManagers.get(subscriptionId);
    while (true) {
      if (existingManager == null) {
        if (newManager == null) {
          // newManager is lazily created so that it will not be created
          // at all if existingManager found, and so it will only be
          // created once if we try multiple times.
          // Note that newManager is initialized to contain subscription.
          newManager = createClientManager(client, subscription);
        }
        existingManager = clientManagers.putIfAbsent(subscriptionId, newManager);
        if (existingManager == null) {
          // newManager was added to map so all done
          subscriptionAdded(subscriptionId);
          redisStats.changeSubscribers(1);
          return subscription;
        }
      }
      if (existingManager.add(client, subscription)) {
        // subscription added to existingManager so all done
        redisStats.changeSubscribers(1);
        return subscription;
      }
      // existingManager is empty so remove it
      // and try to add newManager again.
      clientManagers.remove(subscriptionId, existingManager);
      existingManager = null;
    }
  }

  @Override
  public void remove(byte[] channelOrPattern, Client client) {
    if (isEmpty()) {
      return;
    }
    SubscriptionId subscriptionId = new SubscriptionId(channelOrPattern);
    ClientSubscriptionManager manager = clientManagers.get(subscriptionId);
    if (manager == null) {
      return;
    }
    if (manager.remove(client)) {
      redisStats.changeSubscribers(-1);
      if (manager.getSubscriptionCount() == 0) {
        if (clientManagers.remove(subscriptionId, manager)) {
          subscriptionRemoved(subscriptionId);
        }
      }
    }
  }

  @Immutable
  private static final ClientSubscriptionManager EMPTY_CLIENT_MANAGER =
      new ClientSubscriptionManager() {
        @Override
        public int getSubscriptionCount() {
          return 0;
        }

        @Override
        public boolean add(Client client, Subscription subscription) {
          return true;
        }

        @Override
        public boolean remove(Client client) {
          return false;
        }

        @Override
        public Collection<Subscription> getSubscriptions() {
          return emptyList();
        }
      };

  private ClientSubscriptionManager emptyClientManager() {
    return EMPTY_CLIENT_MANAGER;
  }

  private ClientSubscriptionManager createClientManager(Client client,
      Subscription subscription) {
    return new ClientSubscriptionManagerImpl(client, subscription);
  }

  protected abstract boolean addToClient(Client client, byte[] channelOrPattern);

  /**
   * This method will be called when a subscription is added to this manager
   * and it was not in already in the manager.
   */
  protected abstract void subscriptionAdded(SubscriptionId id);

  /**
   * This method will be called when a subscription is removed from this manager
   */
  protected abstract void subscriptionRemoved(SubscriptionId id);

  /**
   * Wraps a id (channel or pattern) so it can be used as a key on a hash map
   */
  static class SubscriptionId {
    private final int hashCode;
    private final byte[] bytes;

    public SubscriptionId(byte[] idBytes) {
      bytes = idBytes;
      hashCode = Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubscriptionId that = (SubscriptionId) o;
      return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return "SubscriptionId{" +
          "bytes=" + Arrays.toString(bytes) +
          '}';
    }

    public byte[] getSubscriptionIdBytes() {
      return bytes;
    }
  }
}
