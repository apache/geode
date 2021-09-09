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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;

abstract class AbstractSubscriptionManager<S extends Subscription>
    implements SubscriptionManager<S> {
  protected final Map<SubscriptionId, ClientSubscriptionManager<S>> clientManagers =
      new ConcurrentHashMap<>();

  protected ClientSubscriptionManager<S> getClientManager(byte[] channelOrPattern) {
    SubscriptionId subscriptionId = new SubscriptionId(channelOrPattern);
    return clientManagers.getOrDefault(subscriptionId, emptyClientManager());
  }

  @Override
  public List<byte[]> getIds() {
    final ArrayList<byte[]> result = new ArrayList<>(clientManagers.size());
    for (SubscriptionId key : clientManagers.keySet()) {
      result.add(key.getSubscriptionIdBytes());
    }
    return result;
  }

  @Override
  public List<byte[]> getIds(byte[] pattern) {
    final Pattern globPattern = GlobPattern.createPattern(pattern);
    final ArrayList<byte[]> result = new ArrayList<>();
    for (SubscriptionId key : clientManagers.keySet()) {
      byte[] idBytes = key.getSubscriptionIdBytes();
      if (GlobPattern.matches(globPattern, idBytes)) {
        result.add(idBytes);
      }
    }
    return result;
  }

  @Override
  public int getSubscriptionCount() {
    int sum = 0;
    for (ClientSubscriptionManager<S> manager : clientManagers.values()) {
      sum += manager.getSubscriptionCount();
    }
    return sum;
  }

  protected abstract boolean addToClient(Client client, byte[] channelOrPattern);

  protected abstract S createSubscription(byte[] channelOrPattern, Client client);

  @Override
  public S add(byte[] channelOrPattern, Client client) {
    if (!addToClient(client, channelOrPattern)) {
      return null;
    }
    final S subscription = createSubscription(channelOrPattern, client);
    final SubscriptionId subscriptionId = new SubscriptionId(channelOrPattern);
    ClientSubscriptionManager<S> newManager = null;
    ClientSubscriptionManager<S> existingManager = clientManagers.get(subscriptionId);
    while (true) {
      if (existingManager == null) {
        if (newManager == null) {
          // newManager is lazily created so that it will not be created
          // at all if existingManager found, and so it will only be
          // created once if we try multiple times.
          // Note that newManager is initialized to contain subscription.
          newManager = createClientManager(subscription);
        }
        existingManager = clientManagers.putIfAbsent(subscriptionId, newManager);
        if (existingManager == null) {
          // newManager was added to map so all done
          return subscription;
        }
      }
      if (existingManager.add(subscription)) {
        // subscription added to existingManager so all done
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
    SubscriptionId subscriptionId = new SubscriptionId(channelOrPattern);
    ClientSubscriptionManager<S> manager = clientManagers.get(subscriptionId);
    if (manager == null) {
      return;
    }
    if (!manager.remove(client)) {
      clientManagers.remove(subscriptionId, manager);
    }
  }


  protected abstract ClientSubscriptionManager<S> emptyClientManager();

  protected abstract ClientSubscriptionManager<S> createClientManager(S subscription);

  /**
   * Wraps a id (channel or pattern) so it can be used as a key on a hash map
   */
  private static class SubscriptionId {
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
