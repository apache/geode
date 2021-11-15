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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscriptions.PatternSubscriptions;
import org.apache.geode.redis.internal.statistics.RedisStats;

class PatternSubscriptionManager
    extends AbstractSubscriptionManager {

  /**
   * The key is always a channel name. This cache
   * is used to optimize publish by caching the
   * PatternSubscriptions for a publish on a given channel.
   * When this manager detects that a new pattern is added to it
   * or that a pattern no longer has any subscriptions, then it
   * will clear this cache and the next publish will need to
   * recompute an entry for its channel.
   */
  private final Map<SubscriptionId, List<PatternSubscriptions>> patternSubscriptionCache =
      new ConcurrentHashMap<>();

  public PatternSubscriptionManager(RedisStats redisStats) {
    super(redisStats);
  }

  @Override
  public int getSubscriptionCount(byte[] channel) {
    int result = 0;
    for (PatternSubscriptions patternSubscriptions : getPatternSubscriptions(channel)) {
      result += patternSubscriptions.getSubscriptions().size();
    }
    return result;
  }

  private boolean matches(SubscriptionId id, byte[] channel) {
    return GlobPattern.matches(id.getSubscriptionIdBytes(), channel);
  }

  @Override
  protected boolean addToClient(Client client, byte[] pattern) {
    return client.addPatternSubscription(pattern);
  }

  @Override
  public void remove(Client client) {
    client.getPatternSubscriptions().forEach(
        channel -> remove(channel, client));
  }

  public List<PatternSubscriptions> getPatternSubscriptions(byte[] channel) {
    if (isEmpty()) {
      return emptyList();
    }
    SubscriptionId channelId = new SubscriptionId(channel);
    List<PatternSubscriptions> result = patternSubscriptionCache.get(channelId);
    if (result == null) {
      result = createPatternSubscriptions(channelId);
      if (!result.isEmpty()) {
        patternSubscriptionCache.put(channelId, result);
      }
    }
    return result;
  }

  private List<PatternSubscriptions> createPatternSubscriptions(SubscriptionId channelId) {
    List<PatternSubscriptions> result = null;
    for (Map.Entry<SubscriptionId, ClientSubscriptionManager> entry : clientManagers.entrySet()) {
      if (matches(entry.getKey(), channelId.getSubscriptionIdBytes())) {
        if (result == null) {
          result = new ArrayList<>();
        }
        result.add(new PatternSubscriptions(entry.getKey().getSubscriptionIdBytes(),
            entry.getValue().getSubscriptions()));
      }
    }
    // since we are going to cache result make its size perfect
    if (result == null) {
      result = emptyList();
    } else if (result.size() == 1) {
      result = singletonList(result.get(0));
    } else {
      result = asList(result.toArray(new PatternSubscriptions[0]));
    }
    return result;
  }

  private void clearPatternSubscriptionCache() {
    patternSubscriptionCache.clear();
  }

  @Override
  protected void subscriptionAdded(SubscriptionId id) {
    clearPatternSubscriptionCache();
    redisStats.changeUniquePatternSubscriptions(1);
  }

  @Override
  protected void subscriptionRemoved(SubscriptionId id) {
    clearPatternSubscriptionCache();
    redisStats.changeUniquePatternSubscriptions(-1);
  }

  @VisibleForTesting
  int cacheSize() {
    return patternSubscriptionCache.size();
  }
}
