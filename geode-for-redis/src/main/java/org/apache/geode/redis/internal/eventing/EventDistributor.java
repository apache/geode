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
 */

package org.apache.geode.redis.internal.eventing;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.internal.lang.utils.JavaWorkarounds;
import org.apache.geode.logging.internal.executors.LoggingThreadFactory;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;

public class EventDistributor extends PartitionListenerAdapter {

  private final Map<RedisKey, Queue<EventListener>> listeners = new ConcurrentHashMap<>();

  private final ScheduledThreadPoolExecutor timerExecutor =
      new ScheduledThreadPoolExecutor(1,
          new LoggingThreadFactory("GeodeForRedisEventTimer-", true));

  public EventDistributor() {
    timerExecutor.setRemoveOnCancelPolicy(true);
  }

  public synchronized void registerListener(EventListener listener) {
    for (RedisKey key : listener.keys()) {
      JavaWorkarounds.computeIfAbsent(listeners, key, k -> new LinkedBlockingQueue<>())
          .add(listener);
    }

    listener.scheduleTimeout(timerExecutor, this);
  }

  public void fireEvent(RedisCommandType command, RedisKey key) {
    Queue<EventListener> listenerList = listeners.get(key);
    if (listenerList == null) {
      return;
    }

    for (EventListener listener : listenerList) {
      if (listener.process(command, key) == EventResponse.REMOVE_AND_STOP) {
        removeListener(listener);
        break;
      }
    }
  }

  /**
   * The total number of keys registered by all listeners (includes duplicates).
   */
  @VisibleForTesting
  public int getRegisteredKeys() {
    return listeners.values().stream().mapToInt(Collection::size).sum();
  }

  @Override
  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
    Set<EventListener> resubmittingList = new HashSet<>();
    for (Map.Entry<RedisKey, Queue<EventListener>> entry : listeners.entrySet()) {
      if (entry.getKey().getBucketId() == bucketId) {
        resubmittingList.addAll(entry.getValue());
      }
    }

    resubmittingList.forEach(x -> {
      removeListener(x);
      x.resubmitCommand();
    });
  }

  public synchronized void removeListener(EventListener listener) {
    for (RedisKey key : listener.keys()) {
      Queue<EventListener> listenerList = listeners.get(key);
      if (listenerList == null) {
        continue;
      }
      listenerList.remove(listener);
      if (listenerList.isEmpty()) {
        listeners.remove(key);
      }
    }
  }

}
