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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;

public class EventDistributor extends PartitionListenerAdapter {

  private static final Logger logger = LogService.getLogger();
  private final Map<RedisKey, List<EventListener>> listeners = new HashMap<>();
  private final Map<EventListener, TimerTask> timerTasks = new HashMap<>();
  private final Timer timer = new Timer("GeodeForRedisEventTimer", true);
  private int keysRegistered = 0;

  public synchronized void registerListener(EventListener listener) {
    for (RedisKey key : listener.keys()) {
      listeners.computeIfAbsent(key, k -> new ArrayList<>()).add(listener);
    }

    if (listener.getTimeout() != 0) {
      scheduleTimeout(listener, listener.getTimeout());
    }

    keysRegistered += listener.keys().size();
  }

  public synchronized void fireEvent(RedisCommandType command, RedisKey key) {
    if (listeners.isEmpty()) {
      return;
    }

    List<EventListener> listenerList = listeners.get(key);
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
  public int size() {
    return keysRegistered;
  }

  @Override
  public synchronized void afterBucketRemoved(int bucketId, Iterable<?> keys) {
    Set<EventListener> resubmittingList = new HashSet<>();
    for (Map.Entry<RedisKey, List<EventListener>> entry : listeners.entrySet()) {
      if (entry.getKey().getBucketId() == bucketId) {
        resubmittingList.addAll(entry.getValue());
      }
    }

    resubmittingList.forEach(x -> {
      x.resubmitCommand();
      removeListener(x);
    });
  }

  private synchronized void removeListener(EventListener listener) {
    boolean listenerRemoved = false;
    for (RedisKey key : listener.keys()) {
      List<EventListener> listenerList = listeners.get(key);
      listenerRemoved |= listenerList.remove(listener);
      if (listenerList.isEmpty()) {
        listeners.remove(key);
      }
    }

    if (listenerRemoved) {
      keysRegistered -= listener.keys().size();
      if (listener.getTimeout() > 0) {
        timerTasks.remove(listener).cancel();
      }
    }
  }

  private void scheduleTimeout(EventListener listener, long timeout) {
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        try {
          removeListener(listener);
        } catch (Exception e) {
          logger.warn("Unable to remove EventListener {}", listener, e);
        }
      }
    };
    timerTasks.put(listener, task);
    timer.schedule(task, timeout);
  }
}
