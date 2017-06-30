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
package org.apache.geode.internal.cache.event;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;

public class EventTrackerExpiryTask extends SystemTimer.SystemTimerTask {

  private final long lifetimeInMillis;
  private final List<EventTracker> trackers = new LinkedList<>();
  private final boolean traceEnabled = logger.isTraceEnabled();

  public EventTrackerExpiryTask(long lifetimeInMillis) {
    this.lifetimeInMillis = lifetimeInMillis;
  }

  void addTracker(EventTracker tracker) {
    synchronized (trackers) {
      trackers.add(tracker);
    }
  }

  void removeTracker(EventTracker tracker) {
    synchronized (trackers) {
      trackers.remove(tracker);
    }
  }

  int getNumberOfTrackers() {
    return trackers.size();
  }

  @Override
  public void run2() {
    long now = System.currentTimeMillis();
    long expirationTime = now - lifetimeInMillis;
    synchronized (trackers) {
      for (EventTracker tracker : trackers) {
        if (traceEnabled) {
          logger.trace("{} sweeper: starting", tracker.getName());
        }
        removeExpiredSequenceTracker(tracker, now, expirationTime);
        removeExpiredBulkOperations(tracker, now, expirationTime);

        if (traceEnabled) {
          logger.trace("{} sweeper: done", tracker.getName());
        }
      }
    }
  }

  private void removeExpiredSequenceTracker(EventTracker tracker, long now, long expirationTime) {
    for (Iterator<Map.Entry<ThreadIdentifier, EventSequenceNumberHolder>> entryIterator =
        tracker.getRecordedEvents().entrySet().iterator(); entryIterator.hasNext();) {
      Map.Entry<ThreadIdentifier, EventSequenceNumberHolder> entry = entryIterator.next();
      EventSequenceNumberHolder evh = entry.getValue();
      if (evh.expire(now, expirationTime)) {
        if (traceEnabled) {
          logger.trace("{} sweeper: removing {}", tracker.getName(), entry.getKey());
        }
        entryIterator.remove();
      }
    }
  }

  private void removeExpiredBulkOperations(EventTracker tracker, long now, long expirationTime) {
    for (Iterator<Map.Entry<ThreadIdentifier, BulkOperationHolder>> entryIterator =
        tracker.getRecordedBulkOpVersionTags().entrySet().iterator(); entryIterator.hasNext();) {
      Map.Entry<ThreadIdentifier, BulkOperationHolder> entry = entryIterator.next();
      BulkOperationHolder evh = entry.getValue();
      if (evh.expire(now, expirationTime)) {
        if (traceEnabled) {
          logger.trace("{} sweeper: removing bulkOp {}", tracker.getName(), entry.getKey());
        }
        entryIterator.remove();
      }
    }
  }
}
