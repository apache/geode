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
package org.apache.geode.management.internal.beans.stats;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.geode.management.internal.FederationComponent;

public class LongStatsDeltaAggregator {

  private AtomicLongArray prevCounters;

  private AtomicLongArray currCounters;

  private List<String> keys;

  public LongStatsDeltaAggregator(List<String> keys) {
    this.keys = keys;
    prevCounters = new AtomicLongArray(keys.size());
    currCounters = new AtomicLongArray(keys.size());
    initializeArray(currCounters);
  }

  public void aggregate(FederationComponent newState, FederationComponent oldState) {
    incData(newState, oldState);// Only increase the value. No need to decrease
                                // when a
    // member goes away.
  }

  private void incData(FederationComponent newComp, FederationComponent oldComp) {

    Map<String, Object> newState = (newComp != null ? newComp.getObjectState() : null);
    Map<String, Object> oldState;

    if (oldComp != null && oldComp.getOldState().size() > 0) {
      oldState = oldComp.getOldState();
    } else {
      oldState = (oldComp != null ? oldComp.getObjectState() : null);
    }

    if (newState != null) {
      for (int index = 0; index < keys.size(); index++) {
        prevCounters.set(index, currCounters.get(index));
        Long newVal = (Long) newState.get(keys.get(index));

        if (newVal == null) {
          continue;
        }

        Long oldVal = 0L;
        if (oldState != null) {
          Object val = oldState.get(keys.get(index));
          if (val != null) {
            oldVal = (Long) val;
          }

        }
        currCounters.addAndGet(index, newVal - oldVal);
      }
    }
  }

  public long getDelta(String key) {
    int index = keys.indexOf(key);
    if (index == -1) {
      return 0;
    }
    return currCounters.get(keys.indexOf(key)) - prevCounters.get(keys.indexOf(key));
  }

  private void initializeArray(AtomicLongArray arr) {
    for (int i = 0; i < arr.length(); i++) {
      arr.set(i, Long.valueOf(0));
    }
  }
}
