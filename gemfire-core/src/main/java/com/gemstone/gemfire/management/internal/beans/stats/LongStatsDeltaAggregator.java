/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.beans.stats;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

import com.gemstone.gemfire.management.internal.FederationComponent;

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
  
  private void initializeArray(AtomicLongArray arr){
    for(int i = 0; i<arr.length() ; i++){
      arr.set(i, Long.valueOf(0));
    }
  }
}