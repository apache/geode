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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.management.internal.FederationComponent;
import org.apache.geode.management.internal.ManagementConstants;

public class StatsAggregator {

  private Map<String, Class<?>> typeMap;

  private Map<String, AtomicReference<Number>> aggregateMap;

  private LogWriter logger;

  public StatsAggregator(Map<String, Class<?>> typeMap) {
    this.typeMap = typeMap;
    this.aggregateMap = new HashMap<String, AtomicReference<Number>>();
    this.logger = InternalDistributedSystem.getLogger();
    initAggregateMap();
  }

  /**
   * Initialize all counters to 0;
   */
  private void initAggregateMap() {
    Iterator<String> it = typeMap.keySet().iterator();
    while (it.hasNext()) {
      AtomicReference<Number> ref = null;
      String attribute = it.next();
      Class<?> classzz = typeMap.get(attribute);
      if (classzz == Long.TYPE) {
        ref = new AtomicReference<Number>(new Long(0L));
      } else if (classzz == Integer.TYPE) {
        ref = new AtomicReference<Number>(new Integer(0));
      } else if (classzz == Float.TYPE) {
        ref = new AtomicReference<Number>(new Float(0F));
      } else if (classzz == Double.TYPE) {
        ref = new AtomicReference<Number>(new Double(0D));

      }

      aggregateMap.put(attribute, ref);
    }
  }

  public void aggregate(FederationComponent newComp, FederationComponent oldComp) {

    Map<String, Object> newState = (newComp != null ? newComp.getObjectState() : null);
    Map<String, Object> oldState;

    if (oldComp != null && oldComp.getOldState().size() > 0) {
      oldState = oldComp.getOldState();
    } else {
      oldState = (oldComp != null ? oldComp.getObjectState() : null);
    }

    String attribute = null;
    try {
      if (typeMap != null && !typeMap.isEmpty()) {
        for (Map.Entry<String, Class<?>> typeEntry : typeMap.entrySet()) {
          attribute = typeEntry.getKey();

          Object newVal = newState != null ? newState.get(attribute) : null;
          if (newVal != null) {
            Object oldVal = null;

            if (oldState != null) {
              oldVal = oldState.get(attribute);
            }

            Class<?> classzz = typeEntry.getValue();
            if (classzz == Long.TYPE) {
              if (oldVal == null) {
                oldVal = new Long(0L);
              }
              incLong(attribute, (Long) newVal, (Long) oldVal);
            } else if (classzz == Integer.TYPE) {
              if (oldVal == null) {
                oldVal = new Integer(0);
              }
              incInt(attribute, (Integer) newVal, (Integer) oldVal);
            } else if (classzz == Float.TYPE) {
              if (oldVal == null) {
                oldVal = new Float(0F);
              }
              incFloat(attribute, (Float) newVal, (Float) oldVal);
            } else if (classzz == Double.TYPE) {
              if (oldVal == null) {
                oldVal = new Double(0D);
              }
              incDouble(attribute, (Double) newVal, (Double) oldVal);

            }

          } else if (oldState != null && newState == null) {
            Object oldVal = oldState.get(attribute);
            if (oldVal != null) {
              Class<?> classzz = typeEntry.getValue();
              if (classzz == Long.TYPE) {
                decLong(attribute, (Long) oldVal);
              } else if (classzz == Integer.TYPE) {
                decInt(attribute, (Integer) oldVal);
              } else if (classzz == Float.TYPE) {
                decFloat(attribute, (Float) oldVal);
              } else if (classzz == Double.TYPE) {
                decDouble(attribute, (Double) oldVal);

              }
            }

          }

        }

      }
    } catch (Exception e) {
      if (logger.fineEnabled()) {
        logger.fine("Exception while processing attribute " + attribute);
        logger.fine(e);
      }
    }

  }

  public Integer getIntValue(String attributeName) {
    int val = aggregateMap.get(attributeName).get().intValue();
    return val < 0 ? ManagementConstants.NOT_AVAILABLE_INT : val;
  }

  public Long getLongValue(String attributeName) {
    long val = aggregateMap.get(attributeName).get().longValue();
    return val < 0 ? ManagementConstants.NOT_AVAILABLE_LONG : val;
  }

  public Float getFloatValue(String attributeName) {
    float val = aggregateMap.get(attributeName).get().floatValue();
    return val < 0 ? ManagementConstants.NOT_AVAILABLE_FLOAT : val;
  }

  public Double getDoubleValue(String attributeName) {
    double val = aggregateMap.get(attributeName).get().doubleValue();
    return val < 0 ? ManagementConstants.NOT_AVAILABLE_DOUBLE : val;
  }

  public void incLong(String attributeName, Long newVal, Long oldVal) {
    if (newVal.equals(oldVal)) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    for (;;) {
      Number expectedVal = ar.get();
      Number curVal = expectedVal.longValue() + (newVal - oldVal);
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void incInt(String attributeName, Integer newVal, Integer oldVal) {
    if (newVal.equals(oldVal)) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    for (;;) {
      Number expectedVal = ar.get();
      Number curVal = expectedVal.intValue() + (newVal - oldVal);
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void incFloat(String attributeName, Float newVal, Float oldVal) {
    if (newVal.equals(oldVal)) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    for (;;) {
      Number expectedVal = ar.get();
      Number curVal = expectedVal.floatValue() + (newVal - oldVal);
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void incDouble(String attributeName, Double newVal, Double oldVal) {
    if (newVal.equals(oldVal)) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    for (;;) {
      Number expectedVal = ar.get();
      Number curVal = expectedVal.doubleValue() + (newVal - oldVal);
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void decLong(String attributeName, Long oldVal) {
    if (oldVal == 0) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    Number curVal;
    for (;;) {
      Number expectedVal = ar.get();
      if (expectedVal.longValue() != 0) {
        curVal = expectedVal.longValue() - oldVal;
      } else {
        return;
      }
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void decInt(String attributeName, Integer oldVal) {
    if (oldVal == 0) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    Number curVal;
    for (;;) {
      Number expectedVal = ar.get();
      if (expectedVal.intValue() != 0) {
        curVal = expectedVal.intValue() - oldVal;
      } else {
        return;
      }
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void decFloat(String attributeName, Float oldVal) {
    if (oldVal == 0) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    Number curVal;
    for (;;) {
      Number expectedVal = ar.get();
      if (expectedVal.floatValue() != 0) {
        curVal = expectedVal.floatValue() - oldVal;
      } else {
        return;
      }
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }

  public void decDouble(String attributeName, Double oldVal) {
    if (oldVal == 0) {
      return;
    }
    AtomicReference<Number> ar = aggregateMap.get(attributeName);
    Number curVal;
    for (;;) {
      Number expectedVal = ar.get();
      if (expectedVal.doubleValue() != 0) {
        curVal = expectedVal.doubleValue() - oldVal;
      } else {
        return;
      }
      if (ar.compareAndSet(expectedVal, curVal)) {
        return;
      }
    }
  }
}
