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
package org.apache.geode.internal.cache.wan;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

public class MyGatewaySenderEventListener implements AsyncEventListener, Serializable {
  String id = "MyGatewaySenderEventListener";
  /**
   * Creates a latency listener.
   */
  private final Map eventsMap;

  public MyGatewaySenderEventListener() {
    eventsMap = new HashMap();
  }

  /**
   * Processes events by recording their latencies.
   */
  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    synchronized (eventsMap) {
      for (AsyncEvent event : events) {
        eventsMap.put(event.getKey(), event.getDeserializedValue());
      }
    }
    return true;
  }

  @Override
  public void close() {}

  public Map getEventsMap() {
    return eventsMap;
  }

  public void printMap() {
    System.out.println("Printing Map " + eventsMap);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MyGatewaySenderEventListener)) {
      return false;
    }
    MyGatewaySenderEventListener listener = (MyGatewaySenderEventListener) obj;
    return id.equals(listener.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }


  @Override
  public String toString() {
    return id;
  }
}
