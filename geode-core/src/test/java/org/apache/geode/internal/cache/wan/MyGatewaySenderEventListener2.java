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

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

public class MyGatewaySenderEventListener2 implements AsyncEventListener, Serializable {
  String id = "MyGatewaySenderEventListener2";

  /**
   * Creates a latency listener.
   */
  private final Map eventsMap;

  public MyGatewaySenderEventListener2() {
    this.eventsMap = new HashMap();
  }

  /**
   * Processes events by recording their latencies.
   */
  public boolean processEvents(List<AsyncEvent> events) {
    for (AsyncEvent event : events) {
      this.eventsMap.put(event.getKey(), event.getDeserializedValue());
    }
    return true;
  }

  public void close() {}

  public Map getEventsMap() {
    return this.eventsMap;
  }

  public void printMap() {
    System.out.println("Printing Map " + this.eventsMap);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MyGatewaySenderEventListener2))
      return false;
    MyGatewaySenderEventListener2 listener = (MyGatewaySenderEventListener2) obj;
    return this.id.equals(listener.id);
  }

  @Override
  public String toString() {
    return id;
  }

}
