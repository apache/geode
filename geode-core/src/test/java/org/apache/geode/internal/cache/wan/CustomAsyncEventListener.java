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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

public class CustomAsyncEventListener implements AsyncEventListener {

  private Map<Long, AsyncEvent> eventsMap;
  private boolean exceptionThrown = false;

  public CustomAsyncEventListener() {
    this.eventsMap = new HashMap<Long, AsyncEvent>();
  }

  public boolean processEvents(List<AsyncEvent> events) {
    int i = 0;
    for (AsyncEvent event : events) {
      i++;
      if (!exceptionThrown && i == 40) {
        i = 0;
        exceptionThrown = true;
        throw new Error("TestError");
      }
      if (exceptionThrown) {
        eventsMap.put((Long) event.getKey(), event);
      }
    }
    return true;
  }

  public Map<Long, AsyncEvent> getEventsMap() {
    return eventsMap;
  }

  public void close() {}
}
