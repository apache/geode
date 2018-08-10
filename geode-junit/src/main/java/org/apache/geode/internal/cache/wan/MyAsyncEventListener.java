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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.internal.logging.LogService;

public class MyAsyncEventListener implements AsyncEventListener, Declarable {
  private static final Logger logger = LogService.getLogger();

  private final Map eventsMap;

  public MyAsyncEventListener() {
    eventsMap = new HashMap();
  }

  @Override
  public synchronized boolean processEvents(List<AsyncEvent> events) {
    for (AsyncEvent event : events) {
      if (eventsMap.containsKey(event.getKey())) {
        logger.debug("This is a duplicate event --> {}", event.getKey());
      }
      eventsMap.put(event.getKey(), event.getDeserializedValue());

      logger.debug("Received an event --> {}", event.getKey());
    }
    return true;
  }

  public Map getEventsMap() {
    return eventsMap;
  }
}
