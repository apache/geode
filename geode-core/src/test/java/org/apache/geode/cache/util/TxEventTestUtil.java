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
package org.apache.geode.cache.util;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.cache.CacheEvent;

/**
 * Utilty class for getting Transaction events for create, invalidate, put, destroy operations.
 */
public class TxEventTestUtil {

  /**
   * Retrieves the cache events with operation create
   *
   * @return list of cache events
   */
  public static List getCreateEvents(List<CacheEvent<?, ?>> events) {
    if (events != null && !events.isEmpty()) {
      List<CacheEvent<?, ?>> result =
          events.stream().filter(ce -> ce.getOperation().isCreate()).collect(Collectors.toList());
      return result;
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  /**
   * Retrieves the cache events with operation update
   *
   * @return list of cache events
   */
  public static List getPutEvents(List<CacheEvent<?, ?>> events) {
    if (events != null && !events.isEmpty()) {
      List<CacheEvent<?, ?>> result =
          events.stream().filter(ce -> ce.getOperation().isUpdate()).collect(Collectors.toList());
      return result;
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  /**
   * Retrieves the cache events with operation invalidate
   *
   * @return list of cache events
   */
  public static List getInvalidateEvents(List<CacheEvent<?, ?>> events) {
    if (events != null && !events.isEmpty()) {
      List<CacheEvent<?, ?>> result = events.stream().filter(ce -> ce.getOperation().isInvalidate())
          .collect(Collectors.toList());
      return result;
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  /**
   * Retrieves the cache events with operation destroy
   *
   * @return list of cache events
   */
  public static List getDestroyEvents(List<CacheEvent<?, ?>> events) {
    if (events != null && !events.isEmpty()) {
      List<CacheEvent<?, ?>> result =
          events.stream().filter(ce -> ce.getOperation().isDestroy()).collect(Collectors.toList());
      return result;
    } else {
      return Collections.EMPTY_LIST;
    }
  }

}
