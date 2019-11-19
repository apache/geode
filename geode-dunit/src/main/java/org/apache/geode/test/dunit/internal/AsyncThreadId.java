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
package org.apache.geode.test.dunit.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.annotations.internal.MakeNotStatic;

public class AsyncThreadId {

  private static final AtomicLong ASYNC_ID_COUNTER = new AtomicLong();

  @MakeNotStatic
  private static final Map<Long, Long> ASYNC_THREAD_ID_MAP = new ConcurrentHashMap<>();

  public static long nextId() {
    return ASYNC_ID_COUNTER.incrementAndGet();
  }

  public static void put(long asyncId, long threadId) {
    ASYNC_THREAD_ID_MAP.put(asyncId, threadId);
  }

  public static void remove(long asyncId) {
    ASYNC_THREAD_ID_MAP.remove(asyncId);
  }

  public static long get(long asyncId) {
    return ASYNC_THREAD_ID_MAP.get(asyncId);
  }
}
