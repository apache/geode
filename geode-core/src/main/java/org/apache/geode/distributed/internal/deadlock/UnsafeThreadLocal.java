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
package org.apache.geode.distributed.internal.deadlock;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * A ThreadLocal implementation that allows reading values from arbitrary threads, useful for
 * deadlock detection. This implementation uses a WeakHashMap to track values per thread without
 * requiring reflection or JVM internal access.
 *
 * <p>
 * Unlike standard ThreadLocal, this class maintains an additional mapping that allows querying the
 * value for any thread, not just the current thread. This is useful for deadlock detection where
 * we need to inspect what resources other threads are holding.
 * </p>
 *
 * <p>
 * The implementation uses WeakHashMap with Thread keys to ensure threads can be garbage collected
 * when they terminate, preventing memory leaks.
 * </p>
 */
public class UnsafeThreadLocal<T> extends ThreadLocal<T> {
  /**
   * Maps threads to their values. Uses WeakHashMap so terminated threads can be GC'd. Synchronized
   * to ensure thread-safe access.
   */
  private final Map<Thread, T> threadValues =
      java.util.Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Sets the value for the current thread and records it in the cross-thread map.
   */
  @Override
  public void set(T value) {
    super.set(value);
    if (value != null) {
      threadValues.put(Thread.currentThread(), value);
    } else {
      threadValues.remove(Thread.currentThread());
    }
  }

  /**
   * Removes the value for the current thread from both the ThreadLocal and the cross-thread map.
   */
  @Override
  public void remove() {
    super.remove();
    threadValues.remove(Thread.currentThread());
  }

  /**
   * Gets the value for an arbitrary thread, useful for deadlock detection.
   *
   * <p>
   * Unlike get(), this method does not set the initial value if none is found. Returns null if the
   * specified thread has no value set.
   * </p>
   *
   * @param thread the thread whose value to retrieve
   * @return the value for the specified thread, or null if none exists
   */
  public T get(Thread thread) {
    return threadValues.get(thread);
  }

}
