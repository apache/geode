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
package org.apache.geode.cache.control;

import java.util.ServiceLoader;
import java.util.function.LongConsumer;

/**
 * This interface can be implemented with a class
 * that is then configured as a java service (see {@link ServiceLoader}
 * and will then be used by geode to determine how much heap memory
 * is in use and what the maximum heap size is.
 * This interface provides the heap usage in two ways.
 * It must implement {@link #getBytesUsed()} which is used by geode to directly
 * ask this provider for the current used memory.
 * It can also provide notifications when the usage has changed by calling
 * {@link LongConsumer#accept(long)} on the listener passed to startNotifications.
 * It should only do this after startNotifications has been
 * called and should stop notifications after stopNotifications has been called.
 * Notifications are optional. If the underlying JVM does not support notifications
 * then startNotifications and stopNotifications can have empty implementations.
 */
public interface HeapUsageProvider {
  /**
   * Called by geode when this provider should start providing notifications
   * to the given listener.
   */
  void startNotifications(LongConsumer listener);

  /**
   * Called by geode when this provider should stop providing notifications.
   */
  void stopNotifications();

  /**
   * Called by geode when it wants to know what the maximum amount of heap is.
   * Geode does not expect this value to change, so it tends to call it once and
   * cache the returned value. Provider implementations must implement this method
   * to return a reasonable value.
   *
   * @return the maximum amount of heap memory in bytes
   */
  long getMaxMemory();

  /**
   * Called by geode when it needs to know how many bytes are current used of heap memory.
   * Geode will call this method periodically (by default every 500 milliseconds which can
   * be changed using the "gemfire.heapPollerInterval" system property).
   * Provider implementations must implement this method
   * to return a reasonable value.
   *
   * @return the number of heap memory bytes currently in use
   * @throws IllegalStateException if this provider is not able to determine heap usage
   */
  long getBytesUsed();

}
