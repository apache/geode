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
package org.apache.geode.internal.statistics;

@FunctionalInterface
public interface StatisticsClock {

  /**
   * Returns the current value of the running Java Virtual Machine's high-resolution time source,
   * in nanoseconds.
   *
   * <p>
   * See {@code java.lang.System#nanoTime()}.
   */
  long getTime();

  /**
   * Returns true if this clock is enabled. If disabled then {@code getTime()} will return zero.
   *
   * <p>
   * Default returns {@code true}.
   */
  default boolean isEnabled() {
    return true;
  }
}
