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

package org.apache.geode.internal.statistics.meters;

/**
 * Reads and writes a stat.
 */
public interface StatisticBinding {
  /**
   * Adds the specified amount to the stat.
   */
  void add(double amount);

  /**
   * Returns the value of the stat as a {@code double}.
   */
  double doubleValue();

  /**
   * Returns the value of the stat as a {@code long}.
   */
  long longValue();

  static StatisticBinding noOp() {
    return new StatisticBinding() {
      @Override
      public void add(double amount) {}

      @Override
      public double doubleValue() {
        return 0.0;
      }

      @Override
      public long longValue() {
        return 0L;
      }
    };
  }
}
