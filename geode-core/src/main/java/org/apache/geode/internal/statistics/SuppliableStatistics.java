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

import org.apache.geode.Statistics;

public interface SuppliableStatistics extends Statistics {

  /**
   * Cast {@code Statistics} to {@code SuppliableStatistics}.
   */
  static SuppliableStatistics toSuppliableStatistics(Statistics statistics) {
    return (SuppliableStatistics) statistics;
  }

  /**
   * Invoke sample suppliers to retrieve the current value for the supplier controlled sets and
   * update the stats to reflect the supplied values.
   *
   * @return the number of callback errors that occurred while sampling stats
   */
  int updateSuppliedValues();
}
