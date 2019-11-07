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
package org.apache.geode.internal.cache.partitioned.colocation;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.cache.PartitionedRegion;

class SingleThreadColocationLoggerFactory implements ColocationLoggerFactory {

  @VisibleForTesting
  static final String LOG_INTERVAL_PROPERTY =
      "geode.SingleThreadColocationLoggerFactory.LOG_INTERVAL_MILLIS";

  /**
   * Sleep period (milliseconds) between posting log entries.
   */
  @VisibleForTesting
  static final long DEFAULT_LOG_INTERVAL = 30_000;

  private final long logIntervalMillis;
  private final SingleThreadColocationLoggerConstructor constructor;

  SingleThreadColocationLoggerFactory() {
    this(SingleThreadColocationLogger::new);
  }

  @VisibleForTesting
  SingleThreadColocationLoggerFactory(SingleThreadColocationLoggerConstructor constructor) {
    this(Long.getLong(LOG_INTERVAL_PROPERTY, DEFAULT_LOG_INTERVAL), constructor);
  }

  private SingleThreadColocationLoggerFactory(long logIntervalMillis,
      SingleThreadColocationLoggerConstructor constructor) {
    this.logIntervalMillis = logIntervalMillis;
    this.constructor = constructor;
  }

  @Override
  public ColocationLogger startColocationLogger(PartitionedRegion region) {
    return constructor.create(region, logIntervalMillis / 2, logIntervalMillis).start();
  }
}
