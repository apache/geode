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
package org.apache.geode.modules.session.catalina.internal;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class DeltaSessionStatistics {

  private static final String typeName = "SessionStatistics";

  private static final StatisticsType type;

  private static final String SESSIONS_CREATED = "sessionsCreated";
  private static final String SESSIONS_INVALIDATED = "sessionsInvalidated";
  private static final String SESSIONS_EXPIRED = "sessionsExpired";

  private static final int sessionsCreatedId;
  private static final int sessionsInvalidatedId;
  private static final int sessionsExpiredId;

  static {
    // Initialize type
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType(typeName, typeName,
        new StatisticDescriptor[] {
            f.createLongCounter(SESSIONS_CREATED, "The number of sessions created", "operations"),
            f.createLongCounter(SESSIONS_INVALIDATED,
                "The number of sessions invalidated by invoking invalidate", "operations"),
            f.createLongCounter(SESSIONS_EXPIRED, "The number of sessions invalidated by timeout",
                "operations"),});

    // Initialize id fields
    sessionsCreatedId = type.nameToId(SESSIONS_CREATED);
    sessionsInvalidatedId = type.nameToId(SESSIONS_INVALIDATED);
    sessionsExpiredId = type.nameToId(SESSIONS_EXPIRED);
  }

  private final Statistics stats;

  public DeltaSessionStatistics(StatisticsFactory factory, String applicationName) {
    stats = factory.createAtomicStatistics(type, typeName + "_" + applicationName);
  }

  public void close() {
    stats.close();
  }

  @SuppressWarnings("unused")
  public long getSessionsCreated() {
    return stats.getLong(sessionsCreatedId);
  }

  public void incSessionsCreated() {
    stats.incLong(sessionsCreatedId, 1);
  }

  @SuppressWarnings("unused")
  public long getSessionsInvalidated() {
    return stats.getLong(sessionsInvalidatedId);
  }

  public void incSessionsInvalidated() {
    stats.incLong(sessionsInvalidatedId, 1);
  }

  @SuppressWarnings("unused")
  public long getSessionsExpired() {
    return stats.getLong(sessionsExpiredId);
  }

  public void incSessionsExpired() {
    stats.incLong(sessionsExpiredId, 1);
  }
}
