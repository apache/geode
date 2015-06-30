/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina.internal;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

public class DeltaSessionStatistics {

  public static final String typeName = "SessionStatistics";

  private static final StatisticsType type;

  private static final String SESSIONS_CREATED = "sessionsCreated";
  private static final String SESSIONS_INVALIDATED= "sessionsInvalidated";
  private static final String SESSIONS_EXPIRED= "sessionsExpired";

  private static final int sessionsCreatedId;
  private static final int sessionsInvalidatedId;
  private static final int sessionsExpiredId;

  static {
    // Initialize type
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType(typeName, typeName,
      new StatisticDescriptor[] {
        f.createIntCounter(SESSIONS_CREATED, "The number of sessions created", "operations"),
        f.createIntCounter(SESSIONS_INVALIDATED, "The number of sessions invalidated by invoking invalidate", "operations"),
        f.createIntCounter(SESSIONS_EXPIRED, "The number of sessions invalidated by timeout", "operations"),
      }
    );

    // Initialize id fields
    sessionsCreatedId = type.nameToId(SESSIONS_CREATED);
    sessionsInvalidatedId = type.nameToId(SESSIONS_INVALIDATED);
    sessionsExpiredId = type.nameToId(SESSIONS_EXPIRED);
  }

  private final Statistics stats;

  public DeltaSessionStatistics(StatisticsFactory factory, String applicationName) {
    this.stats = factory.createAtomicStatistics(type, typeName + "_" + applicationName);
  }

  public void close() {
    this.stats.close();
  }

  public int getSessionsCreated() {
    return this.stats.getInt(sessionsCreatedId);
  }

  public void incSessionsCreated() {
    this.stats.incInt(sessionsCreatedId, 1);
  }

  public int getSessionsInvalidated() {
    return this.stats.getInt(sessionsInvalidatedId);
  }

  public void incSessionsInvalidated() {
    this.stats.incInt(sessionsInvalidatedId, 1);
  }

  public int getSessionsExpired() {
    return this.stats.getInt(sessionsExpiredId);
  }

  public void incSessionsExpired() {
    this.stats.incInt(sessionsExpiredId, 1);
  }
}
