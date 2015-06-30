/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.jmx;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to manage session statistics
 */
public class SessionStatistics implements SessionStatisticsMXBean {

  private AtomicInteger activeSessions = new AtomicInteger(0);

  private AtomicInteger totalSessions = new AtomicInteger(0);

  private AtomicLong regionUpdates = new AtomicLong(0);

  @Override
  public int getActiveSessions() {
    return activeSessions.get();
  }

  @Override
  public int getTotalSessions() {
    return totalSessions.get();
  }

  @Override
  public long getRegionUpdates() {
    return regionUpdates.get();
  }

  public void setActiveSessions(int sessions) {
    activeSessions.set(sessions);
  }

  public void setTotalSessions(int sessions) {
    totalSessions.set(sessions);
  }

  public void incActiveSessions() {
    activeSessions.incrementAndGet();
    totalSessions.incrementAndGet();
  }

  public void decActiveSessions() {
    activeSessions.decrementAndGet();
  }

  public void incTotalSessions() {
    totalSessions.incrementAndGet();
  }

  public void decTotalSessions() {
    totalSessions.decrementAndGet();
  }

  public void incRegionUpdates() {
    regionUpdates.incrementAndGet();
  }

}
