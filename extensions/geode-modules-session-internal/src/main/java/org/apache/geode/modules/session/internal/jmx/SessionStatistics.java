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

package org.apache.geode.modules.session.internal.jmx;

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
