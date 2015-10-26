/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

public class CompositeStats{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final String connectionStatsType; // shouldn't change
  private long connectionsOpened;
  private long connectionsClosed;
  private long connectionsAttempted;
  private long connectionsFailed;
  private long connectionLifeTime; // is this total TTL??

  @ConstructorProperties(value = { "connectionStatsType", "connectionsOpened", "connectionsClosed", "connectionsAttempted", "connectionsFailed", "connectionLifeTime" })
  public CompositeStats(String connectionStatsType,
      long connectionsOpen, long connectionsClosed,
      long connectionsAttempts, long connectionsFailures,
      long connectionLifeTime) {
    this.connectionStatsType  = connectionStatsType;
    this.connectionsOpened    = connectionsOpen;
    this.connectionsClosed    = connectionsClosed;
    this.connectionsAttempted = connectionsAttempts;
    this.connectionsFailed    = connectionsFailures;
    this.connectionLifeTime   = connectionLifeTime;
  }

  /**
   * @return the connectionStatsType
   */
  public String getConnectionStatsType() {
    return connectionStatsType;
  }

  /**
   * @return the connectionsOpened
   */
  public long getConnectionsOpened() {
    return connectionsOpened;
  }

  /**
   * @return the connectionsClosed
   */
  public long getConnectionsClosed() {
    return connectionsClosed;
  }

  /**
   * @return the connectionsAttempted
   */
  public long getConnectionsAttempted() {
    return connectionsAttempted;
  }

  /**
   * @return the connectionsFailed
   */
  public long getConnectionsFailed() {
    return connectionsFailed;
  }

  /**
   * @return the connectionLifeTime
   */
  public long getConnectionLifeTime() {
    return connectionLifeTime;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(CompositeStats.class.getSimpleName());
    builder.append("[connectionStatsType=").append(connectionStatsType);
    builder.append(", connectionsOpened=").append(connectionsOpened);
    builder.append(", connectionsClosed=").append(connectionsClosed);
    builder.append(", connectionsAttempted=").append(connectionsAttempted);
    builder.append(", connectionsFailed=").append(connectionsFailed);
    builder.append(", connectionsTTL=").append(connectionLifeTime);
    builder.append("]");
    return builder.toString();
  }

  
}
