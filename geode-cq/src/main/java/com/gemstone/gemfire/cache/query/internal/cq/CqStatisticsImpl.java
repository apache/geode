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
package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.query.CqStatistics;

/**
 * Provides statistical information about a CqQuery.
 * 
 * @since 5.5
 * @author Rao Madduri
 */
public class CqStatisticsImpl implements CqStatistics {
  private CqQueryImpl cqQuery;
  
//  private long numInserts;
//  private long numDeletes;
//  private long numUpdates;
//  private long numEvents;
  
  /**
   * Constructor for CqStatisticsImpl
   * @param cq - CqQuery reference to the CqQueryImpl object
   */
  public CqStatisticsImpl(CqQueryImpl cq) {
    cqQuery = cq;
  }
  
  /**
   * Returns the number of Insert events for this CQ.
   * @return the number of insert events
   */
  public long numInserts() {
    return this.cqQuery.getVsdStats().getNumInserts();
  }
  
  /**
   * Returns number of Delete events for this CQ.
   * @return the number of delete events
   */
  public long numDeletes() {
    return this.cqQuery.getVsdStats().getNumDeletes();
  }
  
  /**
   * Returns number of Update events for this CQ.
   * @return the number of update events
   */
  public long numUpdates(){
    return this.cqQuery.getVsdStats().getNumUpdates();
  }
  
  /**
   * Returns the total number of events for this CQ.
   * @return the total number of insert, update, and delete events
   */
  public long numEvents(){
    return cqQuery.getVsdStats().getNumEvents();
  }
  
}
