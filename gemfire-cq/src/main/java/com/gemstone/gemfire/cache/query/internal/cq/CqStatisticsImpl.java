/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
