/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.query.CqServiceStatistics;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Provides statistical information about CqService.
 *
 * @since 5.5
 * @author anil
 */
public class CqServiceStatisticsImpl implements CqServiceStatistics {
  private CqServiceImpl cqService;
//  private long activeCqs;
//  private long stoppedCqs;
//  private long closedCqs;
//  private long createdCqs;
  
  /**
   * Constructor for CqStatisticsImpl
   * @param cqs - CqService 
   */
  public CqServiceStatisticsImpl(CqServiceImpl cqs) {
    cqService = cqs;
  }
  
  /**
   * Returns the number of CQs currently executing
   */
  public long numCqsActive(){
    return this.cqService.getCqServiceVsdStats().getNumCqsActive();
  }
  
  /**
   * Returns number of CQs created.
   * @return long number of cqs created.
   */
  public long numCqsCreated(){
    return this.cqService.getCqServiceVsdStats().getNumCqsCreated();
  }
  
  /**
   * Returns number of Cqs that are closed.
   */
  public long numCqsClosed(){
    return this.cqService.getCqServiceVsdStats().getNumCqsClosed();
  }
  
  /**
   * Returns number of Cqs that are stopped.
   */
  public long numCqsStopped(){
    return this.cqService.getCqServiceVsdStats().getNumCqsStopped();
  }
  
  /**
   * Returns number of CQs created from the client.
   */
  public long numCqsOnClient(){
    return this.cqService.getCqServiceVsdStats().getNumCqsOnClient();
  }
  
  /**
   * Returns the number of CQs (active + suspended) on the given region.
   * @param regionName
   */
  public long numCqsOnRegion(String regionName){
    
    DefaultQueryService queryService = (DefaultQueryService)((GemFireCacheImpl)CacheFactory.getAnyInstance()).getLocalQueryService();
    try {
      CqQuery[] cqs = queryService.getCqs(regionName);
      
      if (cqs != null) {
        return cqs.length;
      }
    } catch(Exception ex) {
      // Dont do anything.
    }
    return 0;
  } 
}
