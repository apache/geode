/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * This class provides methods to get aggregate statistical information 
 * about the CQs of a client.
 *
 * @since 5.5
 * @author anil
 */
public interface CqServiceStatistics {

  /**
   * Get the number of CQs currently active. 
   * Active CQs are those which are executing (in running state).
   * @return long number of CQs
   */
  public long numCqsActive();
  
  /**
   * Get the total number of CQs created. This is a cumulative number.
   * @return long number of CQs created.
   */
  public long numCqsCreated();
  
  /**
   * Get the total number of closed CQs. This is a cumulative number.
   * @return long number of CQs closed.
   */
  public long numCqsClosed();
 
  /**
   * Get the number of stopped CQs currently.
   * @return number of CQs stopped.
   */
  public long numCqsStopped();
 
  /**
   * Get number of CQs that are currently active or stopped. 
   * The CQs included in this number are either running or stopped (suspended).
   * Closed CQs are not included.
   * @return long number of CQs on client.
   */
  public long numCqsOnClient();
  
  /**
   * Get number of CQs on the given region. Active CQs and stopped CQs on this region 
   * are included and closed CQs are not included.
   * @param regionFullPath
   * @return long number of CQs on the region.
   */
  public long numCqsOnRegion(String regionFullPath);
  
}
