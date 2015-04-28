/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * This class provides methods to get statistical information about a registered Continuous Query (CQ)
 * represented by the CqQuery object. 
 * 
 * @since 5.5
 * @author Anil
 */
public interface CqStatistics {

  /**
   * Get number of Insert events qualified by this CQ.
   * @return long number of inserts.
   */
  public long numInserts();
  
  /**
   * Get number of Delete events qualified by this CQ.
   * @return long number of deletes.
   */
  public long numDeletes();
  
  /**
   * Get number of Update events qualified by this CQ.
   * @return long number of updates.
   */
  public long numUpdates();

  /**
   * Get total of all the events qualified by this CQ.
   * @return long total number of events.
   */
  public long numEvents();

}
