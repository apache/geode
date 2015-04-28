/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.CacheCallback;

/**
 * Application plug-in interface for handling continuous query events after 
 * they occur. The listener has two methods, one that is called when there 
 * is an event satisfied by the CQ and the other called when there is an 
 * error during CQ processing. 
 *
 * @author Anil 
 * @since 5.5
 */

public interface CqListener extends CacheCallback {
  
 /**
   * This method is invoked when an event is occurred on the region
   * that satisfied the query condition of this CQ.
   * This event does not contain an error.
   * If CQ is executed using ExecuteWithInitialResults the returned
   * result may already include the changes with respect to this event. 
   * This could arise when updates are happening on the region while
   * CQ registration is in progress. The CQ does not block any region 
   * operation as it could affect the performance of region operation. 
   * Its up to the application to synchronize between the region 
   * operation and CQ registration to avoid duplicate event being 
   * delivered.   
   * 
   * @see com.gemstone.gemfire.cache.query.CqQuery#executeWithInitialResults
   */
  public void onEvent(CqEvent aCqEvent);

  /** 
   * This method is invoked when there is an error during CQ processing.  
   * The error can appear while applying query condition on the event.
   * e.g if the event doesn't has attributes as specified in the CQ query.
   * This event does contain an error. The newValue may or may not be 
   * available, and will be null if not available.
   */
  public void onError(CqEvent aCqEvent);
}
