/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.util;

/**
 * Abstract class for CqListener. 
 * Utility class that implements all methods in <code>CqListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods of interest.
 *
 * @author anil 
 * @since 5.1
 */

import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqEvent;

public abstract class CqListenerAdapter implements CqListener {
  
  /**
   * An event occurred that modifies the results of the query.
   * This event does not contain an error.
   */
  public void onEvent(CqEvent aCqEvent) {
  }

  /** 
   * An error occurred in the processing of a CQ.
   * This event does contain an error. The newValue and oldValue in the
   * event may or may not be available, and will be null if not available.
   */
  public void onError(CqEvent aCqEvent) {
  }
  
  /**
  * Called when the CQ is closed, the base region is destroyed, when
  * the cache is closed, or when this listener is removed from a CqQuery
  * using a <code>CqAttributesMutator</code>.
  *
  * <p>Implementations should cleanup any external
  * resources such as database connections. Any runtime exceptions this method
  * throws will be logged.
  *
  * <p>It is possible for this method to be called multiple times on a single
  * callback instance, so implementations must be tolerant of this.
  *
  * @see com.gemstone.gemfire.cache.CacheCallback#close
  */
  public void close() {
  }
}
