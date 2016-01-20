/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * CqListener class, implements CqListener interface methods. 
 * Offers callback methods for the CqQuery.
 *
 * @author anil 
 * @since 5.5
 */
public class CqListenerImpl implements CqListener {
  
  /**
   * An event occurred that modifies the results of the query.
   * This event does not contain an error.
   */
  public void onEvent(CqEvent aCqEvent) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqListenerImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  }
  
  /** 
   * An error occurred in the processing of a CQ.
   * This event does contain an error. The newValue and oldValue in the
   * may or may not be available, and will be null if not available.
   */
  public void onError(CqEvent aCqEvent) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqListenerImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  }
  
  /** CQ is being closed, do any cleanup here */
  public void close() {
  }
}
