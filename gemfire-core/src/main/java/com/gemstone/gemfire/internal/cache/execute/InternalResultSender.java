/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.ResultSender;

/**
 * 
 * @author ymahajan
 *
 */
public interface InternalResultSender extends ResultSender<Object> {

  public void enableOrderedResultStreming(boolean enable);

  public boolean isLocallyExecuted();

  public boolean isLastResultReceived();
  
  public void setException(Throwable t);
}
