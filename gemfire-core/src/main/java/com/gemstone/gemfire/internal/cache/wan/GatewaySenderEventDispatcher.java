/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.List;
/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 *
 */
public interface GatewaySenderEventDispatcher {
  
  public boolean dispatchBatch(List events, boolean removeFromQueueOnException, boolean isRetry);
  
  public boolean isRemoteDispatcher();
  
  public boolean isConnectedToRemote();
  
  public void stop();
}
