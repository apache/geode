/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

/**
 * @author dsmith
 *
 */
public interface ClientUpdater {

  void close();
  
  boolean isAlive();

  void join(long wait) throws InterruptedException;
  
  public void setFailedUpdater(ClientUpdater failedUpdater);
  
  public boolean isProcessing();
  
  public boolean isPrimary();
}
