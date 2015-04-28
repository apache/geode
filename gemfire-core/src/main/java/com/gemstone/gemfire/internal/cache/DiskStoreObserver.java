/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Map;

/**
 * For testing purposes only, observers events in the disk store.
 * @author dsmith
 *
 */
public abstract class DiskStoreObserver {
  
  private static DiskStoreObserver INSTANCE = null;
  
  public static void setInstance(DiskStoreObserver observer) {
    INSTANCE = observer;
  }
  
  public void beforeAsyncValueRecovery(DiskStoreImpl store) {
    
  }
  
  public void afterAsyncValueRecovery(DiskStoreImpl store) {
    
  }
  
  public void afterWriteGCRVV(DiskRegion dr) {
  }
  
  
  static void startAsyncValueRecovery(DiskStoreImpl store) {
    if(INSTANCE != null) {
      INSTANCE.beforeAsyncValueRecovery(store);
    }
  }
  
  static void endAsyncValueRecovery(DiskStoreImpl store) {
    if(INSTANCE != null) {
      INSTANCE.afterAsyncValueRecovery(store);
    }
  }
  
  public static void endWriteGCRVV(DiskRegion dr) {
    if(INSTANCE != null) {
      INSTANCE.afterWriteGCRVV(dr);
    }
  }
}
