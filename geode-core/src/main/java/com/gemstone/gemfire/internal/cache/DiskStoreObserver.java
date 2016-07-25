/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Map;

/**
 * For testing purposes only, observers events in the disk store.
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
