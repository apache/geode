/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;


import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;

/**
 * HDFSStoreDirector is created for managing all instances of HDFSStoreImpl.    
 *
 * @author Hemant Bhanawat
 */
public final class HDFSStoreDirector {
  private final ConcurrentHashMap<String, HDFSStoreImpl> storeMap = new ConcurrentHashMap<String, HDFSStoreImpl>();

  // singleton instance
  private static volatile HDFSStoreDirector instance;
  
  private HDFSStoreDirector() {

  }
  
  public static final HDFSStoreDirector getInstance() {
    if (instance == null) {
      synchronized (HDFSStoreDirector.class)  {
        if (instance == null)
          instance = new HDFSStoreDirector();
      }
    }
    return instance;
  }

  // Called when the region is created.
  public final void addHDFSStore(HDFSStoreImpl hdfsStore){
    this.storeMap.put(hdfsStore.getName(), hdfsStore); 
  }
  
  public final HDFSStoreImpl getHDFSStore(String hdfsStoreName) {
    return this.storeMap.get(hdfsStoreName);
  }
  
  public final void removeHDFSStore(String hdfsStoreName) {
    this.storeMap.remove(hdfsStoreName);
  } 
  
  public void closeHDFSStores() {
    Iterator<HDFSStoreImpl> it = this.storeMap.values().iterator();
    while (it.hasNext()) {
      HDFSStoreImpl hsi = it.next();
      hsi.close();
    }
    this.storeMap.clear();
  }

   public ArrayList<HDFSStoreImpl> getAllHDFSStores() {
    ArrayList<HDFSStoreImpl> hdfsStores = new ArrayList<HDFSStoreImpl>();
    hdfsStores.addAll(this.storeMap.values());
    return hdfsStores;
  }
}
