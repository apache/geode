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
