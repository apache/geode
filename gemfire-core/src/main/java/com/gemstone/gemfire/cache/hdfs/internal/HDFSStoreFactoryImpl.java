/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.StoreExistsException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;


/**
 * Implementation of HDFSStoreFactory 
 * 
 * @author Hemant Bhanawat
 */
public class HDFSStoreFactoryImpl extends HDFSStoreCreation {
  public static final String DEFAULT_ASYNC_QUEUE_ID_FOR_HDFS= "HDFS_QUEUE";
  
  private Cache cache;
  
  public HDFSStoreFactoryImpl(Cache cache) {
    this(cache, null);
  }
  
  public HDFSStoreFactoryImpl(Cache cache, HDFSStoreCreation config) {
    super(config);
    this.cache = cache;
  }

  @Override
  public HDFSStore create(String name) {
    if (name == null) {
      throw new GemFireConfigException("HDFS store name not provided");
    }
    
    HDFSStore result = null;
    synchronized (this) {
      if (this.cache instanceof GemFireCacheImpl) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) this.cache;
        if (gfc.findHDFSStore(name) != null) {
          throw new StoreExistsException(name);
        }
        
        HDFSStoreImpl hsi = new HDFSStoreImpl(name, this.configHolder);
        gfc.addHDFSStore(hsi);
        result = hsi;
      }
    }
    return result;
  }

  public static final String getEventQueueName(String regionPath) {
    return HDFSStoreFactoryImpl.DEFAULT_ASYNC_QUEUE_ID_FOR_HDFS + "_"
        + regionPath.replace('/', '_');
  }

  public HDFSStore getConfigView() {
    return (HDFSStore) configHolder;
  }
}
