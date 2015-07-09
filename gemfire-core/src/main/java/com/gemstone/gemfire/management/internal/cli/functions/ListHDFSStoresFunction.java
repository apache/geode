/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Function used by the 'list hdfs-stores' gfsh command to determine all the
 * Hdfs stores that exist for the entire cache, distributed across the GemFire distributed system.
 * on each member.
 * 
 * @author Namrata Thanvi
 */

public class ListHDFSStoresFunction extends FunctionAdapter implements InternalEntity {

  private static final long serialVersionUID = 1L;

  private static final String ID = ListHDFSStoresFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }
  
  protected DistributedMember getDistributedMemberId(Cache cache){
    return ((InternalCache)cache).getMyId();
  }
  
  public void execute(final FunctionContext context) {
    Set<HdfsStoreDetails>  hdfsStores = new HashSet<HdfsStoreDetails>();
    try {
      final Cache cache = getCache();     
      if (cache instanceof GemFireCacheImpl) {    
        final GemFireCacheImpl gemfireCache = (GemFireCacheImpl)cache;
        final DistributedMember member = getDistributedMemberId(cache);        
        for (final HDFSStore store : gemfireCache.getHDFSStores()) {  
          hdfsStores.add(new HdfsStoreDetails (store.getName() , member.getId() , member.getName()));      
        }             
      }
      context.getResultSender().lastResult(hdfsStores);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  } 
  
  @Override
  public String getId() {
    return ID;
  }

  
  public static class HdfsStoreDetails implements Serializable {
    private static final long serialVersionUID = 1L;
    private String storeName;
    private String memberId, memberName;
    
    public HdfsStoreDetails(String storeName, String memberId, String memberName) {
      super();
      this.storeName = storeName;
      this.memberId = memberId;
      this.memberName = memberName;
    }
    
    public String getStoreName() {
      return storeName;
    }
   
    public String getMemberId() {
      return memberId;
    }
   
    public String getMemberName() {
      return memberName;
    }

}
}


