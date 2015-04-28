package com.gemstone.gemfire.internal.cache.execute.util;

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
* The FindRestEnabledServersFunction class is a gemfire function that gives details about REST enabled gemfire servers.
* <p/>
* @author Nilkanth Patel
* @since 8.1
*/

public class FindRestEnabledServersFunction extends FunctionAdapter implements InternalEntity  { 

  private static final long serialVersionUID = 7851518767859544678L;

  
  public void execute(FunctionContext context) {
    
    try {
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getAnyInstance();
      DistributionConfig config = InternalDistributedSystem.getAnyInstance().getConfig();
      
      final String protocolType =  config.getHttpServiceSSLEnabled() ? "https" : "http";
      
      if(c.isRESTServiceRunning()){
        context.getResultSender().lastResult(protocolType
        + "://"
        + config.getHttpServiceBindAddress()
        + ":"
        + config.getHttpServicePort());
        
      }else {
        context.getResultSender().lastResult("");
      }
    } catch (CacheClosedException ex) {
      context.getResultSender().lastResult("");
      
    }
  }

  public String getId() {
    return GemFireCacheImpl.FIND_REST_ENABLED_SERVERS_FUNCTION_ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
