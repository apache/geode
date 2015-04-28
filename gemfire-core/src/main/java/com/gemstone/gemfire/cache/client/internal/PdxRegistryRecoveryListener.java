/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

/**
 * A listener which will wipe out the PDX registry on the client side if the
 * entire server distributed system was lost and came back on line. <br>
 * <br>
 * TODO - There is a window in which all of the servers could crash and come
 * back up and we would connect to a new server before realizing that all the
 * servers crashed. To fix this, we would need to get some kind of birthdate of
 * the server ds we connect and use that to decide if we need to recover
 * the PDX registry.
 * 
 * We can also lose connectivity with the servers, even if the servers are still
 * running. Maybe for the PDX registry we need some way of telling if the PDX
 * registry was lost at the server side in the interval. 
 * 
 * 
 * @author dsmith
 * 
 */
public class PdxRegistryRecoveryListener extends EndpointManager.EndpointListenerAdapter {
  private static final Logger logger = LogService.getLogger();
  
  private final AtomicInteger endpointCount = new AtomicInteger();
  private final InternalPool pool;
  
  public PdxRegistryRecoveryListener(InternalPool pool) {
    this.pool = pool;
  }
  
  @Override
  public void endpointCrashed(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("PdxRegistryRecoveryListener - EndpointCrashed. Now have {} endpoints", count);
    }
  }

  @Override
  public void endpointNoLongerInUse(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("PdxRegistryRecoveryListener - EndpointNoLongerInUse. Now have {} endpoints", count);
    }
  }

  @Override
  public void endpointNowInUse(Endpoint endpoint) {
    int count  = endpointCount.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("PdxRegistryRecoveryListener - EndpointNowInUse. Now have {} endpoints", count);
    }
    if(count == 1) {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache == null) {
        return;
      }
      TypeRegistry registry = cache.getPdxRegistry();
      
      if(registry == null) {
        return;
      }
      registry.clear();
    }
  }
}
