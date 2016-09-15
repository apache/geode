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
package org.apache.geode.cache.client.internal;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.internal.TypeRegistry;

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
    if (count == 1) {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache == null) {
        return;
      }
      TypeRegistry registry = cache.getPdxRegistry();
      
      if (registry == null) {
        return;
      }
      registry.clear();
    }
  }
}
