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
package com.gemstone.gemfire.internal.cache.execute.util;


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
* @since GemFire 8.1
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
