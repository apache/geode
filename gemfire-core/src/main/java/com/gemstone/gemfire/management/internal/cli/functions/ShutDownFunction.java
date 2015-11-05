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
package com.gemstone.gemfire.management.internal.cli.functions;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.tcp.ConnectionTable;

/**
 * 
 * Class for Shutdown function
 * 
 * @author apande
 *  
 * 
 */
public class ShutDownFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  public static final String ID = ShutDownFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      String memberName = cache.getDistributedSystem().getDistributedMember().getId();
      cache.getLogger().info("Received GFSH shutdown. Shutting down member " + memberName);
      final InternalDistributedSystem system = ((InternalDistributedSystem) cache.getDistributedSystem());

      if (system.isConnected()) {
        ConnectionTable.threadWantsSharedResources();
        if (system.isConnected()) {
          system.disconnect();
        }
      }
      
      context.getResultSender().lastResult("SUCCESS: succeeded in shutting down " + memberName);
    } catch (Exception ex) {
      context.getResultSender().lastResult("FAILURE: failed in shutting down " +ex.getMessage());
    }
  }

  @Override
  public String getId() {
    return ShutDownFunction.ID;

  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

}
