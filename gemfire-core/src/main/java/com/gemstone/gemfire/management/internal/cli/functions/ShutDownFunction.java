/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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