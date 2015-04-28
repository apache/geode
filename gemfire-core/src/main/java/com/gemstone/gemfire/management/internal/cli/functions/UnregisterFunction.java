/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;

/**
 * 
 * Class for Unregister function
 * 
 * @author apande
 *  
 * 
 */


public class UnregisterFunction implements Function, InternalEntity {
  public static final String ID = UnregisterFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String functionId = (String) args[0];  
    try{
    FunctionService.unregisterFunction(functionId);
    }catch(Exception e){
      context.getResultSender().lastResult("Failed in unregistering "+ e.getMessage());
    }
    context.getResultSender().lastResult("Succeeded in unregistering");
  }

  @Override
  public String getId() {
    return UnregisterFunction.ID;

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