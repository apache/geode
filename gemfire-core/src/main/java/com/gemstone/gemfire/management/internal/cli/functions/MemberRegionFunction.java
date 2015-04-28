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
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.InternalEntity;


public class MemberRegionFunction implements Function, InternalEntity {
  public static final String ID = MemberRegionFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    String region = (String) args[0];
    String functionId = (String) args[1];    
    Cache cache = CacheFactory.getAnyInstance();    
    
    try {      
      Function function = FunctionService.getFunction(functionId);
      if (function == null){
        context.getResultSender().lastResult("For region on a member did not get function "+functionId);
      }
      Execution execution = FunctionService.onRegion(cache.getRegion(region));
      if (execution == null){
        context.getResultSender().lastResult("For region on a member could not execute");
      }else{
        execution.execute(function);
        context.getResultSender().lastResult("succeeded in executing on region "+region);
      }

    }catch(FunctionException e){
      context.getResultSender().lastResult(
          "FunctionException in MemberRegionFunction =" + e.getMessage());
    }catch (Exception e) {
      context.getResultSender().lastResult(
          "Exception in MemberRegionFunction =" + e.getMessage());
    }
    
  }

  @Override
  public String getId() {
    return MemberRegionFunction.ID;

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