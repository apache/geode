/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;

public class GetValueForKey implements Function {
  
  @Override
  public void execute(FunctionContext context) {  
    Object args = context.getArguments();
    
    Cache cache = null;
    
    try{
      cache = CacheFactory.getAnyInstance();
      
      if(args.toString().equalsIgnoreCase("1") ){
        Region<String, Object> r = cache.getRegion("Products");
        Object result = r.get("1");
        context.getResultSender().lastResult(result);
      
      }else if(args.toString().equalsIgnoreCase("2")){
        Region<String, Object> r = cache.getRegion("People");
        Object result = r.get("2");
        context.getResultSender().lastResult(result);      
      }else{
        //Default case
        int i=10;
        context.getResultSender().lastResult(i);
      }
    }catch(CacheClosedException e){
      context.getResultSender().lastResult("Error: CacheClosedException");
    }
    
  }

  @Override
  public String getId() {    
    return "GetValueForKey";
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

