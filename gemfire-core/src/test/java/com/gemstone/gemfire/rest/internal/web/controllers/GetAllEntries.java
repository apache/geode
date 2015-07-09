/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;

/**
 * The GetAllEntries is function that will return a map as a result of its execution.
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
 */

public class GetAllEntries implements Function {

  @Override
  public void execute(FunctionContext context) {
    Map<String, String> myMap = new HashMap();
    myMap.put("k11", "v1");
    myMap.put("k12", "v2");
    myMap.put("k13", "v3");
    myMap.put("k14", "v4");
    myMap.put("k15", "v5");
     
    //return map as a function result
    context.getResultSender().lastResult(myMap);
  }

  @Override
  public String getId() {
    return "GetAllEntries";
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

