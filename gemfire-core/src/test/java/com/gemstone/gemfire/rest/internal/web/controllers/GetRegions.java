/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.util.ArrayList;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;

/**
* The GetRegions class is an gemfire function that gives data about available regions.
* <p/>
* @author Nilkanth Patel
* @since 8.0
*/

public class GetRegions implements Function  {

  public void execute(FunctionContext context) {
    
    ArrayList<String> vals = new ArrayList<String>();
    
    Cache c = null;
    try {
      c = CacheFactory.getAnyInstance();
    } catch (CacheClosedException ex) {
      vals.add("NoCacheResult");
      context.getResultSender().lastResult(vals);
    }

    final Set<Region<?, ?>> regionSet = c.rootRegions();
    for (Region<?, ?> r : regionSet) {
      vals.add(r.getName());
    }
    
    context.getResultSender().lastResult(vals);
  }

  public String getId() {
    return "GetRegions";
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
};
