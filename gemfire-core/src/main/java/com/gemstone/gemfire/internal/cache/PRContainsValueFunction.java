/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;

/**
 * @author Suranjan Kumar
 *
 */
public class PRContainsValueFunction extends FunctionAdapter implements InternalEntity {

  @Override
  public void execute(FunctionContext context) {

    RegionFunctionContext prContext = (RegionFunctionContext)context;
    Region dataSet = prContext.getDataSet();
    Object values = context.getArguments();
    
    Iterator itr = dataSet.values().iterator();
    while (itr.hasNext()) {
      Object val = itr.next();
      if(val.equals(values)) {
        prContext.getResultSender().lastResult(Boolean.TRUE);
        return;
      }
    }
    prContext.getResultSender().lastResult(Boolean.FALSE);
  }
  
  @Override
  public String getId() {
    return getClass().getName();
  }
  
  @Override
  public boolean optimizeForWrite() {
    return false;
  }
}
