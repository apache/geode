/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.ArrayList;

import java.util.concurrent.TimeUnit;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;

public class CustomResultCollector implements ResultCollector{

  private ArrayList resultList = new ArrayList();  

  public void addResult(DistributedMember memberID,
      Object result) {
    this.resultList.add(result);
  }
  
  public void endResults() {
  }

  public Object getResult() throws FunctionException {
    return resultList;
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    return resultList;
  }
  
  public void clearResults() {
    resultList.clear();
  }
}
