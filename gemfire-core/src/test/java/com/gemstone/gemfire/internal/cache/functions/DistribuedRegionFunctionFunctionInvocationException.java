/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.functions;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;

public class DistribuedRegionFunctionFunctionInvocationException extends
    FunctionAdapter {
  private int retryCount;

  private boolean isHA;

  private int count;

  public DistribuedRegionFunctionFunctionInvocationException(boolean isHA,
      int retryCount) {
    this.isHA = isHA;
    this.retryCount = retryCount;
    this.count = 0;
  }

  public void execute(FunctionContext context) {
    this.count++;
    if (retryCount != 0 && count >= retryCount) {
      context.getResultSender().lastResult(new Integer(5));
    }
    else {
      throw new FunctionInvocationTargetException("I have been thrown from DistribuedRegionFunctionFunctionInvocationException");
    }
  }

  public String getId() {
    return "DistribuedRegionFunctionFunctionInvocationException";
  }

  public boolean isHA() {
    return this.isHA;
  }
  
  @Override
  public boolean optimizeForWrite() {
    return false;
  }
}
