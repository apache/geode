/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.cache.execute;

import java.io.Serializable;

import java.util.concurrent.TimeUnit;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A Special ResultCollector implementation. Functions having
 * {@link Function#hasResult()} false, this ResultCollector will be returned.
 * <br>
 * Calling getResult on this NoResult will throw
 * {@link FunctionException}
 * 
 * 
 * @author Yogesh Mahajan
 * 
 * @since 5.8 Beta
 * 
 * @see Function#hasResult()
 * 
 */
public final class NoResult implements ResultCollector, Serializable {

  private static final long serialVersionUID = -4901369422864228848L;

  public void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
    throw new UnsupportedOperationException(
        LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
            .toLocalizedString("add"));
  }

  public void endResults() {
    throw new UnsupportedOperationException(
        LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
            .toLocalizedString("close"));
  }

  public Object getResult() throws FunctionException {
    throw new FunctionException(
        LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
            .toLocalizedString("return any"));
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    throw new FunctionException(
        LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
            .toLocalizedString("return any"));
  }
  
  public void clearResults() {
    throw new UnsupportedOperationException(
        LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
            .toLocalizedString("clear"));
  }
}
