/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;

/**
 * Context available to application functions which is passed from GemFire to
 * {@link Function}. <br>
 * 
 * For data dependent functions refer to {@link RegionFunctionContext}
 * 
 * @author Yogesh Mahajan
 * @author Mitch Thomas
 * 
 * @since 6.0
 * 
 * @see RegionFunctionContextImpl
 * 
 */
public class FunctionContextImpl implements FunctionContext {

  private Object args = null;

  private String functionId = null;
  
  private ResultSender resultSender = null ;
  
  private final boolean isPossDup;
  
  public FunctionContextImpl(final String functionId, final Object args,
      ResultSender resultSender) {
    this.functionId = functionId;
    this.args = args;
    this.resultSender = resultSender;
    this.isPossDup = false;
  }
  
  public FunctionContextImpl(final String functionId, final Object args,
      ResultSender resultSender, boolean isPossibleDuplicate) {
    this.functionId = functionId;
    this.args = args;
    this.resultSender = resultSender;
    this.isPossDup = isPossibleDuplicate;
  }

  /**
   * Returns the arguments provided to this function execution. These are the
   * arguments specified by caller using
   * {@link Execution#withArgs(Object)}
   * 
   * @return the arguments or null if there are no arguments
   */
  public final Object getArguments() {
    return this.args;
  }

  /**
   * Get the identifier of the running function used for logging and
   * administration purposes
   * 
   * @return String uniquely identifying this running instance
   * @see Function#getId()
   */
  public String getFunctionId() {
    return this.functionId;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("[FunctionContextImpl:");
    buf.append("functionId=");
    buf.append(this.functionId);
    buf.append(";args=");
    buf.append(this.args);
    buf.append(']');
    return buf.toString();
  }

  public <T> ResultSender<T> getResultSender() {    
    return this.resultSender;
  }

  public boolean isPossibleDuplicate() {
    return this.isPossDup;
  }
  
}
