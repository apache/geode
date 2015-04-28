/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.cache.execute;

/**
 * Defines the execution context of a {@link Function}. It is required
 * by the {@link Function#execute(FunctionContext)} to execute a {@link Function}
 * on a particular member.
 * <p>
 * A context can be data dependent or data independent.
 * For data dependent functions refer to {@link RegionFunctionContext}
 * </p>
 * <p>This interface is implemented by GemFire. Instances of it will be passed
 * in to {@link Function#execute(FunctionContext)}.
 * 
 * @author Yogesh Mahajan
 * @author Mitch Thomas
 *
 * @since 6.0
 *
 * @see RegionFunctionContext
 *
 */
public interface FunctionContext {
  /**
   * Returns the arguments provided to this function execution. These are the
   * arguments specified by the caller using
   * {@link Execution#withArgs(Object)}
   * 
   * @return the arguments or null if there are no arguments
   * @since 6.0
   */
  public Object getArguments();

  /**
   * Returns the identifier of the function.
   *  
   * @return a unique identifier
   * @see Function#getId()
   * @since 6.0
   */
  public String getFunctionId();
  
  /**
   * Returns the ResultSender which is used to add the ability for an execute
   * method to send a single result back, or break its result into multiple
   * pieces and send each piece back to the calling thread's ResultCollector.
   * 
   * @return ResultSender
   * @since 6.0
   */
  
  public <T> ResultSender<T> getResultSender();
  
  /**
   * Returns a boolean to identify whether this is a re-execute. Returns true if
   * it is a re-execute else returns false
   * 
   * @return a boolean (true) to identify whether it is a re-execute (else
   *         false)
   * 
   * @since 6.5
   * @see Function#isHA()
   */
  public boolean isPossibleDuplicate();
}
