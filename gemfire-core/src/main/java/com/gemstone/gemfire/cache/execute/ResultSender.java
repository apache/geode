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
 * Provides methods to send results back to the ResultCollector. A ResultSender
 * adds the ability for an execute method to send a single result back, or break
 * its result into multiple pieces and send each piece back to the calling
 * thread's {@link ResultCollector}. For each result sent using this method,
 * {@link ResultCollector#addResult(DistributedMember, Object)} is called,
 * making that result available to the calling thread immediately.
 * 
 * <p>
 * Example:
 * 
 * <pre>
 *   
 *      execute(FunctionContext context){
 *              ResultSender rs = context.getResultSender();
 *              int lastResult = -1;
 *              for(int i=0;i&lt; 10; i++) {
 *                      rs.sendResult(i);
 *              }
 *              rs.lastResult(lastResult);
 *      }
 *      
 *  Application can receive the results as they are sent using ResultSender in the above for loop.
 *  It is very important to send a last result as it informs {@link ResultCollector}
 *  to stop waiting for the result.
 * <br>
 * </pre>
 * 
 * @author Mitch Thomas
 * @author Yogesh Mahajan
 * 
 * @since 6.0
 * 
 * @see ResultCollector#addResult(DistributedMember, Object)
 * 
 */
public interface ResultSender<T> {
  /**
   * Sends a result back to the FunctionService calling thread and invokes
   * {@link ResultCollector#addResult(DistributedMember, Object)}.
   * 
   * @param oneResult
   */
  public void sendResult(T oneResult);

  /**
   * Sends a result back to the FunctionService calling thread and invokes
   * {@link ResultCollector#addResult(DistributedMember, Object)} and then
   * {@link ResultCollector#endResults()} if it is the last instance of the
   * Function to report results. The ResultCollector will keep waiting for
   * results until it receives last result. Therefore, it is very important to
   * use this method to indicate end of function execution.
   * 
   * @throws IllegalStateException
   *                 if called more than once
   * @param lastResult
   * 
   * @see ResultCollector#endResults()
   */
  public void lastResult(T lastResult);

  /**
   * Sends an Exception back to the FunctionService calling thread.
   * sendException adds exception to ResultCollector as a result. If
   * sendException is called then {@link ResultCollector#getResult} will not
   * throw exception but will have exception as a part of results received.
   * Calling sendException will act as a lastResult.
   *  
   * @param t
   * 
   * @see #lastResult(Object)
   * @since 6.3
   */
  public void sendException(Throwable t);
}
