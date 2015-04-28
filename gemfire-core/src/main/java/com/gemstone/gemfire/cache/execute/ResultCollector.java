/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.cache.execute;

import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * <p>
 * Defines the interface for a container that gathers results from function
 * execution.<br>
 * GemFire provides a default implementation for ResultCollector. Applications
 * can choose to implement their own custom ResultCollector. A custom
 * ResultCollector facilitates result sorting or aggregation. Aggregation
 * functions like sum, minimum, maximum and average can also be applied to the
 * result using a custom ResultCollector. Results arrive as they are sent using
 * the {@link ResultSender#sendResult(Object)} and can be used as they arrive.
 * To indicate that all results have been received {@link #endResults()} is
 * called.
 * 
 * </p>
 * Example: <br>
 * 
 * <pre>
 *  Region region ; 
 *  Set keySet = Collections.singleton(&quot;myKey&quot;);
 *  Function multiGetFunction ;
 *  Object args ;
 *  ResultCollector rc = FunctionService.onRegion(region)
 *                                      .withArgs(args)
 *                                      .withFilter(keySet)
 *                                      .withCollector(new MyCustomResultCollector())
 *                                      .execute(multiGetFunction.getId());
 *  Application can do something else here before retrieving the result
 *  Or it can have some logic in {{@link #addResult(DistributedMember, Object)} to use the partial results.
 *  If it wants to see all the results it can use 
 *  Object functionResult = rc.getResult();
 *  or
 *  Object functionResult = rc.getResult(timeout,TimeUnit);
 *  depending on if it wants to wait for all the results or wait for a timeout period.
 * </pre>
 * 
 * GemFire provides default implementation of ResultCollector which collects
 * results in Arraylist. There is no need to provide a synchronization mechanism
 * in the user implementations of ResultCollector
 * 
 * @author Yogesh Mahajan
 * @since 6.0
 * 
 */
public interface ResultCollector<T,S> {

  /**
   * Method used to pull results from the ResultCollector. It returns the result
   * of function execution, potentially blocking until {@link #endResults() all
   * the results are available} has been called.
   * 
   * @return the result
   * @throws FunctionException
   *           if result retrieval fails
   * @since 6.0
   */
  public S getResult() throws FunctionException;

  /**
   * Method used to pull results from the ResultCollector. It returns the result
   * of function execution, blocking for the timeout period until
   * {@link #endResults() all the results are available}. If all the results are
   * not received in provided time a FunctionException is thrown.
   * 
   * @param timeout
   *          the maximum time to wait
   * @param unit
   *          the time unit of the timeout argument
   * @return the result
   * @throws FunctionException
   *           if result retrieval fails within timeout provided
   * @throws InterruptedException
   *           if the current thread was interrupted while waiting
   * @since 6.0
   * 
   */
  public S getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException;

  /**
   * Method used to feed result to the ResultCollector. It adds a single
   * function execution result to the ResultCollector It is invoked every time a
   * result is sent using ResultSender.
   * 
   * @param resultOfSingleExecution
   * @since 6.0
   * @param memberID
   *          DistributedMember ID to which result belongs
   */
  public void addResult(DistributedMember memberID, T resultOfSingleExecution);

  /**
   * GemFire will invoke this method when function execution has completed and
   * all results for the execution have been obtained and
   * {@link #addResult(DistributedMember, Object) added to the ResultCollector}
   * Unless the {@link ResultCollector} has received
   * {@link ResultSender#lastResult(Object) last result} from all the
   * executing nodes, it keeps waiting for more results to come.
   * 
   * @since 6.0
   * 
   * @see ResultSender#lastResult(Object)
   */
  public void endResults();
  
  /**
   * GemFire will invoke this method before re-executing function (in case of
   * Function Execution HA). This is to clear the previous execution results from
   * the result collector
   * 
   * @since 6.5
   */
  public void clearResults();

}
