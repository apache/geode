/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.cache.execute;

/**
 * Provides methods to send results back to the ResultCollector. A ResultSender
 * adds the ability for an execute method to send a single result back, or break
 * its result into multiple pieces and send each piece back to the calling
 * thread's {@link ResultCollector}. For each result sent using this method,
 * {@link ResultCollector#addResult(com.gemstone.gemfire.distributed.DistributedMember, Object)} is called,
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
 * @see ResultCollector#addResult(com.gemstone.gemfire.distributed.DistributedMember, Object)
 * 
 */
public interface ResultSender<T> {
  /**
   * Sends a result back to the FunctionService calling thread and invokes
   * {@link ResultCollector#addResult(com.gemstone.gemfire.distributed.DistributedMember, Object)}.
   * 
   * @param oneResult
   */
  public void sendResult(T oneResult);

  /**
   * Sends a result back to the FunctionService calling thread and invokes
   * {@link ResultCollector#addResult(com.gemstone.gemfire.distributed.DistributedMember, Object)} and then
   * {@link ResultCollector#endResults()} if it is the last instance of the
   * Function to report results. The ResultCollector will keep waiting for
   * results until it receives last result. Therefore, it is very important to
   * use this method to indicate end of function execution.
   * 
   * @throws IllegalStateException
   *                 if called more than once
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
