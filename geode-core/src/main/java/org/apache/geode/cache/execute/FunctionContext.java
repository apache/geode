/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.execute;

import org.apache.geode.cache.Cache;

/**
 * Defines the execution context of a {@link Function}. It is required by the
 * {@link Function#execute(FunctionContext)} to execute a {@link Function} on a particular member.
 * <p>
 * A context can be data dependent or data independent. For data dependent functions refer to
 * {@link RegionFunctionContext}
 * </p>
 * <p>
 * This interface is implemented by GemFire. Instances of it will be passed in to
 * {@link Function#execute(FunctionContext)}.
 * 
 * @param T1 object type of Arguments
 *
 * @since GemFire 6.0
 *
 * @see RegionFunctionContext
 *
 */
public interface FunctionContext<T1> {
  /**
   * Returns the arguments provided to this function execution. These are the arguments specified by
   * the caller using {@link Execution#setArguments(Object)}
   * 
   * @return the arguments or null if there are no arguments
   * @since GemFire 6.0
   */
  public T1 getArguments();

  /**
   * Returns the identifier of the function.
   * 
   * @return a unique identifier
   * @see Function#getId()
   * @since GemFire 6.0
   */
  public String getFunctionId();

  /**
   * Returns the ResultSender which is used to add the ability for an execute method to send a
   * single result back, or break its result into multiple pieces and send each piece back to the
   * calling thread's ResultCollector.
   *
   * The returned ResultSender is only valid for the duration of the function call. If the Function
   * needs to return a result, the result should be sent before the function exits.
   * 
   * @return ResultSender
   * @since GemFire 6.0
   */

  public <T2> ResultSender<T2> getResultSender();

  /**
   * Returns a boolean to identify whether this is a re-execute. Returns true if it is a re-execute
   * else returns false
   * 
   * @return a boolean (true) to identify whether it is a re-execute (else false)
   * 
   * @since GemFire 6.5
   * @see Function#isHA()
   */
  public boolean isPossibleDuplicate();

  public Cache getCache();
}
