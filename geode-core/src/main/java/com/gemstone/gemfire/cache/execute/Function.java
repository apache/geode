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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.lang.Identifiable;

/**
 * Defines the interface a user defined function implements.
 * {@link Function}s can be of different types. Some can have results while others need not return
 * any result. Some functions require writing in the targeted {@link Region} while some may just
 * be read operations.
 * Consider extending {@link FunctionAdapter} which has default values for some of the function attributes.
 * <p>Even though this interface extends Serializable, functions will only be serialized if they are not registered.
 * For best performance it is recommended that you implement {@link #getId()} to return a non-null identifier
 * and register your function using {@link FunctionService#registerFunction(Function)} or the cache.xml <code>function</code> element.
 *      
 * @see FunctionAdapter
 * @see FunctionService
 *
 *
 * @since GemFire 6.0
 */
@FunctionalInterface
public interface Function extends Identifiable<String> {

  /**
   * Specifies whether the function sends results while executing.
   * The method returns false if no result is expected.<br>
   * <p>
   * If {@link Function#hasResult()} returns false, 
   * {@link ResultCollector#getResult()} throws {@link FunctionException}. 
   * </p>
   * <p>
   * If {@link Function#hasResult()} returns true,
   * {@link ResultCollector#getResult()} blocks and waits for the
   * result of function execution
   * </p>
   * 
   * @return whether this function returns a Result back to the caller.
   * @since GemFire 6.0
   */
  public default boolean hasResult() {
    return true;
  }

  /**
   * The method which contains the logic to be executed. This method should be
   * thread safe and may be invoked more than once on a given member for a
   * single {@link Execution}.
   * The context provided to this function is the one which was built using {@linkplain Execution}.
   * The contexts can be data dependent or data-independent so user should check to see if the context
   * provided in parameter is instance of {@link RegionFunctionContext}.
   * 
   * @param context as created by {@link Execution}
   * @since GemFire 6.0
   */
  public void execute(FunctionContext context);

  /**
   * Return a unique function identifier, used to register the function
   * with {@link FunctionService}
   * 
   * @return string identifying this function
   * @since GemFire 6.0
   */
  public default String getId() {
    return getClass().getCanonicalName();
  }

  /**
   * <p>Return true to indicate to GemFire the method
   * requires optimization for writing the targeted {@link FunctionService#onRegion(com.gemstone.gemfire.cache.Region)} and any
   * associated {@linkplain Execution#withFilter(java.util.Set) routing objects}.</p>
   *
   * <p>Returning false will optimize for read behavior on the targeted
   * {@link FunctionService#onRegion(com.gemstone.gemfire.cache.Region)} and any
   * associated {@linkplain Execution#withFilter(java.util.Set) routing objects}.</p>
   *
   * <p>This method is only consulted when Region passed to 
   * FunctionService#onRegion(com.gemstone.gemfire.cache.Region) is a partitioned region
   * </p>
   *
   * @return false if the function is read only, otherwise returns true
   * @since GemFire 6.0
   * @see FunctionService
   */
  public default boolean optimizeForWrite() {
    return false;
  }
  
  /**
   * Specifies whether the function is eligible for re-execution (in case of
   * failure).
   * 
   * @return whether the function is eligible for re-execution.
   * @see RegionFunctionContext#isPossibleDuplicate()
   * 
   * @since GemFire 6.5
   */
  public default boolean isHA() {
    return true;
  }

}
