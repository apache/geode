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

import java.util.Set;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LowMemoryException;

/**
 * Provides methods to build the context for the execution of a {@link Function}
 * . A Context describes the environment in which the {@link Execution} will
 * take place.
 * <p>This interface is implemented by GemFire. To obtain an instance of it use {@link FunctionService}.
 * 
 * 
 * @since GemFire 6.0
 * 
 * @see FunctionService
 * @see Function
 */
public interface Execution {

  /**
   * Specifies a data filter of routing objects for selecting the GemFire
   * members to execute the function on.
   * <p>
   * Applicable only for regions with {@link DataPolicy#PARTITION} DataPolicy.
   * If the filter is null, it will execute on all data of the region.
   * 
   * @param filter
   *          Set defining the data filter to be used for executing the function
   * @return an Execution with the filter
   * @throws IllegalArgumentException
   *           if filter passed is null.
   * @throws UnsupportedOperationException
   *           if not called after
   *           {@link FunctionService#onRegion(com.gemstone.gemfire.cache.Region)}
   * @since GemFire 6.0
   */
  public Execution withFilter(Set<?> filter);

  /**
   * Specifies the user data passed to the function when it is executed.
   * The function can retrieve these arguments using {@link FunctionContext#getArguments()}
   * @param args user data passed to the function execution
   * @return an Execution with args 
   * @throws IllegalArgumentException if the input parameter is null
   * @since GemFire 6.0
   *                 
   */
  public Execution withArgs(Object args);

  /**
   * Specifies the {@link ResultCollector} that will receive the results after
   * the function has been executed. Collector will receive results as they are sent 
   * from the {@link Function#execute(FunctionContext)} using {@link ResultSender}.
   * @return an Execution with a collector
   * @throws IllegalArgumentException if {@link ResultCollector} is null
   * @see ResultCollector
   * @since GemFire 6.0
   */
  public Execution withCollector(
      ResultCollector<?, ?> rc);

  /**
   * Executes the function using its {@linkplain Function#getId() id}
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the instance
   * retrieved using {@link FunctionService#getFunction(String)} on the
   * executing member.
   * 
   * @param functionId
   *          the {@link Function#getId()} of the function
   * @throws LowMemoryException
   *           if the {@link Function#optimizeForWrite()} returns true and there
   *           is a low memory condition
   * @return ResultCollector to retrieve the results received. This is different
   *         object than the ResultCollector provided in
   *         {@link Execution#withCollector(ResultCollector)}. User has to use
   *         this reference to retrieve results.
   * 
   * @since GemFire 6.0
   */
  public ResultCollector<?, ?> execute(
      String functionId) throws FunctionException;

  /**
   * Executes the function instance provided.
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the de-serialized
   * instance on the executing member.
   * 
   * @param function
   *          instance to execute
   * @throws LowMemoryException
   *           if the {@link Function#optimizeForWrite()} returns true and there
   *           is a low memory condition
   * @return ResultCollector to retrieve the results received. This is different
   *         object than the ResultCollector provided in
   *         {@link Execution#withCollector(ResultCollector)}. User has to use
   *         this reference to retrieve results.
   * 
   * @since GemFire 6.0
   */
  public ResultCollector<?, ?> execute(
      Function function) throws FunctionException;

  /**
   * Executes the function using its {@linkplain Function#getId() id}
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the de-serialized
   * instance on the executing member. Function should be registered on the
   * executing member using {@link FunctionService#registerFunction(Function)}
   * method before calling this method.
   * 
   * As of 6.6, this is deprecated, since users can pass different value for
   * the hasResult parameter that than the boolean value returned from {@link Function#hasResult()}.
   * 
   * @param functionId
   *          the {@link Function#getId()} of the function
   * @param hasResult
   *          Whether the function returns any result
   * @throws LowMemoryException
   *           if the {@link Function#optimizeForWrite()} returns true and there
   *           is a low memory condition
   * @return ResultCollector to retrieve the results received. This is different
   *         object than the ResultCollector provided in
   *         {@link Execution#withCollector(ResultCollector)}. User has to use
   *         this reference to retrieve results.
   * 
   * @since GemFire 6.5
   * @deprecated as of 6.6, use {@link #execute(String)} instead
   */
  @Deprecated
  public ResultCollector<?, ?> execute(
      String functionId, boolean hasResult) throws FunctionException;

  /**
   * Executes the function using its {@linkplain Function#getId() id}
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the de-serialized
   * instance on the executing member.Function should be registered on the
   * executing member using {@link FunctionService#registerFunction(Function)}
   * method before calling this method.
   * 
   * As of 6.6, this is deprecated, since users can pass different value for the
   * hasResult, isHA parameter that than the boolean values returned from
   * {@link Function#hasResult()}, {@link Function#isHA()}.
   * 
   * @param functionId
   *          the {@link Function#getId()} of the function
   * @param hasResult
   *          Whether the function returns any result
   * @param isHA
   *          Whether the given function is HA
   * @throws LowMemoryException
   *           if the {@link Function#optimizeForWrite()} returns true and there
   *           is a low memory condition
   * @return ResultCollector to retrieve the results received. This is different
   *         object than the ResultCollector provided in
   *         {@link Execution#withCollector(ResultCollector)}. User has to use
   *         this reference to retrieve results.
   * 
   * @since GemFire 6.5
   * @deprecated as of 6.6, use {@link #execute(String)} instead
   */
  @Deprecated
  public ResultCollector<?, ?> execute(
      String functionId, boolean hasResult, boolean isHA)
      throws FunctionException;

  /**
   * Executes the function using its {@linkplain Function#getId() id}
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the de-serialized
   * instance on the executing member.Function should be registered on the
   * executing member using {@link FunctionService#registerFunction(Function)}
   * method before calling this method.
   * 
   * As of 6.6, this is deprecated, since users can pass different value for the
   * hasResult, isHA,  optimizeForWrite parameters that than the boolean values returned from
   * {@link Function#hasResult()}, {@link Function#isHA()}, {@link Function#optimizeForWrite()}.
   * 
   * @param functionId
   *          the {@link Function#getId()} of the function
   * @param hasResult
   *          Whether the function returns any result
   * @param isHA
   *          Whether the given function is HA
   * @param optimizeForWrite
   *          Whether the function should be optmized for write operations
   * @throws LowMemoryException
   *           if the {@link Function#optimizeForWrite()} returns true and there
   *           is a low memory condition
   * @return ResultCollector to retrieve the results received. This is different
   *         object than the ResultCollector provided in
   *         {@link Execution#withCollector(ResultCollector)}. User has to use
   *         this reference to retrieve results.
   * 
   * @since GemFire 6.5
   * @deprecated as of 6.6, use {@link #execute(String)} instead
   */
  @Deprecated
  public ResultCollector<?, ?> execute(
      String functionId, boolean hasResult, boolean isHA,
      boolean optimizeForWrite) throws FunctionException;
}
