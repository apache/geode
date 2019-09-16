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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LowMemoryException;

/**
 * Provides methods to build the context for the execution of a {@link Function} . A Context
 * describes the environment in which the {@link Execution} will take place.
 * <p>
 * This interface is implemented by GemFire. To obtain an instance of it use
 * {@link FunctionService}.
 *
 * @param <IN> The type of the argument passed into the function, if any
 * @param <OUT> The type of results sent by the function
 * @param <AGG> The type of the aggregated result returned by the ResultCollector
 *
 * @since GemFire 6.0
 *
 * @see FunctionService
 * @see Function
 */
public interface Execution<IN, OUT, AGG> {

  /**
   * Specifies a data filter of routing objects for selecting the GemFire members to execute the
   * function on.
   * <p>
   * Applicable only for regions with {@link DataPolicy#PARTITION} DataPolicy. If the filter is
   * null, it will execute on all data of the region.
   *
   * @param filter Set defining the data filter to be used for executing the function
   * @return an Execution with the filter
   * @throws IllegalArgumentException if filter passed is null.
   * @throws UnsupportedOperationException if not called after
   *         {@link FunctionService#onRegion(org.apache.geode.cache.Region)}
   * @since GemFire 6.0
   */
  Execution<IN, OUT, AGG> withFilter(Set<?> filter);

  /**
   * Specifies the user data passed to the function when it is executed. The function can retrieve
   * these arguments using {@link FunctionContext#getArguments()}
   *
   * @param args user data passed to the function execution
   * @return an Execution with args
   * @throws IllegalArgumentException if the input parameter is null
   * @since Geode 1.2
   *
   */
  Execution<IN, OUT, AGG> setArguments(IN args);

  /**
   * Specifies the user data passed to the function when it is executed. The function can retrieve
   * these arguments using {@link FunctionContext#getArguments()}
   *
   * @param args user data passed to the function execution
   * @return an Execution with args
   * @throws IllegalArgumentException if the input parameter is null
   * @since GemFire 6.0
   * @deprecated use {@link #setArguments(Object)} instead
   *
   */
  Execution<IN, OUT, AGG> withArgs(IN args);

  /**
   * Specifies the {@link ResultCollector} that will receive the results after the function has been
   * executed. Collector will receive results as they are sent from the
   * {@link Function#execute(FunctionContext)} using {@link ResultSender}.
   *
   * @return an Execution with a collector
   * @throws IllegalArgumentException if {@link ResultCollector} is null
   * @see ResultCollector
   * @since GemFire 6.0
   */
  Execution<IN, OUT, AGG> withCollector(ResultCollector<OUT, AGG> rc);

  /**
   * Executes the function using its {@linkplain Function#getId() id}.
   * When executed from a client, it blocks until all results have been received
   * or the global timeout (gemfire.CLIENT_FUNCTION_TIMEOUT Java property) has expired.
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the instance retrieved using
   * {@link FunctionService#getFunction(String)} on the executing member.
   *
   * @param functionId id of the function to execute
   * @throws LowMemoryException if the {@link Function#optimizeForWrite()} returns true and there is
   *         a low memory condition
   * @return ResultCollector to retrieve the results received. This is different object than the
   *         ResultCollector provided in {@link Execution#withCollector(ResultCollector)}. User has
   *         to use this reference to retrieve results.
   *
   * @since GemFire 6.0
   */
  ResultCollector<OUT, AGG> execute(String functionId) throws FunctionException;

  /**
   * Executes the function using its {@linkplain Function#getId() id} with the specified timeout.
   * It blocks until all results have been received or the timeout has expired.
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the instance retrieved using
   * {@link FunctionService#getFunction(String)} on the executing member.
   *
   * @param functionId id of the function to execute
   * @param timeout time to wait for the operation to finish before timing out
   * @param unit timeout unit
   * @return ResultCollector to retrieve the results received. This is different object than the
   *         ResultCollector provided in {@link Execution#withCollector(ResultCollector)}. User has
   *         to use this reference to retrieve results.
   *
   * @since GemFire 6.0
   */
  ResultCollector<OUT, AGG> execute(String functionId, long timeout, TimeUnit unit)
      throws FunctionException;

  /**
   * Executes the function instance provided.
   * When executed from a client, it blocks until all results have been received
   * or the global timeout (gemfire.CLIENT_FUNCTION_TIMEOUT Java property) has expired.
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the de-serialized instance on the
   * executing member.
   *
   * @param function instance to execute
   * @throws LowMemoryException if the {@link Function#optimizeForWrite()} returns true and there is
   *         a low memory condition
   * @return ResultCollector to retrieve the results received. This is different object than the
   *         ResultCollector provided in {@link Execution#withCollector(ResultCollector)}. User has
   *         to use this reference to retrieve results.
   *
   * @since GemFire 6.0
   */
  ResultCollector<OUT, AGG> execute(Function function) throws FunctionException;

  /**
   * Executes the function instance provided.
   * It blocks until all results have been received or the timeout has expired.
   * <p>
   * {@link Function#execute(FunctionContext)} is called on the de-serialized instance on the
   * executing member.
   *
   * @param function instance to execute
   * @param timeout time to wait for the operation to finish before timing out
   * @param unit timeout unit
   * @throws LowMemoryException if the {@link Function#optimizeForWrite()} returns true and there is
   *         a low memory condition
   * @return ResultCollector to retrieve the results received. This is different object than the
   *         ResultCollector provided in {@link Execution#withCollector(ResultCollector)}. User has
   *         to use this reference to retrieve results.
   *
   * @since GemFire 6.0
   */
  ResultCollector<OUT, AGG> execute(Function function, long timeout, TimeUnit unit)
      throws FunctionException;

}
