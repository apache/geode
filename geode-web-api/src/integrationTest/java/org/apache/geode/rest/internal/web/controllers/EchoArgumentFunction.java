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

package org.apache.geode.rest.internal.web.controllers;

import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;

public class EchoArgumentFunction implements Function<Object> {
  /**
   * Specifies whether the function sends results while executing. The method returns false if no
   * result is expected.<br>
   * <p>
   * If {@link Function#hasResult()} returns false, {@link ResultCollector#getResult()} throws
   * {@link FunctionException}.
   * </p>
   * <p>
   * If {@link Function#hasResult()} returns true, {@link ResultCollector#getResult()} blocks and
   * waits for the result of function execution
   * </p>
   *
   * @return whether this function returns a Result back to the caller.
   * @since GemFire 6.0
   */
  @Override
  public boolean hasResult() {
    return true;
  }

  /**
   * The method which contains the logic to be executed. This method should be thread safe and may
   * be invoked more than once on a given member for a single {@link Execution}. The context
   * provided to this function is the one which was built using {@linkplain Execution}. The contexts
   * can be data dependent or data-independent so user should check to see if the context provided
   * in parameter is instance of {@link RegionFunctionContext}.
   *
   * @param context as created by {@link Execution}
   * @since GemFire 6.0
   */
  @Override
  public void execute(final FunctionContext<Object> context) {
    Object args = context.getArguments();

    // Echo the arguments back to the caller for assertion
    if (args != null && args.getClass().isAssignableFrom(Object.class)) {
      // Object is not serializable
      context.getResultSender().lastResult(new ObjectMapper().createObjectNode());
    } else {
      context.getResultSender().lastResult(args);
    }
  }

  /**
   * Return a unique function identifier, used to register the function with {@link
   * FunctionService}
   *
   * @return string identifying this function
   * @since GemFire 6.0
   */
  @Override
  public String getId() {
    return "EchoArgumentFunction";
  }

  /**
   * <p>
   * Return true to indicate to GemFire the method requires optimization for writing the targeted
   * {@link FunctionService#onRegion(Region)} and any associated {@linkplain
   * Execution#withFilter(Set) routing objects}.
   * </p>
   *
   * <p>
   * Returning false will optimize for read behavior on the targeted {@link
   * FunctionService#onRegion(Region)} and any associated {@linkplain Execution#withFilter(Set)
   * routing objects}.
   * </p>
   *
   * <p>
   * This method is only consulted when Region passed to
   * FunctionService#onRegion(org.apache.geode.cache.Region)
   * is a partitioned region
   * </p>
   *
   * @return false if the function is read only, otherwise returns true
   * @see FunctionService
   * @since GemFire 6.0
   */
  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  /**
   * Specifies whether the function is eligible for re-execution (in case of failure).
   *
   * @return whether the function is eligible for re-execution.
   * @see RegionFunctionContext#isPossibleDuplicate()
   * @since GemFire 6.5
   */
  @Override
  public boolean isHA() {
    return false;
  }
}
