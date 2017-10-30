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
package org.apache.geode.internal.cache.execute;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;

/**
 * Context available to application functions which is passed from GemFire to {@link Function}. <br>
 * 
 * For data dependent functions refer to {@link RegionFunctionContext}
 * 
 * 
 * @since GemFire 6.0
 * 
 * @see RegionFunctionContextImpl
 * 
 */
public class FunctionContextImpl implements FunctionContext {

  private Object args = null;

  private String functionId = null;

  private Cache cache = null;

  private ResultSender resultSender = null;

  private final boolean isPossDup;

  public FunctionContextImpl(final Cache cache, final String functionId, final Object args,
      ResultSender resultSender) {
    this(cache, functionId, args, resultSender, false);
  }

  public FunctionContextImpl(final Cache cache, final String functionId, final Object args,
      ResultSender resultSender, boolean isPossibleDuplicate) {
    this.cache = cache;
    this.functionId = functionId;
    this.args = args;
    this.resultSender = resultSender;
    this.isPossDup = isPossibleDuplicate;
  }

  /**
   * Returns the arguments provided to this function execution. These are the arguments specified by
   * caller using {@link Execution#setArguments(Object)}
   * 
   * @return the arguments or null if there are no arguments
   */
  public Object getArguments() {
    return this.args;
  }

  /**
   * Get the identifier of the running function used for logging and administration purposes
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

  public ResultSender getResultSender() {
    return this.resultSender;
  }

  public boolean isPossibleDuplicate() {
    return this.isPossDup;
  }

  @Override
  public Cache getCache() throws CacheClosedException {
    if (cache == null) {
      throw new CacheClosedException("FunctionContext does not have a valid Cache");
    }
    return cache;
  }

}
