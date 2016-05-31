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
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;

/**
 * Context available to application functions which is passed from GemFire to
 * {@link Function}. <br>
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
  
  private ResultSender resultSender = null ;
  
  private final boolean isPossDup;
  
  public FunctionContextImpl(final String functionId, final Object args,
      ResultSender resultSender) {
    this.functionId = functionId;
    this.args = args;
    this.resultSender = resultSender;
    this.isPossDup = false;
  }
  
  public FunctionContextImpl(final String functionId, final Object args,
      ResultSender resultSender, boolean isPossibleDuplicate) {
    this.functionId = functionId;
    this.args = args;
    this.resultSender = resultSender;
    this.isPossDup = isPossibleDuplicate;
  }

  /**
   * Returns the arguments provided to this function execution. These are the
   * arguments specified by caller using
   * {@link Execution#withArgs(Object)}
   * 
   * @return the arguments or null if there are no arguments
   */
  public final Object getArguments() {
    return this.args;
  }

  /**
   * Get the identifier of the running function used for logging and
   * administration purposes
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

  public <T> ResultSender<T> getResultSender() {    
    return this.resultSender;
  }

  public boolean isPossibleDuplicate() {
    return this.isPossDup;
  }
  
}
