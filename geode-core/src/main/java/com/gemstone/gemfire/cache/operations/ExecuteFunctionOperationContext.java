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
package com.gemstone.gemfire.cache.operations;

import com.gemstone.gemfire.cache.operations.internal.ResourceOperationContext;

import java.util.Set;

/**
 * OperationContext for Function execution operation. This is for the pre-operation case
 * 
 * @since GemFire 6.0
 *
 */
public class ExecuteFunctionOperationContext extends ResourceOperationContext {

  private String functionId;

  private boolean optimizeForWrite;

  private Set keySet;
  
  private Object arguments;

  private Object result;

  /**
   * Constructor for the EXECUTE_FUNCTION operation.
   *
   * @param functionName     the name of the function being executed
   * @param regionName       the name of the region on which the function is being executed
   * @param keySet           the set of keys against which the function is filtered
   * @param arguments        the array of function arguments
   * @param optimizeForWrite boolean indicating whether this function is optimized for writing
   * @param isPostOperation  boolean indicating whether this context is for post-operation evaluation
   */
  public ExecuteFunctionOperationContext(String functionName,
      String regionName, Set keySet, Object arguments,
      boolean optimizeForWrite, boolean isPostOperation) {
    super(Resource.DATA, OperationCode.EXECUTE_FUNCTION, regionName, isPostOperation);
    this.functionId = functionName;
    this.keySet = keySet;
    this.arguments = arguments;
    this.optimizeForWrite = optimizeForWrite;
  }

  public String getFunctionId() {
    return this.functionId;
  }

  public boolean isOptimizeForWrite() {
    return this.optimizeForWrite;
  }
  
  public Object getResult() {
    return this.result;
  }

  public Set getKeySet() {
    return this.keySet;
  }
  
  public Object getArguments() {
    return this.arguments;
  }

  public void setResult(Object oneResult) {
    this.result = oneResult;
  }
  
}
