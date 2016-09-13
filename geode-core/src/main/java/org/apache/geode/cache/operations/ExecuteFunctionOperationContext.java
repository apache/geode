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

import java.io.Serializable;
import java.util.Set;

/**
 * OperationContext for Function execution operation. This is for the pre-operation case
 * 
 * @since GemFire 6.0
 *
 */
public class ExecuteFunctionOperationContext extends OperationContext {

  private String functionId;

  private String regionName;

  private boolean optizeForWrite;

  private boolean isPostOperation;
  
  private Set keySet;
  
  private Object arguments;

  private Object result;

  public ExecuteFunctionOperationContext(String functionName,
      String regionName, Set keySet, Object arguments,
      boolean optimizeForWrite, boolean isPostOperation) {
    this.functionId = functionName;
    this.regionName = regionName;
    this.keySet = keySet;
    this.arguments = arguments;
    this.optizeForWrite = optimizeForWrite;
    this.isPostOperation = isPostOperation;
  }

  @Override
  public OperationCode getOperationCode() {
    return OperationCode.EXECUTE_FUNCTION;
  }

  @Override
  public boolean isPostOperation() {
    return this.isPostOperation;
  }

  public String getFunctionId() {
    return this.functionId;
  }

  public String getRegionName() {
    return this.regionName;
  }

  public boolean isOptimizeForWrite() {
    return this.optizeForWrite;
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
  
  public void setIsPostOperation(boolean isPostOperation) {
    this.isPostOperation = isPostOperation;
  }
}
