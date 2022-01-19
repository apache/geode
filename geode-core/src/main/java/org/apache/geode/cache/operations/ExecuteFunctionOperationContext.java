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
package org.apache.geode.cache.operations;

import java.util.Set;

/**
 * OperationContext for Function execution operation. This is for the pre-operation case
 *
 * @since GemFire 6.0
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public class ExecuteFunctionOperationContext extends OperationContext {

  private final String functionId;

  private final String regionName;

  private final boolean optizeForWrite;

  private boolean isPostOperation;

  private final Set keySet;

  private final Object arguments;

  private Object result;

  public ExecuteFunctionOperationContext(String functionName, String regionName, Set keySet,
      Object arguments, boolean optimizeForWrite, boolean isPostOperation) {
    functionId = functionName;
    this.regionName = regionName;
    this.keySet = keySet;
    this.arguments = arguments;
    optizeForWrite = optimizeForWrite;
    this.isPostOperation = isPostOperation;
  }

  @Override
  public OperationCode getOperationCode() {
    return OperationCode.EXECUTE_FUNCTION;
  }

  @Override
  public boolean isPostOperation() {
    return isPostOperation;
  }

  public String getFunctionId() {
    return functionId;
  }

  public String getRegionName() {
    return regionName;
  }

  public boolean isOptimizeForWrite() {
    return optizeForWrite;
  }

  public Object getResult() {
    return result;
  }

  public Set getKeySet() {
    return keySet;
  }

  public Object getArguments() {
    return arguments;
  }

  public void setResult(Object oneResult) {
    result = oneResult;
  }

  public void setIsPostOperation(boolean isPostOperation) {
    this.isPostOperation = isPostOperation;
  }
}
