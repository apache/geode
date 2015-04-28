/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.operations;

import java.io.Serializable;
import java.util.Set;

/**
 * OperationContext for Function execution operation. This is for the pre-operation case
 * 
 * @author Yogesh Mahajan
 * @since 6.0
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
