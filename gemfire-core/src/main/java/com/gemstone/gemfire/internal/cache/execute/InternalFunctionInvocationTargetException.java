/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.distributed.DistributedMember;

public class InternalFunctionInvocationTargetException extends
    FunctionInvocationTargetException {

  private final Set<String> failedIds = new HashSet<String>();
  
  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   * 
   * @param cause
   *                a Throwable cause of this exception
   */
  public InternalFunctionInvocationTargetException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   */
  public InternalFunctionInvocationTargetException(String msg) {
    super(msg);
  }
  
  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   * @param failedNode
   *                the failed node member
   */
  public InternalFunctionInvocationTargetException(String msg,
      DistributedMember failedNode) {
    super(msg, failedNode);
    this.failedIds.add(failedNode.getId());
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   * 
   * @param msg  
   *           the error message
   * @param failedNodeSet 
   *           set of the failed node member id
   */
  public InternalFunctionInvocationTargetException(String msg, Set<String> failedNodeSet) {
    super(msg);
    this.failedIds.addAll(failedNodeSet);
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   * 
   * @param msg
   *                the error message
   * @param cause
   *                a Throwable cause of this exception
   */
  public InternalFunctionInvocationTargetException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public Set<String> getFailedNodeSet() {
    return this.failedIds;
  }

  public void setFailedNodeSet(Set<String> c) {
    this.failedIds.addAll(c);
  }
}
