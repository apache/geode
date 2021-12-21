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

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.distributed.DistributedMember;

public class InternalFunctionInvocationTargetException extends FunctionInvocationTargetException {
  private static final long serialVersionUID = -6063507496829271815L;

  private final Set<String> failedIds = new HashSet<String>();

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   *
   * @param cause a Throwable cause of this exception
   */
  public InternalFunctionInvocationTargetException(Throwable cause) {
    super(cause);
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   *
   * @param msg the error message
   */
  public InternalFunctionInvocationTargetException(String msg) {
    super(msg);
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   *
   * @param msg the error message
   * @param failedNode the failed node member
   */
  public InternalFunctionInvocationTargetException(String msg, DistributedMember failedNode) {
    super(msg, failedNode);
    failedIds.add(failedNode.getId());
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   *
   * @param msg the error message
   * @param failedNodeSet set of the failed node member id
   */
  public InternalFunctionInvocationTargetException(String msg, Set<String> failedNodeSet) {
    super(msg);
    failedIds.addAll(failedNodeSet);
  }

  /**
   * Construct an instance of InternalFunctionInvocationTargetException
   *
   * @param msg the error message
   * @param cause a Throwable cause of this exception
   */
  public InternalFunctionInvocationTargetException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public Set<String> getFailedNodeSet() {
    return failedIds;
  }

  public void setFailedNodeSet(Set<String> c) {
    failedIds.addAll(c);
  }
}
