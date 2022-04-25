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
package org.apache.geode.management.api;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.runtime.OperationResult;

/**
 * Returned by
 * {@link ClusterManagementService#start(ClusterManagementOperation)} to convey status of
 * launching the async operation,
 * and by {@link ClusterManagementService#get(ClusterManagementOperation, String)} to
 * describe the status of
 * a started async operation.
 *
 * @param <V> the type of the operation's result
 */
@Experimental
public class ClusterManagementOperationResult<A extends ClusterManagementOperation<V>, V extends OperationResult>
    extends ClusterManagementResult {

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  private Date operationStart;
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  private volatile Date operationEnd;
  private String operationId;
  private A operation;
  private V operationResult;
  private Throwable throwable;

  /**
   * for internal use only
   */
  public ClusterManagementOperationResult() {}

  /**
   * normally called by {@link ClusterManagementService#start(ClusterManagementOperation)}
   *
   * @param statusCode the {@code StatusCode} of the result
   * @param message the status message to set
   * @param operationStart a {@link Date} representing the time the operation started
   * @param operationEnd a {@link Date} representing the time the operation ended
   * @param operation the operation
   * @param operationId the ID of the operation
   * @param operationResult the operation's result
   * @param throwable an exception that occurred as a result of the operation, if any
   */
  public ClusterManagementOperationResult(StatusCode statusCode, String message,
      Date operationStart, Date operationEnd,
      A operation, String operationId, V operationResult,
      Throwable throwable) {
    super(statusCode, message);
    this.operationStart = operationStart;
    this.operationEnd = operationEnd;
    this.operation = operation;
    this.operationId = operationId;
    this.operationResult = operationResult;
    this.throwable = throwable;
  }

  /**
   * Returns the async operation.
   *
   * @return the operation that this {@link ClusterManagementOperationResult} describes
   */
  public A getOperation() {
    return operation;
  }

  /**
   * returns the operation id started by this operation.
   *
   * @return the operation ID of the operation that this {@link ClusterManagementOperationResult}
   *         describes
   */
  public String getOperationId() {
    return operationId;
  }

  /**
   * Returns the time the operation was started
   *
   * @return the time the operation was started
   */
  public Date getOperationStart() {
    return operationStart;
  }

  /**
   * Returns the time the operation was completed. This value is null while the operation is in
   * process.
   *
   * @return the time at which the operation completed, or null if the operation has not completed
   */
  public Date getOperationEnd() {
    return operationEnd;
  }

  /**
   * Returns the operation result as an extension of {@link OperationResult}
   *
   * @return the result of the operation
   */
  public V getOperationResult() {
    return operationResult;
  }

  /**
   * Returns any exceptions that might be returned as a result of the operation. Null in case
   * no exceptions occurred.
   *
   * @return any exceptions that might be returned as a result of the operation
   */
  public Throwable getThrowable() {
    return throwable;
  }
}
