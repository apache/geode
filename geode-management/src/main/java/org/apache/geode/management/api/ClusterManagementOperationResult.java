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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.internal.Dormant;
import org.apache.geode.management.runtime.OperationResult;

/**
 * This is normally returned by
 * {@link ClusterManagementService#start(ClusterManagementOperation)} to convey status of
 * launching the async operation, and if successful, the {@link CompletableFuture} to access the
 * status, result, and start/end times of the async operation.
 *
 * @param <V> the type of the operation's result
 */
@Experimental
public class ClusterManagementOperationResult<V extends OperationResult>
    extends ClusterManagementResult {
  @JsonIgnore
  private final CompletableFuture<V> operationResult;
  @JsonIgnore
  private final CompletableFuture<Date> futureOperationEnded;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  private Date operationStart;
  private String operationId;
  private String operator;

  /**
   * for internal use only
   */
  public ClusterManagementOperationResult() {
    this.operationResult = null;
    this.futureOperationEnded = null;
    this.operator = null;
  }

  /**
   * normally called by {@link ClusterManagementService#start(ClusterManagementOperation)}
   */
  public ClusterManagementOperationResult(ClusterManagementResult result,
      CompletableFuture<V> operationResult, Date operationStart,
      CompletableFuture<Date> futureOperationEnded, String operator, String operationId) {
    super(result);
    this.operationResult = operationResult;
    this.operationStart = operationStart;
    this.futureOperationEnded = futureOperationEnded;
    this.operator = operator;
    this.operationId = operationId;
  }

  /**
   * Returns the future result of the async operation
   */
  @JsonIgnore
  public CompletableFuture<V> getFutureResult() {
    if (operationResult instanceof Dormant) {
      ((Dormant) operationResult).wakeUp();
    }
    return operationResult;
  }

  /**
   * Returns the completed result of the async operation (blocks until complete, if necessary)
   */
  @JsonIgnore
  public V getResult() throws ExecutionException, InterruptedException {
    return getFutureResult().get();
  }

  /**
   * Returns the time at which the async operation was requested
   */
  public Date getOperationStart() {
    return operationStart;
  }

  /**
   * Returns the future time the async operation completed. This is guaranteed to complete before
   * {@link #getFutureResult()}. Note: subsequent stages must be chained to
   * {@link #getFutureResult()}, not here.
   */
  @JsonIgnore
  public CompletableFuture<Date> getFutureOperationEnded() {
    return futureOperationEnded;
  }

  /**
   * Returns the actual time the async operation completed, or null if not yet completed
   */
  @JsonProperty
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  public Date getOperationEnded() {
    return futureOperationEnded.getNow(null);
  }

  /**
   * Returns the user who initiated the async operation, if initiated externally and security is
   * enabled
   */
  public String getOperator() {
    return operator;
  }

  /**
   * returns the operation id started by this operation.
   */
  public String getOperationId() {
    return operationId;
  }
}
