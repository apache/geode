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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.internal.Dormant;

/**
 * This is normally returned by
 * {@link ClusterManagementService#startOperation(ClusterManagementOperation)} to convey status of
 * launching the async operation, and if successful, the {@link CompletableFuture} to access the
 * status, result, and start/end times of the async operation.
 *
 * @param <V> the type of the operation's result
 */
@Experimental
public class ClusterManagementOperationResult<V extends JsonSerializable>
    extends ClusterManagementResult {
  @JsonIgnore
  private final CompletableFuture<V> operationResult;
  @JsonIgnore
  private final CompletableFuture<Date> operationEnd;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  private Date operationStart;

  public ClusterManagementOperationResult() {
    this.operationResult = null;
    this.operationEnd = null;
  }

  /**
   * normally called by {@link ClusterManagementService#startOperation(ClusterManagementOperation)}
   */
  public ClusterManagementOperationResult(ClusterManagementResult result,
      CompletableFuture<V> operationResult, Date operationStart,
      CompletableFuture<Date> operationEnd) {
    super(result);
    this.operationResult = operationResult;
    this.operationStart = operationStart;
    this.operationEnd = operationEnd;
  }

  /**
   * @return the future result of the async operation
   */
  @JsonIgnore
  public CompletableFuture<V> getResult() {
    if (operationResult instanceof Dormant)
      ((Dormant) operationResult).wakeUp();
    return operationResult;
  }

  /**
   * @return the time the async operation was requested
   */
  public Date getOperationStart() {
    return operationStart;
  }

  /**
   * @return the future time the async operation completed. This is guaranteed to compete
   *         immediately before {@link #getResult()}; any subsequent stages must be chained to
   *         {@link #getResult()}, not here.
   */
  public CompletableFuture<Date> getOperationEnd() {
    return operationEnd;
  }
}
