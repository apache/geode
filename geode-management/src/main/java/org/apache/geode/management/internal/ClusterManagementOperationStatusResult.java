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
package org.apache.geode.management.internal;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.runtime.OperationResult;

@Experimental
public class ClusterManagementOperationStatusResult<V extends OperationResult>
    extends ClusterManagementResult {
  public ClusterManagementOperationStatusResult() {}

  public ClusterManagementOperationStatusResult(ClusterManagementResult status) {
    super(status);
  }

  private V operationResult;
  private String operator;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  private Date operationStart;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  private Date operationEnded;

  public V getResult() {
    return operationResult;
  }

  public void setResult(V operationResult) {
    this.operationResult = operationResult;
  }

  public Date getOperationStart() {
    return operationStart;
  }

  public void setOperationStart(Date operationStart) {
    this.operationStart = operationStart;
  }

  public Date getOperationEnded() {
    return operationEnded;
  }

  public void setOperationEnded(Date operationEnded) {
    this.operationEnded = operationEnded;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }
}
