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
 *
 */

package org.apache.geode.management.internal.operation;

import java.util.Date;

import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

/**
 * Holds information describing the state of a particular operation.
 */
public class OperationState<A extends ClusterManagementOperation<V>, V extends OperationResult>
    implements Identifiable<String> {
  private final String opId;
  private final A operation;
  private final String operator;
  private final Date operationStart;
  private Date operationEnd;
  private V result;
  private Throwable throwable;

  public OperationState(String opId, A operation, Date operationStart) {
    this.opId = opId;
    this.operation = operation;
    this.operationStart = operationStart;
    if (operation instanceof TaggedWithOperator) {
      this.operator = ((TaggedWithOperator) operation).getOperator();
    } else {
      this.operator = null;
    }
  }

  @Override
  public String getId() {
    return opId;
  }

  public A getOperation() {
    return operation;
  }

  public Date getOperationStart() {
    return operationStart;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperationEnd(Date operationEnd, V result, Throwable exception) {
    this.result = result;
    this.throwable = exception;
    this.operationEnd = operationEnd;
  }

  public Date getOperationEnd() {
    return this.operationEnd;
  }

  public V getResult() {
    return this.result;
  }

  public Throwable getThrowable() {
    return this.throwable;
  }
}
