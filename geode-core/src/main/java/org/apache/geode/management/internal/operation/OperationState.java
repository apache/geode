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
import java.util.Objects;

import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

/**
 * Holds information describing the state of a particular operation.
 */
public class OperationState<A extends ClusterManagementOperation<V>, V extends OperationResult>
    implements Identifiable<String> {
  private static final long serialVersionUID = 8212319653561969588L;
  private final String opId;
  private final A operation;
  private final Date operationStart;
  private Date operationEnd;
  private V result;
  private Throwable throwable;
  private String locator;

  public String getLocator() {
    return locator;
  }

  public void setLocator(
      String locator) {
    synchronized (this) {
      this.locator = locator;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OperationState<?, ?> that = (OperationState<?, ?>) o;
    return Objects.equals(getId(), that.getId()) &&
        Objects.equals(getOperation(), that.getOperation()) &&
        Objects.equals(getOperationStart(), that.getOperationStart()) &&
        Objects.equals(getOperationEnd(), that.getOperationEnd()) &&
        Objects.equals(getResult(), that.getResult()) &&
        Objects.equals(getThrowable(), that.getThrowable()) &&
        Objects.equals(getLocator(), that.getLocator());
  }

  @Override
  public int hashCode() {
    return Objects.hash(opId);
  }

  public OperationState(String opId, A operation, Date operationStart) {
    this.opId = opId;
    this.operation = operation;
    this.operationStart = operationStart;
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

  public void setOperationEnd(Date operationEnd, V result, Throwable exception) {
    synchronized (this) {
      this.result = result;
      throwable = exception;
      this.operationEnd = operationEnd;
    }
  }

  /**
   * Creates and returns a copy of this operation state that will have
   * a consistent view of all the fields. In particular, it will have
   * all or nothing of fields that are being concurrently modified
   * when the copy is made.
   */
  OperationState<A, V> createCopy() {
    OperationState<A, V> result =
        new OperationState(opId, operation, operationStart);
    synchronized (this) {
      result.operationEnd = operationEnd;
      result.result = this.result;
      result.throwable = throwable;
      result.locator = locator;
    }
    return result;
  }

  public Date getOperationEnd() {
    return operationEnd;
  }

  public V getResult() {
    return result;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  @Override
  public String toString() {
    return "OperationState{" +
        "opId=" + opId +
        ", operation=" + operation +
        ", operationStart=" + operationStart +
        ", operationEnd=" + operationEnd +
        ", result=" + result +
        ", throwable=" + (throwable != null ? throwable.getMessage() : "null") +
        ", locator=" + locator +
        '}';
  }
}
