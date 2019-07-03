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

package org.apache.geode.management.operation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.geode.annotations.Experimental;

/**
 * Encapsulate the result of running one of the possible Management service operations.
 */
@Experimental
public class OperationResult implements Cloneable {

  private String name;
  private Status status;
  private String message;
  private long startTime;
  private long endTime;

  public enum Status {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED
  }

  @JsonCreator
  public OperationResult(@JsonProperty("name") String name) {
    this.name = name;
    this.status = Status.PENDING;
  }

  private OperationResult(String name, Status status, long startTime, long endTime,
      String message) {
    this(name);
    this.status = status;
    this.startTime = startTime;
    this.endTime = endTime;
    this.message = message;
  }

  public String getName() {
    return name;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public OperationResult clone() {
    try {
      return (OperationResult) super.clone();
    } catch (CloneNotSupportedException e) {
      return new OperationResult(name, status, startTime, endTime, message);
    }
  }
}
