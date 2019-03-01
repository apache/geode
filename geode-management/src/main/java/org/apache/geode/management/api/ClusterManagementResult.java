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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class ClusterManagementResult {
  // this error code should have a one-to-one mapping to the http status code returned
  // by the controller
  public enum StatusCode {
    OK, ERROR, UNAUTHORIZED, UNAUTHENTICATED, ENTITY_EXISTS, ILLEGAL_ARGUMENT
  }

  private Map<String, Status> memberStatuses = new HashMap<>();

  // we will always have statusCode when the object is created
  private StatusCode statusCode = StatusCode.OK;
  private Status persistenceStatus = new Status();

  public ClusterManagementResult() {}

  public ClusterManagementResult(boolean success, String message) {
    setPersistenceStatus(success, message);
  }

  public ClusterManagementResult(StatusCode statusCode, String message) {
    this.statusCode = statusCode;
    this.persistenceStatus = new Status(statusCode == StatusCode.OK, message);
  }

  public void addMemberStatus(String member, boolean success, String message) {
    this.memberStatuses.put(member, new Status(success, message));
    // if any member failed, status code will be error
    if (!success) {
      statusCode = StatusCode.ERROR;
    }
  }

  public void setPersistenceStatus(boolean success, String message) {
    // if failed to persist, status code will be error
    if (!success) {
      statusCode = StatusCode.ERROR;
    }
    this.persistenceStatus = new Status(success, message);
  }

  public Map<String, Status> getMemberStatuses() {
    return memberStatuses;
  }

  public Status getPersistenceStatus() {
    return persistenceStatus;
  }

  @JsonIgnore
  public boolean isSuccessful() {
    return statusCode == StatusCode.OK;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }
}
