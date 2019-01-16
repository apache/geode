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
package org.apache.geode.management.internal.api;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class ClusterManagementResult {
  private Map<String, Status> memberStatuses = new HashMap<>();

  private Status persistenceStatus = new Status(Status.Result.NOT_APPLICABLE, null);

  public ClusterManagementResult() {}

  public ClusterManagementResult(boolean success, String message) {
    this.persistenceStatus = new Status(success, message);
  }

  public void addMemberStatus(String member, Status.Result result, String message) {
    this.memberStatuses.put(member, new Status(result, message));
  }

  public void addMemberStatus(String member, boolean success, String message) {
    this.memberStatuses.put(member, new Status(success, message));
  }

  public void setClusterConfigPersisted(boolean success, String message) {
    this.persistenceStatus = new Status(success, message);
  }

  public Map<String, Status> getMemberStatuses() {
    return memberStatuses;
  }

  public Status getPersistenceStatus() {
    return persistenceStatus;
  }

  @JsonIgnore
  public boolean isSuccessfullyAppliedOnMembers() {
    if (memberStatuses.isEmpty()) {
      return false;
    }
    return memberStatuses.values().stream().allMatch(x -> x.status == Status.Result.SUCCESS);
  }

  @JsonIgnore
  public boolean isSuccessfullyPersisted() {
    return persistenceStatus.status == Status.Result.SUCCESS;
  }

  /**
   * - true if operation is successful on all distributed members,
   * and configuration persistence is either not applicable (in case cluster config is disabled)
   * or configuration persistence is applicable and successful
   * - false otherwise
   */
  @JsonIgnore
  public boolean isSuccessful() {
    return (persistenceStatus.status == Status.Result.NOT_APPLICABLE || isSuccessfullyPersisted())
        && isSuccessfullyAppliedOnMembers();
  }

}
