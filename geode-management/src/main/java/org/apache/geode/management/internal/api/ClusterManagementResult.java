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

public class ClusterManagementResult {
  private Map<String, Status> memberStatuses = new HashMap<>();

  private Status persistenceStatus = new Status(Status.Result.NOT_APPLICABLE, null);

  private boolean successfullyAppliedOnMembers = false;

  private boolean successfullyPersisted = false;

  private boolean successful = false;

  public ClusterManagementResult() {
    generateSuccessStatus();
  }

  public ClusterManagementResult(boolean success, String message) {
    this.persistenceStatus = new Status(success, message);

    generateSuccessStatus();
  }

  public ClusterManagementResult(Status.Result status, String message) {
    this.persistenceStatus = new Status(status, message);

    generateSuccessStatus();
  }

  private void generateSuccessStatus() {
    if (persistenceStatus.status == Status.Result.NO_OP) {
      successfullyAppliedOnMembers = true;
    } else if (memberStatuses.isEmpty()) {
      successfullyAppliedOnMembers = false;
    } else {
      successfullyAppliedOnMembers =
          memberStatuses.values().stream().allMatch(x -> x.status == Status.Result.SUCCESS);
    }

    if (persistenceStatus.status == Status.Result.NO_OP) {
      successfullyPersisted = true;
    } else {
      successfullyPersisted = persistenceStatus.status == Status.Result.SUCCESS;
    }

    if (persistenceStatus.status == Status.Result.NO_OP) {
      successful = true;
    } else {
      successful =
          (persistenceStatus.status == Status.Result.NOT_APPLICABLE || isSuccessfullyPersisted())
              && isSuccessfullyAppliedOnMembers();
    }
  }

  public void addMemberStatus(String member, Status.Result result, String message) {
    this.memberStatuses.put(member, new Status(result, message));
    generateSuccessStatus();
  }

  public void addMemberStatus(String member, boolean success, String message) {
    this.memberStatuses.put(member, new Status(success, message));
    generateSuccessStatus();
  }

  public void setClusterConfigPersisted(boolean success, String message) {
    this.persistenceStatus = new Status(success, message);
    generateSuccessStatus();
  }

  public Map<String, Status> getMemberStatuses() {
    return memberStatuses;
  }

  public Status getPersistenceStatus() {
    return persistenceStatus;
  }

  public boolean isSuccessfullyAppliedOnMembers() {
    return successfullyAppliedOnMembers;
  }

  public boolean isSuccessfullyPersisted() {
    return successfullyPersisted;
  }

  /**
   * - true if operation is a NO_OP, is successful on all distributed members,
   * and configuration persistence is either not applicable (in case cluster config is disabled)
   * or configuration persistence is applicable and successful
   * - false otherwise
   */
  public boolean isSuccessful() {
    return successful;
  }
}
