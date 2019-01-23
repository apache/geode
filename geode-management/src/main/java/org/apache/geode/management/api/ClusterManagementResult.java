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

import org.apache.geode.annotations.Experimental;

/**
 * Represents the result of a ClusterManagementService operation. Note: only to be used in that
 * context.
 */
@Experimental
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

  public void setClusterConfigPersisted(Status.Result result, String message) {
    this.persistenceStatus = new Status(result, message);
  }

  public void setClusterConfigPersisted(boolean success, String message) {
    this.persistenceStatus = new Status(success, message);
  }

  /**
   * Status of operation on each member
   *
   * @return Map with keys as member names, values as Status objects representing
   *         the results (and error messages if any) of running the operation on each member
   */
  public Map<String, Status> getMemberStatuses() {
    return memberStatuses;
  }

  /**
   * Status of cluster config persistence.
   *
   * @return Status object representing result (and error message if any) of persisting
   *         the cluster config
   */
  public Status getPersistenceStatus() {
    return persistenceStatus;
  }

  /**
   * Represents success of operation on all distributed members
   *
   * @return true if operation is successful on all members, false if not.
   */
  @JsonIgnore
  public boolean isSuccessfullyAppliedOnMembers() {
    return memberStatuses.values().stream().allMatch(x -> x.result == Status.Result.SUCCESS);
  }

  /**
   * Represents successful persistence of cluster config after the relevant operation.
   *
   * @return true if cluster config is successfully persisted, false otherwise
   */
  @JsonIgnore
  public boolean isSuccessfullyPersisted() {
    return persistenceStatus.result == Status.Result.SUCCESS;
  }

  /**
   * Represents success of the ClusterManagementResult
   *
   * @return true if operation is successful on all distributed members, and configuration
   *         persistence
   *         is either not applicable (i.e. cluster config disabled), or configuration persistence
   *         is applicable
   *         and successful. false otherwise.
   */
  @JsonIgnore
  public boolean isSuccessful() {
    return (persistenceStatus.result == Status.Result.NOT_APPLICABLE || isSuccessfullyPersisted())
        && isSuccessfullyAppliedOnMembers();
  }
}
