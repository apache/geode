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


public class APIResult {
  enum Result {
    SUCCESS, FAILURE, NOT_APPLICABLE
  }

  class Status {
    Result result;
    String message;

    public Status(Result result, String message) {
      this.result = result;
      this.message = message;
    }
  }

  private Map<String, Status> memberStatuses = new HashMap<>();
  private Status clusterConfigStatus = new Status(Result.NOT_APPLICABLE, null);

  public void addMemberStatus(String member, Result result, String message) {
    this.memberStatuses.put(member, new Status(result, message));
  }

  public void setClusterConfigPersisted(Result result, String message) {
    this.clusterConfigStatus = new Status(result, message);
  }

  public Map<String, Status> getMemberStatuses() {
    return memberStatuses;
  }

  public Status getClusterConfigStatus() {
    return clusterConfigStatus;
  }

  public boolean isSuccessfulOnDistributedMembers() {
    return memberStatuses.values().stream().allMatch(x -> x.result == Result.SUCCESS);
  }

  public boolean isSuccessfullyPersisted() {
    return clusterConfigStatus.result == Result.SUCCESS;
  }

  /**
   * - true if operation is successful on all distributed members,
   * and configuration persistence is either not applicable (in case cluster config is disabled)
   * or configuration persistence is applicable and successful
   * - false otherwise
   */
  public boolean isSuccessful() {
    return (clusterConfigStatus.result == Result.NOT_APPLICABLE || isSuccessfullyPersisted())
        && isSuccessfulOnDistributedMembers();
  }
}
