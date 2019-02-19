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

public class ClusterManagementResultFactory {
  private final Map<String, Status> memberStatuses = new HashMap<>();

  private Status persistenceStatus = new Status(Status.Result.NOT_APPLICABLE, null);

  public ClusterManagementResultFactory() {}

  public ClusterManagementResult createResult() {
    boolean successfullyPersisted = false;
    boolean successfullyAppliedOnMembers = false;
    boolean successful = false;


    return new ClusterManagementResult(memberStatuses, persistenceStatus, isSuccessful(),
        isSuccessfullyAppliedOnMembers(), isSuccessfullyPersisted());
  }

  public ClusterManagementResultFactory addMemberStatus(String member, boolean success,
      String message) {
    this.memberStatuses.put(member, new Status(success, message));
    return this;
  }

  public ClusterManagementResultFactory setPersistenceStatus(boolean success, String message) {
    this.persistenceStatus = new Status(success, message);
    return this;
  }

  public ClusterManagementResultFactory setPersistenceStatus(Status.Result status, String message) {
    this.persistenceStatus = new Status(status, message);
    return this;
  }

  public ClusterManagementResultFactory setPersistenceStatusNA(String message) {
    this.persistenceStatus = new Status(Status.Result.NOT_APPLICABLE, message);
    return this;
  }


  public boolean isSuccessfullyPersisted() {
    if (persistenceStatus.status == Status.Result.NO_OP) {
      return true;
    } else {
      return persistenceStatus.status == Status.Result.SUCCESS;
    }
  }

  public boolean isSuccessfullyAppliedOnMembers() {
    if (persistenceStatus.status == Status.Result.NO_OP) {
      return true;
    } else if (memberStatuses.isEmpty()) {
      return false;
    } else {
      return memberStatuses.values().stream().allMatch(x -> x.status == Status.Result.SUCCESS);
    }
  }

  public boolean isSuccessful() {
    if (persistenceStatus.status == Status.Result.NO_OP) {
      return true;
    } else {
      return (persistenceStatus.status == Status.Result.NOT_APPLICABLE || isSuccessfullyPersisted())
          && isSuccessfullyAppliedOnMembers();
    }

  }
}
