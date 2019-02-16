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
  private final Map<String, Status> memberStatuses;

  private final Status persistenceStatus;

  private final boolean successfullyAppliedOnMembers;

  private final boolean successfullyPersisted;

  private final boolean successful;

  // Required for deserialization
  private ClusterManagementResult() {
    memberStatuses = new HashMap<>();
    persistenceStatus = new Status(Status.Result.NOT_APPLICABLE, "");
    successful = false;
    successfullyPersisted = false;
    successfullyAppliedOnMembers = false;
  }

  protected ClusterManagementResult(Map<String, Status> memberStatuses, Status persistenceStatus,
      boolean successful, boolean successfullyAppliedOnMembers, boolean sucessfullyPersisted) {

    this.memberStatuses = memberStatuses;
    this.persistenceStatus = persistenceStatus;
    this.successfullyAppliedOnMembers = successfullyAppliedOnMembers;
    this.successfullyPersisted = sucessfullyPersisted;
    this.successful = successful;
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
