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


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;

@Experimental
public class ClusterManagementRealizationResult extends ClusterManagementResult {
  private final List<RealizationResult> memberStatuses = new ArrayList<>();

  /**
   * for internal use only
   */
  public ClusterManagementRealizationResult() {}

  /**
   * for internal use only
   */
  public ClusterManagementRealizationResult(StatusCode statusCode, String message) {
    super(statusCode, message);
  }

  /**
   * for internal use only
   */
  public void addMemberStatus(RealizationResult result) {
    memberStatuses.add(result);
    // if any member failed, status code will be error
    if (!result.isSuccess()) {
      setStatus(StatusCode.ERROR, "");
    }
  }

  /**
   * For a {@link ClusterManagementService#create(AbstractConfiguration)} operation, this will
   * return
   * per-member status of the create.
   */
  public List<RealizationResult> getMemberStatuses() {
    return memberStatuses;
  }

  @Override
  public String toString() {
    return super.toString() + " " + StringUtils.join(memberStatuses, "; ");
  }
}
