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

package org.apache.geode.management.internal.rest.controllers;

import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;
import static org.apache.geode.management.operation.RebalanceOperation.REBALANCE_ENDPOINT;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.internal.ClusterManagementOperationStatusResult;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;

@Controller("rebalance")
@RequestMapping(MANAGEMENT_API_VERSION)
public class RebalanceController extends AbstractManagementController {
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.GET, value = REBALANCE_ENDPOINT + "/{id}")
  @ResponseBody
  public ClusterManagementOperationStatusResult<RebalanceResult> checkRebalanceStatus(
      @PathVariable String id) {
    return clusterManagementService.checkStatus(id);
  }

  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.POST, value = REBALANCE_ENDPOINT)
  public ResponseEntity<ClusterManagementOperationResult<RebalanceResult>> startRebalance(
      @RequestBody RebalanceOperation operation) {
    ClusterManagementOperationResult<RebalanceResult> result =
        clusterManagementService.startOperation(operation);

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.ACCEPTED : HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
