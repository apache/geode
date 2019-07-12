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

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceInfo;

@Controller("rebalance")
@RequestMapping(MANAGEMENT_API_VERSION)
public class RebalanceController extends AbstractManagementController {
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.GET, value = REBALANCE_ENDPOINT + "/{id}")
  public ResponseEntity<ClusterManagementOperationResult<RebalanceInfo>> checkRebalanceStatus(
      @PathVariable(name = "id") String id) {
    ClusterManagementOperationResult<RebalanceInfo> result =
        clusterManagementService.checkStatus(id);

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.GET, value = REBALANCE_ENDPOINT)
  public ResponseEntity<ClusterManagementOperationResult<RebalanceInfo>> startRebalance(
      @RequestParam(required = false) List<String> includeRegions,
      @RequestParam(required = false) List<String> excludeRegions) {
    RebalanceOperation rebalance = new RebalanceOperation();
    rebalance.setIncludeRegions(includeRegions);
    rebalance.setExcludeRegions(excludeRegions);
    ClusterManagementOperationResult<RebalanceInfo> result =
        clusterManagementService.startOperation(rebalance);

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
  }

}
