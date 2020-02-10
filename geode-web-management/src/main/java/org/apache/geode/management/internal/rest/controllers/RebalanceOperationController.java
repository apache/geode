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

import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.operation.RebalanceOperation.REBALANCE_ENDPOINT;

import io.swagger.annotations.ApiOperation;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;

@RestController("rebalanceOperation")
@RequestMapping(URI_VERSION)
public class RebalanceOperationController extends AbstractManagementController {
  @ApiOperation(value = "start rebalance")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @PostMapping(REBALANCE_ENDPOINT)
  public ResponseEntity<ClusterManagementOperationResult<RebalanceResult>> startRebalance(
      @RequestBody RebalanceOperation operation) {
    ClusterManagementOperationResult<RebalanceResult> result =
        clusterManagementService
            .start(new RebalanceOperationWithOperator(operation, securityService));
    return new ResponseEntity<>(result, HttpStatus.ACCEPTED);
  }

  @ApiOperation(value = "list rebalances")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @GetMapping(REBALANCE_ENDPOINT)
  public ClusterManagementListOperationsResult<RebalanceResult> listRebalances() {
    return clusterManagementService.list(new RebalanceOperation());
  }

  @ApiOperation(value = "check rebalance")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @GetMapping(REBALANCE_ENDPOINT + "/{id:.+}")
  public ResponseEntity<ClusterManagementOperationResult<RebalanceResult>> checkRebalanceStatus(
      @PathVariable String id) {
    ClusterManagementOperationResult<RebalanceResult> result =
        clusterManagementService.checkStatus(new RebalanceOperation(), id);
    HttpHeaders headers = new HttpHeaders();
    headers.add("Retry-After", "30"); // TODO consider removing this
    return new ResponseEntity<>(result, headers, HttpStatus.OK);
  }
}
