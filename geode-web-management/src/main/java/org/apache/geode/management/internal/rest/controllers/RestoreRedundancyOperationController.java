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
 *
 */

package org.apache.geode.management.internal.rest.controllers;

import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.operation.RestoreRedundancyRequest.RESTORE_REDUNDANCY_ENDPOINT;

import java.util.Optional;

import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

@RestController("restoreRedundancyOperation")
@RequestMapping(URI_VERSION)
public class RestoreRedundancyOperationController extends AbstractManagementController {
  @Operation(summary = "start restore-redundancy")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @PostMapping(RESTORE_REDUNDANCY_ENDPOINT)
  public ResponseEntity<ClusterManagementOperationResult<RestoreRedundancyRequest, RestoreRedundancyResults>> startRestoreRedundancy(
      @RequestBody RestoreRedundancyRequest operation) {
    operation.setOperator(
        Optional.ofNullable(securityService).map(SecurityService::getSubject).map(Object::toString)
            .orElse(null));
    ClusterManagementOperationResult<RestoreRedundancyRequest, RestoreRedundancyResults> result =
        clusterManagementService
            .start(operation);
    return new ResponseEntity<>(result, HttpStatus.ACCEPTED);
  }

  @Operation(summary = "list restore-redundancy")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @GetMapping(RESTORE_REDUNDANCY_ENDPOINT)
  public ClusterManagementListOperationsResult<RestoreRedundancyRequest, RestoreRedundancyResults> listRestoreRedundancies() {
    return clusterManagementService.list(new RestoreRedundancyRequest());
  }

  @Operation(summary = "get restore-redundancy")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @GetMapping(RESTORE_REDUNDANCY_ENDPOINT + "/{id:.+}")
  public ClusterManagementOperationResult<RestoreRedundancyRequest, RestoreRedundancyResults> getRestoreRedundancy(
      @PathVariable String id) {
    ClusterManagementOperationResult<RestoreRedundancyRequest, RestoreRedundancyResults> result =
        clusterManagementService.get(new RestoreRedundancyRequest(), id);
    return result;
  }
}
