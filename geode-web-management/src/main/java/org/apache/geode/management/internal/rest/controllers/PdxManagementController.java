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
import static org.apache.geode.management.configuration.Pdx.PDX_ENDPOINT;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.runtime.PdxInfo;

@RestController("pdxManagement")
@RequestMapping(URI_VERSION)
public class PdxManagementController extends AbstractManagementController {

  @Operation(summary = "configure pdx")
  @ApiResponses({
      @ApiResponse(responseCode = "400", description = "Bad request."),
      @ApiResponse(responseCode = "409", description = "Pdx already configured."),
      @ApiResponse(responseCode = "500", description = "Internal error.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @PostMapping(PDX_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> configurePdx(
      @RequestBody Pdx pdxType) {
    ClusterManagementResult result = clusterManagementService.create(pdxType);
    return new ResponseEntity<>(result, HttpStatus.CREATED);
  }

  @Operation(summary = "get pdx")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(PDX_ENDPOINT)
  public ClusterManagementGetResult<Pdx, PdxInfo> getPDX() {
    return clusterManagementService.get(new Pdx());
  }

  @Operation(summary = "update pdx")
  @ApiResponses({
      @ApiResponse(responseCode = "400", description = "Bad request."),
      @ApiResponse(responseCode = "500", description = "Internal error.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @PutMapping(PDX_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> updatePdx(@RequestBody Pdx pdxType) {
    ClusterManagementResult result = clusterManagementService.update(pdxType);

    return new ResponseEntity<>(result, HttpStatus.CREATED);
  }

  @Operation(summary = "delete pdx")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @DeleteMapping(PDX_ENDPOINT)
  public ClusterManagementResult deletePDX() {
    return clusterManagementService.delete(new Pdx());
  }
}
