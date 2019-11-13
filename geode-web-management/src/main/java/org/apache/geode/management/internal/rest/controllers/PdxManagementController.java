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

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
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

  @ApiOperation(value = "configure pdx")
  @ApiResponses({
      @ApiResponse(code = 400, message = "Bad request."),
      @ApiResponse(code = 409, message = "Pdx already configured."),
      @ApiResponse(code = 500, message = "Internal error.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @PostMapping(PDX_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> configurePdx(
      @RequestBody Pdx pdxType) {
    ClusterManagementResult result = clusterManagementService.create(pdxType);
    return new ResponseEntity<>(result, HttpStatus.CREATED);
  }

  @ApiOperation(value = "get pdx")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(PDX_ENDPOINT)
  public ClusterManagementGetResult<Pdx, PdxInfo> getPDX() {
    return clusterManagementService.get(new Pdx());
  }
}
