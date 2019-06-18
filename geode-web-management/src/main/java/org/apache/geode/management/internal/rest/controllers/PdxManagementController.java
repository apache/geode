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

import static org.apache.geode.cache.configuration.PdxType.PDX_ENDPOINT;
import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.management.api.ClusterManagementResult;

@Controller("pdxManagement")
@RequestMapping(MANAGEMENT_API_VERSION)
public class PdxManagementController extends AbstractManagementController {

  @ApiOperation(value = "configure PDX")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @RequestMapping(method = RequestMethod.POST, value = PDX_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> configurePdx(
      @RequestBody PdxType pdxType) {
    ClusterManagementResult<PdxType> result = clusterManagementService.create(pdxType);
    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.CREATED : HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
