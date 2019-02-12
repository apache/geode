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

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.api.ClusterManagementResult;
import org.apache.geode.management.internal.api.Status;

@Controller("regionManagement")
@RequestMapping(MANAGEMENT_API_VERSION)
public class RegionManagementController extends AbstractManagementController {

  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.POST, value = "/regions")
  public ResponseEntity<ClusterManagementResult> createRegion(
      @RequestBody RegionConfig regionConfig) {
    ClusterManagementResult result =
        clusterManagementService.create(regionConfig, "cluster");

    HttpStatus httpStatus;
    Status.Result requestStatus = result.getPersistenceStatus().getStatus();
    if (requestStatus == Status.Result.NO_OP) {
      httpStatus = HttpStatus.OK;
    } else if (result.isSuccessful()) {
      httpStatus = HttpStatus.CREATED;
    } else {
      boolean condition = !result.isSuccessfullyAppliedOnMembers()
          && (result.getPersistenceStatus().getStatus() == Status.Result.NOT_APPLICABLE);
      httpStatus = (condition
          ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
    }

    return new ResponseEntity<>(result, httpStatus);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/ping")
  public ResponseEntity<String> ping() {
    return new ResponseEntity<>("pong", HttpStatus.OK);
  }
}
