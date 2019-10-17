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

import static org.apache.geode.management.configuration.GatewayReceiver.GATEWAY_RECEIVERS_ENDPOINTS;
import static org.apache.geode.management.configuration.Links.URI_VERSION;

import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.runtime.GatewayReceiverInfo;


@RestController("gatewayManagement")
@RequestMapping(URI_VERSION)
public class GatewayManagementController extends AbstractManagementController {
  @ApiOperation(value = "list gateway-receivers")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(GATEWAY_RECEIVERS_ENDPOINTS)
  public ClusterManagementListResult<GatewayReceiver, GatewayReceiverInfo> listGatewayReceivers(
      @RequestParam(required = false) String group) {
    GatewayReceiver filter = new GatewayReceiver();
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    return clusterManagementService.list(filter);
  }

  @ApiOperation(value = "get gateway-receiver")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(GATEWAY_RECEIVERS_ENDPOINTS + "/{id}")
  public ClusterManagementGetResult<GatewayReceiver, GatewayReceiverInfo> getGatewayReceiver(
      @PathVariable(name = "id") String id) {
    GatewayReceiver config = new GatewayReceiver();
    config.setGroup(id);
    return clusterManagementService.get(config);
  }

  @ApiOperation(value = "create gateway-receiver")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @PostMapping(GATEWAY_RECEIVERS_ENDPOINTS)
  public ResponseEntity<ClusterManagementResult> createGatewayReceiver(
      @RequestBody GatewayReceiver gatewayReceiverConfig) {
    ClusterManagementResult result = clusterManagementService.create(gatewayReceiverConfig);
    return new ResponseEntity<>(result, HttpStatus.CREATED);
  }
}
