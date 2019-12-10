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

import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;

@RestController("deploymentManagement")
@RequestMapping(URI_VERSION)
public class DeploymentManagementController extends AbstractManagementController {

  @ApiOperation(value = "list deployed")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(Deployment.DEPLOYMENT_ENDPOINT)
  public ClusterManagementListResult<Deployment, DeploymentInfo> list(
      @RequestParam(required = false) String id,
      @RequestParam(required = false) String group) {
    Deployment deployment = new Deployment();
    if (StringUtils.isNotBlank(id)) {
      deployment.setJarFileName(id);
    }
    if (StringUtils.isNotBlank(group)) {
      deployment.setGroup(group);
    }
    return clusterManagementService.list(deployment);
  }
}
