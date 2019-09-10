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

import static org.apache.geode.management.configuration.MemberConfig.MEMBER_CONFIG_ENDPOINT;
import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;

import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementResult.StatusCode;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.runtime.MemberInformation;

@Controller("members")
@RequestMapping(MANAGEMENT_API_VERSION)
public class MemberManagementController extends AbstractManagementController {
  @ApiOperation(value = "get member")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = MEMBER_CONFIG_ENDPOINT + "/{id}")
  public ResponseEntity<ClusterManagementListResult<MemberConfig, MemberInformation>> getMember(
      @PathVariable(name = "id") String id) {
    MemberConfig config = new MemberConfig();
    config.setId(id);
    ClusterManagementListResult<MemberConfig, MemberInformation> result =
        clusterManagementService.list(config);
    if (result.getRuntimeResult().size() == 0) {
      throw new ClusterManagementException(new ClusterManagementResult(StatusCode.ENTITY_NOT_FOUND,
          "Member '" + config.getId() + "' does not exist."));
    }

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ApiOperation(value = "list members")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = MEMBER_CONFIG_ENDPOINT)
  public ResponseEntity<ClusterManagementListResult<MemberConfig, MemberInformation>> listMembers(
      @RequestParam(required = false) String id, @RequestParam(required = false) String group) {
    MemberConfig filter = new MemberConfig();
    if (StringUtils.isNotBlank(id)) {
      filter.setId(id);
    }
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    ClusterManagementListResult<MemberConfig, MemberInformation> result =
        clusterManagementService.list(filter);

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
  }

}
