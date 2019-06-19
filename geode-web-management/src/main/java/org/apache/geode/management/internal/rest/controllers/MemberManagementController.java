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

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeMemberConfig;

@Controller("members")
@RequestMapping(MANAGEMENT_API_VERSION)
public class MemberManagementController extends AbstractManagementController {
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = MEMBER_CONFIG_ENDPOINT + "/{id}")
  public ResponseEntity<ClusterManagementResult<RuntimeMemberConfig>> getMember(
      @PathVariable(name = "id") String id) {
    MemberConfig config = new MemberConfig();
    config.setId(id);
    ClusterManagementResult<RuntimeMemberConfig> result = clusterManagementService.get(config);

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = MEMBER_CONFIG_ENDPOINT)
  public ResponseEntity<ClusterManagementResult<RuntimeMemberConfig>> listMembers(
      @RequestParam(required = false) String id, @RequestParam(required = false) String group) {
    MemberConfig filter = new MemberConfig();
    if (StringUtils.isNotBlank(id)) {
      filter.setId(id);
    }
    ClusterManagementResult<RuntimeMemberConfig> result = clusterManagementService.list(filter);

    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR);
  }

}
