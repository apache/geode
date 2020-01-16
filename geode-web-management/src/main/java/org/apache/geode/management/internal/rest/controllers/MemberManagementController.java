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
import static org.apache.geode.management.configuration.Member.MEMBER_ENDPOINT;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Extension;
import io.swagger.annotations.ExtensionProperty;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.configuration.Member;
import org.apache.geode.management.runtime.MemberInformation;

@RestController("members")
@RequestMapping(URI_VERSION)
public class MemberManagementController extends AbstractManagementController {
  @ApiOperation(value = "get member",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result | .configurationByGroup[] | .runtimeInfo[] | {name:.memberName,status:.status}")})})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(MEMBER_ENDPOINT + "/{id:.+}")
  public ClusterManagementGetResult<Member, MemberInformation> getMember(
      @PathVariable(name = "id") String id) {
    Member config = new Member();
    config.setId(id);
    return clusterManagementService.get(config);
  }

  @ApiOperation(value = "list members",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .runtimeInfo[] | {name:.memberName,status:.status}")})})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(MEMBER_ENDPOINT)
  public ClusterManagementListResult<Member, MemberInformation> listMembers(
      @RequestParam(required = false) String id, @RequestParam(required = false) String group) {
    Member filter = new Member();
    if (StringUtils.isNotBlank(id)) {
      filter.setId(id);
    }
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    return clusterManagementService.list(filter);
  }
}
