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

import static org.apache.geode.cache.configuration.RegionConfig.REGION_CONFIG_ENDPOINT;
import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementResult.StatusCode;
import org.apache.geode.management.api.ConfigurationResult;
import org.apache.geode.management.runtime.RuntimeInfo;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

@Controller("regionManagement")
@RequestMapping(MANAGEMENT_API_VERSION)
public class RegionManagementController extends AbstractManagementController {

  @ApiOperation(value = "create regions")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 409, message = "Region already exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.POST, value = REGION_CONFIG_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> createRegion(
      @RequestBody RegionConfig regionConfig) {
    ClusterManagementResult result =
        clusterManagementService.create(regionConfig);
    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.CREATED : HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = REGION_CONFIG_ENDPOINT)
  @ResponseBody
  public ClusterManagementListResult<RegionConfig, RuntimeRegionInfo> listRegion(
      @RequestParam(required = false) String id,
      @RequestParam(required = false) String group) {
    RegionConfig filter = new RegionConfig();
    if (StringUtils.isNotBlank(id)) {
      filter.setName(id);
    }
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    return clusterManagementService.list(filter);
  }

  @RequestMapping(method = RequestMethod.GET, value = REGION_CONFIG_ENDPOINT + "/{id}")
  @ResponseBody
  public ClusterManagementListResult<RegionConfig, RuntimeRegionInfo> getRegion(
      @PathVariable(name = "id") String id) {
    securityService.authorize(Resource.CLUSTER, Operation.READ, id);
    RegionConfig config = new RegionConfig();
    config.setName(id);
    return clusterManagementService.get(config);
  }

  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.DELETE, value = REGION_CONFIG_ENDPOINT + "/{id}")
  @ResponseBody
  public ClusterManagementResult deleteRegion(
      @PathVariable(name = "id") String id,
      @RequestParam(required = false) String group) {
    RegionConfig config = new RegionConfig();
    config.setName(id);
    if (StringUtils.isNotBlank(group)) {
      config.setGroup(group);
    }
    return clusterManagementService.delete(config);
  }

  @RequestMapping(method = RequestMethod.GET,
      value = REGION_CONFIG_ENDPOINT + "/{regionName}/indexes")
  @ResponseBody
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementListResult<RegionConfig.Index, RuntimeInfo> listIndex(
      @PathVariable String regionName,
      @RequestParam(required = false) String id) {

    ClusterManagementListResult<RegionConfig, RuntimeRegionInfo> result0 = getRegion(regionName);
    RegionConfig regionConfig = result0.getResult().get(0).getConfig();

    // only send the index information back
    List<RegionConfig.Index> indexList = regionConfig.getIndexes().stream().map(e -> {
      if (StringUtils.isNotBlank(id) && !e.getId().equals(id)) {
        return null;
      }
      e.setRegionName(regionName);
      return e;
    }).filter(Objects::nonNull).collect(Collectors.toList());

    List<ConfigurationResult<RegionConfig.Index, RuntimeInfo>> responses = new ArrayList<>();
    for (RegionConfig.Index index : indexList) {
      responses.add(new ConfigurationResult<>(index));
    }

    ClusterManagementListResult<RegionConfig.Index, RuntimeInfo> result =
        new ClusterManagementListResult<>();
    result.setResult(responses);
    return result;
  }

  @RequestMapping(method = RequestMethod.GET,
      value = REGION_CONFIG_ENDPOINT + "/{regionName}/indexes/{id}")
  @ResponseBody
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementListResult<RegionConfig.Index, RuntimeInfo> getIndex(
      @PathVariable String regionName,
      @PathVariable String id) {
    ClusterManagementListResult<RegionConfig.Index, RuntimeInfo> result = listIndex(regionName, id);
    List<ConfigurationResult<RegionConfig.Index, RuntimeInfo>> indexList = result.getResult();

    if (indexList.size() == 0) {
      throw new ClusterManagementException(
          new ClusterManagementResult(StatusCode.ENTITY_NOT_FOUND, "Index " + id + " not found."));
    }

    if (indexList.size() > 1) {
      throw new ClusterManagementException(
          new ClusterManagementResult(StatusCode.ERROR, "More than one entity found."));
    }

    result.setResult(indexList);
    return result;
  }
}
