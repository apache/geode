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

import java.util.List;

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
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.RuntimeIndex;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
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
  public ResponseEntity<ClusterManagementResult<RegionConfig>> createRegion(
      @RequestBody RegionConfig regionConfig) {
    ClusterManagementResult<RegionConfig> result =
        clusterManagementService.create(regionConfig);
    return new ResponseEntity<>(result,
        result.isSuccessful() ? HttpStatus.CREATED : HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = REGION_CONFIG_ENDPOINT)
  @ResponseBody
  public ClusterManagementResult<RuntimeRegionConfig> listRegion(
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
  public ClusterManagementResult<RuntimeRegionConfig> getRegion(
      @PathVariable(name = "id") String id) {
    securityService.authorize(Resource.CLUSTER, Operation.READ, id);
    RegionConfig config = new RegionConfig();
    config.setName(id);
    return clusterManagementService.get(config);
  }

  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.DELETE, value = REGION_CONFIG_ENDPOINT + "/{id}")
  @ResponseBody
  public ClusterManagementResult<RegionConfig> deleteRegion(
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
  public ClusterManagementResult<RuntimeIndex> listIndex(
      @PathVariable String regionName,
      @RequestParam(required = false) String id) {

    ClusterManagementResult<RuntimeRegionConfig> result0 = getRegion(regionName);
    RuntimeRegionConfig runtimeRegion = result0.getResult().get(0);

    // only send the index information back
    List<RuntimeIndex> runtimeIndexes = runtimeRegion.getRuntimeIndexes(id);
    ClusterManagementResult<RuntimeIndex> result = new ClusterManagementResult<>();
    result.setResult(runtimeIndexes);

    return result;
  }

  @RequestMapping(method = RequestMethod.GET,
      value = REGION_CONFIG_ENDPOINT + "/{regionName}/indexes/{id}")
  @ResponseBody
  public ClusterManagementResult<RuntimeIndex> getIndex(
      @PathVariable String regionName,
      @PathVariable String id) {
    ClusterManagementResult<RuntimeIndex> result = listIndex(regionName, id);
    List<RuntimeIndex> indexList = result.getResult();

    if (indexList.size() == 0) {
      throw new EntityNotFoundException("Index " + id + " not found.");
    }

    if (indexList.size() > 1) {
      throw new IllegalStateException("More than one entity found.");
    }

    result.setResult(indexList);
    return result;
  }
}
