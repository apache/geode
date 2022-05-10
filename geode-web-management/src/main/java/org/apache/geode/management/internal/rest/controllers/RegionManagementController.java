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

import static org.apache.geode.management.configuration.Index.INDEXES;
import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.configuration.Region.REGION_CONFIG_ENDPOINT;

import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
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
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.runtime.IndexInfo;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

@RestController("regionManagement")
@RequestMapping(URI_VERSION)
public class RegionManagementController extends AbstractManagementController {

  @io.swagger.v3.oas.annotations.Operation(summary = "create region")
  @ApiResponses({
      @ApiResponse(responseCode = "400", description = "Bad request."),
      @ApiResponse(responseCode = "409", description = "Region already exists."),
      @ApiResponse(responseCode = "500", description = "Internal error.")})
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @PostMapping(REGION_CONFIG_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> createRegion(
      @RequestBody Region regionConfig) {
    ClusterManagementResult result =
        clusterManagementService.create(regionConfig);
    return new ResponseEntity<>(result,
        HttpStatus.CREATED);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "list regions",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .groups[] | .runtimeInfo[] + .configuration | {name:.name,type:.type,entryCount:.entryCount}")})})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(REGION_CONFIG_ENDPOINT)
  public ClusterManagementListResult<Region, RuntimeRegionInfo> listRegion(
      @RequestParam(required = false) String id,
      @RequestParam(required = false) String group) {
    Region filter = new Region();
    if (StringUtils.isNotBlank(id)) {
      filter.setName(id);
    }
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    return clusterManagementService.list(filter);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "get region",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result | .groups[] | .runtimeInfo[] + .configuration | {name:.name,type:.type,entryCount:.entryCount}")})})
  @GetMapping(REGION_CONFIG_ENDPOINT + "/{id:.+}")
  public ClusterManagementGetResult<Region, RuntimeRegionInfo> getRegion(
      @PathVariable(name = "id") String id) {
    securityService.authorize(Resource.CLUSTER, Operation.READ, id);
    Region config = new Region();
    config.setName(id);
    return clusterManagementService.get(config);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "delete region")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @DeleteMapping(REGION_CONFIG_ENDPOINT + "/{id:.+}")
  public ClusterManagementResult deleteRegion(
      @PathVariable(name = "id") String id,
      @RequestParam(required = false) String group) {
    Region config = new Region();
    config.setName(id);
    if (StringUtils.isNotBlank(group)) {
      config.setGroup(group);
    }
    return clusterManagementService.delete(config);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "list region indexes",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .groups[] | .configuration | {name:.name,expression:.expression}")})})
  @GetMapping(REGION_CONFIG_ENDPOINT + "/{regionName}" + INDEXES)
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementListResult<Index, IndexInfo> listIndex(
      @PathVariable String regionName,
      @RequestParam(required = false, name = "id") String indexName) {

    Index filter = new Index();
    filter.setRegionPath(regionName);
    if (StringUtils.isNotBlank(indexName)) {
      filter.setName(indexName);
    }
    return clusterManagementService.list(filter);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "list indexes",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .groups[] | .configuration | {name:.name,expression:.expression,regionPath:.regionPath}")})})
  @GetMapping(INDEXES)
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementListResult<Index, IndexInfo> listAllIndex(
      @RequestParam(required = false, name = "id") String indexName) {
    Index filter = new Index();
    if (StringUtils.isNotBlank(indexName)) {
      filter.setName(indexName);
    }
    return clusterManagementService.list(filter);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "get index",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result | .groups[] | .configuration | {name:.name,expression:.expression}")})})
  @GetMapping(REGION_CONFIG_ENDPOINT + "/{regionName}" + INDEXES + "/{id:.+}")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementGetResult<Index, IndexInfo> getIndex(
      @PathVariable String regionName,
      @PathVariable String id) {

    Index filter = new Index();
    filter.setRegionPath(regionName);
    filter.setName(id);
    return clusterManagementService.get(filter);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "create index")
  @ApiResponses({
      @ApiResponse(responseCode = "400", description = "Bad request."),
      @ApiResponse(responseCode = "409", description = "Index already exists."),
      @ApiResponse(responseCode = "500", description = "Internal error.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE', 'QUERY')")
  @PostMapping(INDEXES)
  public ResponseEntity<ClusterManagementResult> createIndex(
      @RequestBody Index indexConfig) {
    ClusterManagementResult result =
        clusterManagementService.create(indexConfig);
    return new ResponseEntity<>(result,
        HttpStatus.CREATED);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "create region index")
  @ApiResponses({
      @ApiResponse(responseCode = "400", description = "Bad request."),
      @ApiResponse(responseCode = "409", description = "Index already exists."),
      @ApiResponse(responseCode = "500", description = "Internal error.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE', 'QUERY')")
  @PostMapping(REGION_CONFIG_ENDPOINT + "/{regionName}" + INDEXES)
  public ResponseEntity<ClusterManagementResult> createIndexOnRegion(
      @RequestBody Index indexConfig, @PathVariable String regionName) {
    if (indexConfig.getRegionName() == null) {
      indexConfig.setRegionPath(regionName);
    } else if (!regionName.equals(indexConfig.getRegionName())) {
      throw new IllegalArgumentException(
          "Region name in path must match Region name in configuration");
    }
    ClusterManagementResult result =
        clusterManagementService.create(indexConfig);
    return new ResponseEntity<>(result,
        HttpStatus.CREATED);
  }

  @io.swagger.v3.oas.annotations.Operation(summary = "delete region index")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE', 'QUERY')")
  @DeleteMapping(REGION_CONFIG_ENDPOINT + "/{regionName}" + INDEXES + "/{indexName:.+}")
  public ClusterManagementResult deleteIndex(
      @PathVariable String regionName,
      @PathVariable String indexName) {
    Index config = new Index();
    config.setName(indexName);
    config.setRegionPath(regionName);
    return clusterManagementService.delete(config);
  }
}
