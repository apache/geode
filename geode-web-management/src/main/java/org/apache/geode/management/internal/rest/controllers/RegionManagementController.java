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

import static org.apache.geode.management.api.Links.SELF;
import static org.apache.geode.management.api.RestfulEndpoint.URI_VERSION;
import static org.apache.geode.management.configuration.Region.REGION_CONFIG_ENDPOINT;

import java.util.Map;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Extension;
import io.swagger.annotations.ExtensionProperty;
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

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ConfigurationResult;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.runtime.RuntimeInfo;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

@Controller("regionManagement")
@RequestMapping(URI_VERSION)
public class RegionManagementController extends AbstractManagementController {
  private static final String INDEXES = "/indexes";
  private static final String DISKSTORES = "/diskstores";

  @ApiOperation(value = "create region")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 409, message = "Region already exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.POST, value = REGION_CONFIG_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> createRegion(
      @RequestBody Region regionConfig) {
    ClusterManagementResult result =
        clusterManagementService.create(regionConfig);
    return new ResponseEntity<>(result,
        HttpStatus.CREATED);
  }

  @ApiOperation(value = "list regions",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .runtimeInfo[] + .configuration | {name:.name,type:.type,entryCount:.entryCount}")})})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = REGION_CONFIG_ENDPOINT)
  @ResponseBody
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
    ClusterManagementListResult<Region, RuntimeRegionInfo> list =
        clusterManagementService.list(filter);
    list.getResult().forEach(r -> addBiDirectionalDownLinks(r.getLinks(), INDEXES, DISKSTORES));
    return list;
  }

  @ApiOperation(value = "get region",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result | .runtimeInfo[] + .configuration | {name:.name,type:.type,entryCount:.entryCount}")})})
  @RequestMapping(method = RequestMethod.GET, value = REGION_CONFIG_ENDPOINT + "/{id}")
  @ResponseBody
  public ClusterManagementGetResult<Region, RuntimeRegionInfo> getRegion(
      @PathVariable(name = "id") String id) {
    securityService.authorize(Resource.CLUSTER, Operation.READ, id);
    Region config = new Region();
    config.setName(id);
    return addBiDirectionalDownLinks(clusterManagementService.get(config), INDEXES, DISKSTORES);
  }

  @ApiOperation(value = "delete region")
  @PreAuthorize("@securityService.authorize('DATA', 'MANAGE')")
  @RequestMapping(method = RequestMethod.DELETE, value = REGION_CONFIG_ENDPOINT + "/{id}")
  @ResponseBody
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

  @ApiOperation(value = "list region indexes",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .configuration | {name:.name,expression:.expression}")})})
  @RequestMapping(method = RequestMethod.GET,
      value = REGION_CONFIG_ENDPOINT + "/{regionName}" + INDEXES)
  @ResponseBody
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementListResult<Index, RuntimeInfo> listIndex(
      @PathVariable String regionName,
      @RequestParam(required = false, name = "id") String indexName) {

    Index filter = new Index();
    filter.setRegionPath(regionName);
    if (StringUtils.isNotBlank(indexName)) {
      filter.setName(indexName);
    }
    return addUplinks(clusterManagementService.list(filter));
  }

  @ApiOperation(value = "list indexes",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .configuration | {name:.name,expression:.expression,regionPath:.regionPath}")})})
  @RequestMapping(method = RequestMethod.GET, value = INDEXES)
  @ResponseBody
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementListResult<Index, RuntimeInfo> listAllIndex(
      @RequestParam(required = false, name = "id") String indexName) {
    Index filter = new Index();
    if (StringUtils.isNotBlank(indexName)) {
      filter.setName(indexName);
    }
    return addUplinks(clusterManagementService.list(filter));
  }

  @ApiOperation(value = "get index",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result | .configuration | {name:.name,expression:.expression}")})})
  @RequestMapping(method = RequestMethod.GET,
      value = REGION_CONFIG_ENDPOINT + "/{regionName}" + INDEXES + "/{id}")
  @ResponseBody
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ', 'QUERY')")
  public ClusterManagementGetResult<Index, RuntimeInfo> getIndex(
      @PathVariable String regionName,
      @PathVariable String id) {

    Index filter = new Index();
    filter.setRegionPath(regionName);
    filter.setName(id);
    return addBiDirectionalUpLink(clusterManagementService.get(filter));
  }

  private static <R extends ClusterManagementGetResult> R addBiDirectionalDownLinks(R ret,
      String... subs) {
    ConfigurationResult<?, ?> result = ret.getResult();
    addBiDirectionalDownLinks(result.getLinks(), subs);
    return ret;
  }

  private static void addBiDirectionalDownLinks(Map<String, String> links, String... subs) {
    for (String subCollection : subs) {
      links.put(subCollection.replace("/", ""), links.get(SELF) + subCollection);
    }
  }

  private static <R extends ClusterManagementGetResult> R addBiDirectionalUpLink(R ret) {
    ConfigurationResult<?, ?> result = ret.getResult();
    addBiDirectionalUpLink(result.getLinks());
    return ret;
  }

  private static void addBiDirectionalUpLink(Map<String, String> links) {
    String parent = links.get(SELF).replaceFirst("/[^/]*/[^/*]*$", "");
    String type = parent.replaceFirst("s/[^/]*$", "").replaceFirst(".*/", "");
    links.put(type, parent);
  }

  private static ClusterManagementListResult<Index, RuntimeInfo> addUplinks(
      ClusterManagementListResult<Index, RuntimeInfo> list) {
    list.getResult().forEach(r -> addBiDirectionalUpLink(r.getLinks()));
    return list;
  }
}
