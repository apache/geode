/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.management.internal.rest.controllers;

import static org.apache.geode.management.configuration.DiskStore.DISK_STORE_CONFIG_ENDPOINT;
import static org.apache.geode.management.configuration.Links.URI_VERSION;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Extension;
import io.swagger.annotations.ExtensionProperty;
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
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.runtime.DiskStoreInfo;
import org.apache.geode.security.ResourcePermission;

@RestController("diskStoreManagement")
@RequestMapping(URI_VERSION)
public class DiskStoreController extends AbstractManagementController {

  @ApiOperation(value = "create disk-store")
  @ApiResponses({
      @ApiResponse(code = 400, message = "Bad request."),
      @ApiResponse(code = 409, message = "Diskstore already exists."),
      @ApiResponse(code = 500, message = "Internal error.")})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @PostMapping(DISK_STORE_CONFIG_ENDPOINT)
  public ResponseEntity<ClusterManagementResult> createDiskStore(
      @RequestBody DiskStore diskStoreConfig) {
    ClusterManagementResult result = clusterManagementService.create(diskStoreConfig);
    return new ResponseEntity<>(result, HttpStatus.CREATED);
  }

  @ApiOperation(value = "list disk-stores",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .groups[] | .runtimeInfo[] + .configuration | {Member:.memberName,\"Disk Store Name\":.name}")})})
  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @GetMapping(DISK_STORE_CONFIG_ENDPOINT)
  public ClusterManagementListResult<DiskStore, DiskStoreInfo> listDiskStores(
      @RequestParam(required = false) String id,
      @RequestParam(required = false) String group) {
    DiskStore filter = new DiskStore();
    if (StringUtils.isNotBlank(id)) {
      filter.setName(id);
    }
    if (StringUtils.isNotBlank(group)) {
      filter.setGroup(group);
    }
    return clusterManagementService.list(filter);
  }

  @ApiOperation(value = "get disk-store",
      extensions = {@Extension(properties = {
          @ExtensionProperty(name = "jqFilter",
              value = ".result[] | .groups[] | .runtimeInfo[] + .configuration | {Member:.memberName,\"Disk Store Name\":.name}")})})
  @GetMapping(DISK_STORE_CONFIG_ENDPOINT + "/{id}")
  public ClusterManagementGetResult<DiskStore, DiskStoreInfo> getDiskStore(
      @PathVariable(name = "id") String id) {
    securityService.authorize(ResourcePermission.Resource.CLUSTER,
        ResourcePermission.Operation.READ, id);
    DiskStore config = new DiskStore();
    config.setName(id);
    return clusterManagementService.get(config);
  }

  @ApiOperation(value = "delete disk-store")
  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @DeleteMapping(DISK_STORE_CONFIG_ENDPOINT + "/{id}")
  public ClusterManagementResult deleteDiskStore(
      @PathVariable(name = "id") String id,
      @RequestParam(required = false) String group) {
    DiskStore config = new DiskStore();
    config.setName(id);
    if (StringUtils.isNotBlank(group)) {
      config.setGroup(group);
    }
    return clusterManagementService.delete(config);
  }
}
