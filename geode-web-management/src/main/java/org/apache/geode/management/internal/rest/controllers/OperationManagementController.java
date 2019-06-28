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

import static org.apache.geode.management.internal.rest.controllers.AbstractManagementController.MANAGEMENT_API_VERSION;

import java.util.Map;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import org.apache.geode.management.operation.OperationResult;

@Controller("operationManagement")
@RequestMapping(MANAGEMENT_API_VERSION)
public class OperationManagementController extends AbstractManagementController {

  @PreAuthorize("@securityService.authorize('CLUSTER', 'READ')")
  @RequestMapping(method = RequestMethod.GET, value = "/operations/{operation}")
  @ResponseBody
  public ResponseEntity<OperationResult> queryOperation(
      @PathVariable String operation) {

    OperationResult operationResult = clusterManagementService.getOperationResult(operation);
    if (operationResult == null) {
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    return new ResponseEntity<>(operationResult, HttpStatus.OK);
  }

  @PreAuthorize("@securityService.authorize('CLUSTER', 'MANAGE')")
  @RequestMapping(method = RequestMethod.POST, value = "/operations/{operation}")
  public ResponseEntity<?> acceptOperation(
      @PathVariable String operation,
      @RequestBody(required = false) Map<String, String> arguments,
      UriComponentsBuilder b) {

    clusterManagementService.perform(operation, arguments);

    UriComponents uriComponents =
        b.path(MANAGEMENT_API_VERSION + "/operations/{operationType}").buildAndExpand(operation);

    HttpHeaders headers = new HttpHeaders();
    headers.setLocation(uriComponents.toUri());

    return new ResponseEntity<>(headers, HttpStatus.ACCEPTED);
  }

}
