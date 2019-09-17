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

import static org.apache.geode.management.api.Links.API_ROOT;
import static org.apache.geode.management.api.RestfulEndpoint.URI_CONTEXT;
import static org.apache.geode.management.api.RestfulEndpoint.URI_VERSION;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Optional;

import io.swagger.annotations.ApiOperation;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.Links;

@Controller("api-root")
@RequestMapping(URI_VERSION)
public class RootManagementController extends AbstractManagementController {
  private static final Logger logger = LogService.getLogger();

  private final RequestMappingHandlerMapping handlerMapping;

  @Autowired
  public RootManagementController(RequestMappingHandlerMapping handlerMapping) {
    this.handlerMapping = handlerMapping;
  }

  @ApiOperation(value = API_ROOT)
  @RequestMapping(method = RequestMethod.GET, value = "/")
  public ResponseEntity<ClusterManagementResult> getRootLinks() {
    ClusterManagementResult clusterManagementResult = new ClusterManagementResult();
    LinkedHashMap<String, String> links = Links.rootLinks();
    handlerMapping.getHandlerMethods()
        .entrySet()
        .stream()
        .filter(
            entry -> entry.getKey().getPatternsCondition().toString()
                .contains(URI_VERSION))
        .forEach(
            entry -> links
                .put(extractApiOperationValue(entry.getValue().getMethod().getAnnotations())
                    .orElse(entry.getValue().getMethod().getName()),
                    URI_CONTEXT
                        + entry.getKey().getPatternsCondition().getPatterns().iterator().next()));
    clusterManagementResult.setLinks(links);
    return new ResponseEntity<>(clusterManagementResult, HttpStatus.OK);
  }

  private static Optional<String> extractApiOperationValue(Annotation[] annotations) {
    return Arrays.stream(annotations)
        .filter(ApiOperation.class::isInstance)
        .map(ApiOperation.class::cast)
        .map(ApiOperation::value)
        .findAny();
  }
}
