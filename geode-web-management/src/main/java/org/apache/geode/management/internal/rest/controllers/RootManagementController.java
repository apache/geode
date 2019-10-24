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

import static org.apache.geode.management.configuration.Links.URI_CONTEXT;
import static org.apache.geode.management.configuration.Links.URI_VERSION;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Optional;

import io.swagger.annotations.ApiOperation;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.Links;

@RestController("api-root")
@RequestMapping(URI_VERSION)
public class RootManagementController extends AbstractManagementController {
  private static final Logger logger = LogService.getLogger();

  private static final String API_ROOT = "api root";

  private final RequestMappingHandlerMapping handlerMapping;

  @Autowired
  public RootManagementController(RequestMappingHandlerMapping handlerMapping) {
    this.handlerMapping = handlerMapping;
  }

  @ApiOperation(value = API_ROOT)
  @GetMapping("/")
  public ClusterManagementResult getRootLinks() {
    ClusterManagementResult clusterManagementResult = new ClusterManagementResult();
    Links links = new Links();
    links.addLink("swagger", URI_CONTEXT + "/swagger-ui.html");
    links.addLink("docs", "https://geode.apache.org/docs");
    links.addLink("wiki",
        "https://cwiki.apache.org/confluence/display/pages/viewpage.action?pageId=115511910");
    handlerMapping.getHandlerMethods()
        .entrySet()
        .stream()
        .filter(
            entry -> entry.getKey().getPatternsCondition().toString()
                .contains(URI_VERSION))
        .forEach(
            entry -> links
                .addLink(extractApiOperationValue(entry.getValue().getMethod().getAnnotations())
                    .orElse(entry.getValue().getMethod().getName()),
                    URI_CONTEXT
                        + entry.getKey().getPatternsCondition().getPatterns().iterator().next()));
    clusterManagementResult.setLinks(links);
    return clusterManagementResult;
  }

  private static Optional<String> extractApiOperationValue(Annotation[] annotations) {
    return Arrays.stream(annotations)
        .filter(ApiOperation.class::isInstance)
        .map(ApiOperation.class::cast)
        .map(ApiOperation::value)
        .findAny();
  }
}
