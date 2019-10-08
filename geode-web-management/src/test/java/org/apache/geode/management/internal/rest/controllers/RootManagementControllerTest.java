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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.ResponseEntity;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.internal.Links;

@RunWith(MockitoJUnitRunner.class)
public class RootManagementControllerTest {

  @Mock
  private RequestMappingHandlerMapping handlerMapping;

  private RootManagementController rootManagementController;

  @Before
  public void init() {
    rootManagementController = new RootManagementController(handlerMapping);
  }

  @Test
  public void getRootLinksWithHandlerMethods() {
    Map<RequestMappingInfo, HandlerMethod> handlerMethodMap = new HashMap<>();


    Mockito.when(handlerMapping.getHandlerMethods()).thenReturn(handlerMethodMap);

    ResponseEntity<ClusterManagementResult> result = rootManagementController.getRootLinks();
    Map<String, String> links = result.getBody().getLinks();
    Map<String, String> expectedLinks = Links.rootLinks();
    Links.addApiRoot(expectedLinks);
    assertThat(links).isEqualTo(expectedLinks);
  }

  @Test
  public void getRootLinksWithoutHandlerMethods() {
    Mockito.when(handlerMapping.getHandlerMethods()).thenReturn(Collections.emptyMap());

    ResponseEntity<ClusterManagementResult> result = rootManagementController.getRootLinks();
    Map<String, String> links = result.getBody().getLinks();
    Map<String, String> expectedLinks = Links.rootLinks();
    Links.addApiRoot(expectedLinks);
    assertThat(links).isEqualTo(expectedLinks);
  }
}
