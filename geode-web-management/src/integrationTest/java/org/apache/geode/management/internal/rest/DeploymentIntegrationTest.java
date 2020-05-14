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

package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.request.MockMultipartHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.Deployment;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = MockLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DeploymentIntegrationTest {
  @Autowired
  private WebApplicationContext webApplicationContext;
  private MockLocatorContextLoader mockLocator;
  private ClusterManagementService cms;


  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    mockLocator = (MockLocatorContextLoader) context.getLocator();
    cms = mockLocator.getClusterManagementService();
  }

  @Test
  public void configIsNotRequired() throws Exception {
    MockMultipartFile file = new MockMultipartFile("file", "test.jar",
        "application/zip", "Foo".getBytes());
    MockMultipartHttpServletRequestBuilder builder =
        MockMvcRequestBuilders.multipart("/v1/deployments");
    builder.with(new RequestPostProcessor() {
      @Override
      public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
        request.setMethod("PUT");
        return request;
      }
    });

    // request only contains file, not configuration json
    context.perform(builder.file(file))
        .andExpect(status().isCreated());
    ArgumentCaptor<Deployment> argumentCaptor = ArgumentCaptor.forClass(Deployment.class);
    verify(cms).create(argumentCaptor.capture());
    Deployment deployment = argumentCaptor.getValue();
    assertThat(deployment.getFileName()).isEqualTo("test.jar");
    assertThat(deployment.getGroup()).isNull();
  }
}
