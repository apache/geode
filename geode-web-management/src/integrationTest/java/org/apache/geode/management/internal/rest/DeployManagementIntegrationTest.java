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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.request.MockMultipartHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DeployManagementIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();
  private File jar1, jar2;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    context = new LocatorWebContext(webApplicationContext);

    jar1 = new File(temporaryFolder.getRoot(), "jar1.jar");
    jar2 = new File(temporaryFolder.getRoot(), "jar2.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jar1, "ClassOne");
    jarBuilder.buildJarFromClassNames(jar2, "ClassTwo");
  }

  /**
   * This test uses MockMvc directly instead of RestTemplate with MockMvcClientHttpRequestFactory
   * because MockMvcClientHttpRequestFactory doesn't support multipart form data properly.
   * It only uses .content(requestBody) which cannot handle multipart requests.
   */
  @Test
  @WithMockUser
  public void sanityCheck() throws Exception {
    // First deployment: jar1 to group1
    MockMultipartFile file1 = new MockMultipartFile("file", jar1.getName(),
        "application/java-archive", Files.readAllBytes(jar1.toPath()));

    Deployment deployment1 = new Deployment();
    deployment1.setGroup("group1");
    String config1 = mapper.writeValueAsString(deployment1);

    MockMultipartHttpServletRequestBuilder builder1 =
        MockMvcRequestBuilders.multipart("/v1/deployments");
    builder1.with(request -> {
      request.setMethod("PUT");
      return request;
    });

    context.perform(builder1.file(file1).param("config", config1))
        .andExpect(status().isCreated());

    // Second deployment: jar1 to group2
    MockMultipartFile file1Again = new MockMultipartFile("file", jar1.getName(),
        "application/java-archive", Files.readAllBytes(jar1.toPath()));

    Deployment deployment2 = new Deployment();
    deployment2.setGroup("group2");
    String config2 = mapper.writeValueAsString(deployment2);

    MockMultipartHttpServletRequestBuilder builder2 =
        MockMvcRequestBuilders.multipart("/v1/deployments");
    builder2.with(request -> {
      request.setMethod("PUT");
      return request;
    });

    context.perform(builder2.file(file1Again).param("config", config2))
        .andExpect(status().isCreated());

    // Third deployment: jar2 to group2
    MockMultipartFile file2 = new MockMultipartFile("file", jar2.getName(),
        "application/java-archive", Files.readAllBytes(jar2.toPath()));

    MockMultipartHttpServletRequestBuilder builder3 =
        MockMvcRequestBuilders.multipart("/v1/deployments");
    builder3.with(request -> {
      request.setMethod("PUT");
      return request;
    });

    context.perform(builder3.file(file2).param("config", config2))
        .andExpect(status().isCreated());

    // Verify deployments by listing them
    String listResponse = context.perform(
        MockMvcRequestBuilders.get("/v1/deployments"))
        .andExpect(status().isOk())
        .andReturn()
        .getResponse()
        .getContentAsString();

    // Parse and verify the response contains jar1.jar and jar2.jar
    assertThat(listResponse).contains("jar1.jar", "jar2.jar");
  }


}
