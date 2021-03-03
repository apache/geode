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


import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.apache.geode.test.junit.assertions.ClusterManagementRealizationResultAssert.assertManagementResult;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.EntityInfo;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
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

  private ClusterManagementService client;

  private Deployment deployment;
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();
  private File jar1, jar2;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    context = new LocatorWebContext(webApplicationContext);
    client = new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(
            new RestTemplate(context.getRequestFactory())))
        .build();
    deployment = new Deployment();

    jar1 = new File(temporaryFolder.getRoot(), "jar1.jar");
    jar2 = new File(temporaryFolder.getRoot(), "jar2.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jar1, "ClassOne");
    jarBuilder.buildJarFromClassNames(jar2, "ClassTwo");
  }

  @Test
  @WithMockUser
  public void sanityCheck() {
    deployment.setFile(jar1);
    deployment.setGroup("group1");
    assertManagementResult(client.create(deployment)).isSuccessful();

    deployment.setGroup("group2");
    assertManagementResult(client.create(deployment)).isSuccessful();

    deployment.setFile(jar2);
    deployment.setGroup("group2");
    assertManagementResult(client.create(deployment)).isSuccessful();

    assertManagementListResult(client.list(new Deployment())).isSuccessful()
        .hasEntityInfo()
        .hasSize(2)
        .extracting(EntityInfo::getId)
        .containsExactlyInAnyOrder("jar1.jar", "jar2.jar");
  }


}
