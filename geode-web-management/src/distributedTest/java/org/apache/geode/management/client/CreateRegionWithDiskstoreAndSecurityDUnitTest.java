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

package org.apache.geode.management.client;


import static org.hamcrest.Matchers.is;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.BasicRegionConfig;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.rest.LocatorWebContext;
import org.apache.geode.management.internal.rest.LocatorWithSecurityManagerContextLoader;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = LocatorWithSecurityManagerContextLoader.class)
@WebAppConfiguration
public class CreateRegionWithDiskstoreAndSecurityDUnitTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private ClusterManagementService client;
  private LocatorWebContext webContext;
  private MemberVM server;

  @Before
  public void before() throws Exception {
    cluster.setSkipLocalDistributedSystemCleanup(true);
    webContext = new LocatorWebContext(webApplicationContext);


    int port = webContext.getLocator().getPort();
    server = cluster.startServerVM(0,
        s -> s.withConnectionToLocator(port).withCredential("cluster", "cluster"));

    gfsh.secureConnectAndVerify(port, GfshCommandRule.PortType.locator, "cluster", "cluster");
  }

  @Test
  public void createReplicateRegionWithDiskstoreWithoutDataManage() throws Exception {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        cluster.getWorkingDirRoot())).statusIsSuccess();

    BasicRegionConfig regionConfig = new BasicRegionConfig();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);

    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDiskStoreName("DISKSTORE");
    regionConfig.setRegionAttributes(attributes);

    String json = GeodeJsonMapper.getMapper().writeValueAsString(regionConfig);

    webContext.perform(post("/v2/regions")
        .with(httpBasic("user", "user"))
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("user not authorized for DATA:MANAGE")));
  }


  @Test
  public void createReplicateRegionWithDiskstoreWithoutClusterWrite() throws Exception {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        cluster.getWorkingDirRoot())).statusIsSuccess();

    BasicRegionConfig regionConfig = new BasicRegionConfig();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);

    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDiskStoreName("DISKSTORE");
    regionConfig.setRegionAttributes(attributes);

    String json = GeodeJsonMapper.getMapper().writeValueAsString(regionConfig);

    webContext.perform(post("/v2/regions")
        .with(httpBasic("data", "data"))
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("data not authorized for CLUSTER:WRITE:DISK")));
  }

  @Test
  public void createReplicateRegionWithDiskstoreSuccess() throws Exception {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        cluster.getWorkingDirRoot())).statusIsSuccess();

    BasicRegionConfig regionConfig = new BasicRegionConfig();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);

    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDiskStoreName("DISKSTORE");
    regionConfig.setRegionAttributes(attributes);

    String json = GeodeJsonMapper.getMapper().writeValueAsString(regionConfig);

    webContext.perform(post("/v2/regions")
        .with(httpBasic("data,cluster", "data,cluster"))
        .content(json))
        .andExpect(jsonPath("$.statusCode", is("OK")));
  }

}
