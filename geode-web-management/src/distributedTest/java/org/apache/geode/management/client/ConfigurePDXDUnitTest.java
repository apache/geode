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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.internal.rest.LocatorWebContext;
import org.apache.geode.management.internal.rest.PlainLocatorContextLoader;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class ConfigurePDXDUnitTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  private ClusterManagementService client;
  private LocatorWebContext webContext;
  private Pdx pdxType;

  @Before
  public void before() {
    cluster.setSkipLocalDistributedSystemCleanup(true);
    webContext = new LocatorWebContext(webApplicationContext);
    client = ClusterManagementServiceBuilder.buildWithRequestFactory()
        .setRequestFactory(webContext.getRequestFactory()).build();
    pdxType = new Pdx();
  }

  @After
  public void after() {
    // for the test to be run multiple times, we need to clean out the cluster config
    InternalConfigurationPersistenceService cps = getLocator().getConfigurationPersistenceService();
    cps.updateCacheConfig("cluster", config -> {
      config.setPdx(null);
      return config;
    });
  }

  private InternalLocator getLocator() {
    return ((PlainLocatorContextLoader) webContext.getLocator()).getLocatorStartupRule()
        .getLocator();
  }

  @Test
  public void configureWithNoServer() {
    ClusterManagementRealizationResult result = client.create(pdxType);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getUri()).endsWith("/management/experimental/configurations/pdx");
    assertThat(result.getMemberStatuses()).hasSize(0);

    // call create the 2nd time
    pdxType.setDiskStoreName("diskstore");
    assertThatThrownBy(() -> client.create(pdxType))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_EXISTS: Pdx 'PDX' already exists in group cluster");
  }

  @Test
  public void configureWithARunningServer() {
    MemberVM server = cluster.startServerVM(1, webContext.getLocator().getPort());

    ClusterManagementRealizationResult result = client.create(pdxType);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getUri()).endsWith("/management/experimental/configurations/pdx");

    RealizationResult status = result.getMemberStatuses().get(0);
    assertThat(status.getMemberName()).isEqualTo("server-1");
    assertThat(status.getMessage()).contains(
        "Server 'server-1' needs to be restarted for this configuration change to be realized.");

    InternalConfigurationPersistenceService cps = getLocator().getConfigurationPersistenceService();
    CacheConfig cluster = cps.getCacheConfig("cluster");
    PdxType xmlPdxType = cluster.getPdx();
    assertThat(xmlPdxType.getDiskStoreName()).isNull();

    // create the 2nd time
    pdxType.setDiskStoreName("diskstore");
    assertThatThrownBy(() -> client.create(pdxType))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_EXISTS: Pdx 'PDX' already exists in group cluster");

    server.stop();
  }
}
