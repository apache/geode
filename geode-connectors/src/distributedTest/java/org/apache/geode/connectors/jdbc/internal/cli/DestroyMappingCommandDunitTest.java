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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyMappingCommand.DESTROY_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyMappingCommand.DESTROY_MAPPING__REGION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
public class DestroyMappingCommandDunitTest implements Serializable {

  private static final String REGION_NAME = "testRegion";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;

  @Before
  public void before() throws Exception {

    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat("create region --name=" + REGION_NAME + " --type=PARTITION")
        .statusIsSuccess();
  }

  private void setupAsyncMapping() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "myDataSource");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void setupSynchronousMapping() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(CREATE_MAPPING__REGION_NAME, REGION_NAME);
    csb.addOption(CREATE_MAPPING__DATA_SOURCE_NAME, "myDataSource");
    csb.addOption(CREATE_MAPPING__PDX_NAME, "myPdxClass");
    csb.addOption(CREATE_MAPPING__SYNCHRONOUS_NAME, "true");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void destroysAsyncMapping() {
    setupAsyncMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(DESTROY_MAPPING__REGION_NAME, REGION_NAME);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(false);
    });

    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      verifyMappingRemovedFromService(cache);
      verifyRegionAltered(cache);
      verifyQueueRemoved(cache);
    });
  }

  @Test
  public void destroysAsyncMappingWithRegionPath() {
    setupAsyncMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(DESTROY_MAPPING__REGION_NAME, "/" + REGION_NAME);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(false);
    });

    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      verifyMappingRemovedFromService(cache);
      verifyRegionAltered(cache);
      verifyQueueRemoved(cache);
    });
  }

  @Test
  public void destroysSynchronousMapping() throws Exception {
    setupSynchronousMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(DESTROY_MAPPING__REGION_NAME, REGION_NAME);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(true);
    });

    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      verifyMappingRemovedFromService(cache);
      verifyRegionAltered(cache);
      verifyQueueRemoved(cache);
    });
  }

  @Test
  public void destroysSynchronousMappingWithRegionPath() throws Exception {
    setupSynchronousMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(DESTROY_MAPPING__REGION_NAME, "/" + REGION_NAME);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(true);
    });

    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      verifyMappingRemovedFromService(cache);
      verifyRegionAltered(cache);
      verifyQueueRemoved(cache);
    });
  }

  private static RegionMapping getRegionMappingFromClusterConfig() {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(REGION_NAME)).findFirst().orElse(null);
    return (RegionMapping) regionConfig.getCustomRegionElements().stream()
        .filter(element -> element instanceof RegionMapping).findFirst().orElse(null);
  }

  private static void validateAsyncEventQueueRemovedFromClusterConfig() {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    List<CacheConfig.AsyncEventQueue> queueList = cacheConfig.getAsyncEventQueues();
    assertThat(queueList).isEmpty();
  }

  private static void validateRegionAlteredInClusterConfig(boolean synchronous) {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(REGION_NAME)).findFirst().orElse(null);
    RegionAttributesType attributes = regionConfig.getRegionAttributes();
    assertThat(attributes.getCacheLoader()).isNull();
    if (synchronous) {
      assertThat(attributes.getCacheWriter()).isNull();
    } else {
      assertThat(attributes.getAsyncEventQueueIds()).isEqualTo("");
    }
  }

  private void verifyQueueRemoved(InternalCache cache) {
    String queueName = CreateMappingCommand.createAsyncEventQueueName(REGION_NAME);
    assertThat(cache.getAsyncEventQueue(queueName)).isNull();
  }

  private void verifyRegionAltered(InternalCache cache) {
    Region<?, ?> region = cache.getRegion(REGION_NAME);
    assertThat(region.getAttributes().getCacheLoader()).isNull();
    assertThat(region.getAttributes().getCacheWriter()).isNull();
    String queueName = CreateMappingCommand.createAsyncEventQueueName(REGION_NAME);
    assertThat(region.getAttributes().getAsyncEventQueueIds()).doesNotContain(queueName);
  }

  private void verifyMappingRemovedFromService(InternalCache cache) {
    RegionMapping mapping =
        cache.getService(JdbcConnectorService.class).getMappingForRegion(REGION_NAME);
    assertThat(mapping).isNull();
  }
}
