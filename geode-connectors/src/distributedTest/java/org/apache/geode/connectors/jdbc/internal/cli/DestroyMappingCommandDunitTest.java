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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyMappingCommand.DESTROY_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
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
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({JDBCConnectorTest.class})
public class DestroyMappingCommandDunitTest implements Serializable {

  private static final String TEST_REGION = "testRegion";
  private static final String GROUP1_REGION = "group1Region";
  private static final String GROUP2_REGION = "group2Region";
  private static final String GROUP1_GROUP2_REGION = "group1Group2Region";
  private static final String TEST_GROUP1 = "testGroup1";
  private static final String TEST_GROUP2 = "testGroup2";

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;

  @Before
  public void before() throws Exception {

    locator = startupRule.startLocatorVM(0);
    server1 = startupRule.startServerVM(1, locator.getPort());
    server2 = startupRule.startServerVM(2, TEST_GROUP1, locator.getPort());
    server3 = startupRule.startServerVM(3, TEST_GROUP2, locator.getPort());
    server4 = startupRule.startServerVM(4, TEST_GROUP1 + "," + TEST_GROUP2, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat("create region --name=" + TEST_REGION + " --type=PARTITION")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP1_REGION + " --groups=" + TEST_GROUP1 + " --type=PARTITION")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP2_REGION + " --groups=" + TEST_GROUP2 + " --type=PARTITION")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + GROUP1_GROUP2_REGION + " --groups=" + TEST_GROUP1 + ","
            + TEST_GROUP2 + " --type=PARTITION")
        .statusIsSuccess();
    setupDatabase();
  }

  @After
  public void after() throws Exception {
    teardownDatabase();
  }

  private void setupDatabase() {
    gfsh.executeAndAssertThat(
        "create data-source --name=myDataSource"
            + " --username=myuser --password=mypass --pooled=false"
            + " --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    executeSql(
        "create table myuser." + TEST_REGION + " (id varchar(10) primary key, name varchar(10))");
  }

  private void teardownDatabase() {
    executeSql("drop table myuser." + TEST_REGION);
  }

  public static class IdAndName implements PdxSerializable {
    private String id;
    private String name;

    public IdAndName() {
      // nothing
    }

    IdAndName(String id, String name) {
      this.id = id;
      this.name = name;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("id", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      name = reader.readString("name");
    }
  }

  private void executeSql(String sql) {
    for (MemberVM server : Arrays.asList(server1, server2, server3, server4)) {
      server.invoke(() -> {
        try {
          DataSource ds = JNDIInvoker.getDataSource("myDataSource");
          Connection conn = ds.getConnection();
          Statement sm = conn.createStatement();
          sm.execute(sql);
          sm.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private void setupAsyncMapping() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, TEST_REGION);
    csb.addOption(DATA_SOURCE_NAME, "myDataSource");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SCHEMA_NAME, "myuser");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void setupSynchronousMapping() {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING);
    csb.addOption(REGION_NAME, TEST_REGION);
    csb.addOption(DATA_SOURCE_NAME, "myDataSource");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SCHEMA_NAME, "myuser");
    csb.addOption(SYNCHRONOUS_NAME, "true");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void setupMappingWithServerGroup(String groups, String regionName, boolean isSync) {
    CommandStringBuilder csb = new CommandStringBuilder(CREATE_MAPPING + " --groups=" + groups);
    csb.addOption(REGION_NAME, regionName);
    csb.addOption(TABLE_NAME, TEST_REGION);
    csb.addOption(DATA_SOURCE_NAME, "myDataSource");
    csb.addOption(PDX_NAME, IdAndName.class.getName());
    csb.addOption(SCHEMA_NAME, "myuser");
    csb.addOption(SYNCHRONOUS_NAME, Boolean.toString(isSync));

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void destroyRegionThatHasSynchronousMappingFails() {
    setupSynchronousMapping();

    gfsh.executeAndAssertThat("destroy region --name=" + TEST_REGION).statusIsError()
        .containsOutput("Cannot destroy region \"" + TEST_REGION
            + "\" because JDBC mapping exists. Use \"destroy jdbc-mapping\" first.");
  }

  @Test
  public void destroyRegionThatHadSynchronousMappingSucceeds() {
    setupSynchronousMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(REGION_NAME, TEST_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    gfsh.executeAndAssertThat("destroy region --name=" + TEST_REGION).statusIsSuccess();
  }


  @Test
  public void destroysAsyncMapping() {
    setupAsyncMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(REGION_NAME, TEST_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(false);
    });

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, TEST_REGION)).isTrue();
      verifyRegionAltered(cache, TEST_REGION, false);
      assertThat(queueRemoved(cache, TEST_REGION)).isTrue();
    });
  }

  @Test
  public void destroysAsyncMappingWithRegionPath() {
    setupAsyncMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(REGION_NAME, SEPARATOR + TEST_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(false);
    });

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, TEST_REGION)).isTrue();
      verifyRegionAltered(cache, TEST_REGION, false);
      assertThat(queueRemoved(cache, TEST_REGION)).isTrue();
    });
  }

  @Test
  public void destroysSynchronousMapping() throws Exception {
    setupSynchronousMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(REGION_NAME, TEST_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(true);
    });

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, TEST_REGION)).isTrue();
      verifyRegionAltered(cache, TEST_REGION, false);
      assertThat(queueRemoved(cache, TEST_REGION)).isTrue();
    });
  }

  @Test
  public void destroysMappingForServerGroup() throws Exception {
    setupMappingWithServerGroup(TEST_GROUP1, GROUP1_REGION, true);
    CommandStringBuilder csb =
        new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(REGION_NAME, GROUP1_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsError();

    csb =
        new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP1);
    csb.addOption(REGION_NAME, GROUP1_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(true);
    });

    // we have this region on server2 only
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, GROUP1_REGION)).isTrue();
      verifyRegionAltered(cache, GROUP1_REGION, false);
      assertThat(queueRemoved(cache, GROUP1_REGION)).isTrue();
    });
  }

  @Test
  public void destroysMappingForMultiServerGroup() throws Exception {
    setupMappingWithServerGroup(TEST_GROUP1 + "," + TEST_GROUP2, GROUP1_GROUP2_REGION, true);
    // Purposely destroy the mapping on one group only
    CommandStringBuilder csb =
        new CommandStringBuilder(DESTROY_MAPPING + " --groups=" + TEST_GROUP1);
    csb.addOption(REGION_NAME, GROUP1_GROUP2_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(true);
    });

    // server1 never has the mapping
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, GROUP1_GROUP2_REGION)).isTrue();
      verifyRegionAltered(cache, GROUP1_GROUP2_REGION, false);
      assertThat(queueRemoved(cache, GROUP1_GROUP2_REGION)).isTrue();
    });
    // server2 and server4's mapping are destroyed
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, GROUP1_GROUP2_REGION)).isTrue();
      verifyRegionAltered(cache, GROUP1_GROUP2_REGION, false);
      assertThat(queueRemoved(cache, GROUP1_GROUP2_REGION)).isTrue();
    });
    server4.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, GROUP1_GROUP2_REGION)).isTrue();
      verifyRegionAltered(cache, GROUP1_GROUP2_REGION, false);
      assertThat(queueRemoved(cache, GROUP1_GROUP2_REGION)).isTrue();
    });
    // server3 should still have the mapping
    server3.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, GROUP1_GROUP2_REGION)).isFalse();
      verifyRegionAltered(cache, GROUP1_GROUP2_REGION, false);
      assertThat(queueRemoved(cache, GROUP1_GROUP2_REGION)).isTrue();
    });
  }

  @Test
  public void destroysSynchronousMappingWithRegionPath() throws Exception {
    setupSynchronousMapping();
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(REGION_NAME, SEPARATOR + TEST_REGION);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      assertThat(getRegionMappingFromClusterConfig()).isNull();
      validateAsyncEventQueueRemovedFromClusterConfig();
      validateRegionAlteredInClusterConfig(true);
    });

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(mappingRemovedFromService(cache, TEST_REGION)).isTrue();
      verifyRegionAltered(cache, TEST_REGION, false);
      assertThat(queueRemoved(cache, TEST_REGION)).isTrue();
    });
  }

  private static RegionMapping getRegionMappingFromClusterConfig() {
    CacheConfig cacheConfig =
        InternalLocator.getLocator().getConfigurationPersistenceService().getCacheConfig(null);
    RegionConfig regionConfig = cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(TEST_REGION)).findFirst().orElse(null);
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
        .filter(region -> region.getName().equals(TEST_REGION)).findFirst().orElse(null);
    RegionAttributesType attributes = regionConfig.getRegionAttributes();
    assertThat(attributes.getCacheLoader()).isNull();
    if (synchronous) {
      assertThat(attributes.getCacheWriter()).isNull();
    } else {
      assertThat(attributes.getAsyncEventQueueIds()).isEqualTo("");
    }
  }

  private boolean queueRemoved(InternalCache cache, String regionName) {
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    return cache.getAsyncEventQueue(queueName) == null;
  }

  private void verifyRegionAltered(InternalCache cache, String regionName, boolean exists) {
    Region<?, ?> region = cache.getRegion(TEST_REGION);
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    if (exists) {
      assertThat(region.getAttributes().getCacheLoader()).isNotNull();
      assertThat(region.getAttributes().getCacheWriter()).isNotNull();
      assertThat(region.getAttributes().getAsyncEventQueueIds()).contains(queueName);
    } else {
      assertThat(region.getAttributes().getCacheLoader()).isNull();
      assertThat(region.getAttributes().getCacheWriter()).isNull();
      assertThat(region.getAttributes().getAsyncEventQueueIds()).doesNotContain(queueName);
    }
  }

  private boolean mappingRemovedFromService(InternalCache cache, String regionName) {
    RegionMapping mapping =
        cache.getService(JdbcConnectorService.class).getMappingForRegion(regionName);
    return (mapping == null);
  }
}
