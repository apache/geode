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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.dto.Value1;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@SuppressWarnings("serial")
public class QueryCommandDUnitTestBase {
  private static final String DATA_REGION_NAME = "GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_PATH = "/" + DATA_REGION_NAME;
  private static final String DATA_REGION_WITH_EVICTION_NAME =
      "GemfireDataCommandsTestRegionWithEviction";
  private static final String DATA_REGION_WITH_EVICTION_NAME_PATH =
      "/" + DATA_REGION_WITH_EVICTION_NAME;
  private static final String DATA_PAR_REGION_NAME = "GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_PATH = "/" + DATA_PAR_REGION_NAME;

  private static final String SERIALIZATION_FILTER =
      "org.apache.geode.management.internal.cli.dto.**";

  static final int COUNT = 5;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  protected MemberVM locator, server1, server2;

  @Before
  public void before() throws Exception {
    Properties locatorProps = locatorProperties();
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(locatorProps));
    server1 = cluster.startServerVM(1, serverProperties(), locator.getPort());
    server2 = cluster.startServerVM(2, serverProperties(), locator.getPort());

    server1.invoke(() -> setupReplicatedRegion(DATA_REGION_NAME));
    server2.invoke(() -> setupReplicatedRegion(DATA_REGION_NAME));
    server1.invoke(() -> setupPartitionedRegion(DATA_PAR_REGION_NAME));

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_PATH, 1);

    connectToLocator();
  }

  public void connectToLocator() throws Exception {
    gfsh.connectAndVerify(locator.getJmxPort(), jmxManager);
  }

  @Test
  public void testWithGfshEnvironmentVariables() {
    String query =
        "query --query=\"select ID , status , createTime , pk, floatMinValue from ${DATA_REGION} where ID <= ${PORTFOLIO_ID}"
            + " and status=${STATUS}" + "\" --interactive=false";
    gfsh.executeCommand("set variable --name=DATA_REGION --value=" + DATA_REGION_NAME_PATH);
    gfsh.executeCommand("set variable --name=PORTFOLIO_ID --value=3");
    gfsh.executeCommand("set variable --name=STATUS --value=inactive");
    CommandResult cmdResult = gfsh.executeCommand(query);
    assertThat(cmdResult.getStatus()).isEqualTo(Result.Status.OK);
  }

  @Test
  public void testWithUnsetGfshEnvironmentVariables() {
    IgnoredException ex =
        addIgnoredException(QueryInvalidException.class.getSimpleName(), locator.getVM());
    try {
      String query =
          "query --query=\"select ID , status , createTime , pk, floatMinValue from ${UNSET_REGION} where ID <= ${UNSET_PORTFOLIO_ID}"
              + " and status=${UNSET_STATUS}" + "\" --interactive=false";
      CommandResult result = gfsh.executeCommand(query);
      assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
      assertThat(result.getResultData().toString())
          .contains(String.format("Syntax error in query: %s", ""));
    } finally {
      ex.remove();
    }
  }

  @Test
  public void testSimpleQuery() {
    server1.invoke(() -> prepareDataForRegion(DATA_PAR_REGION_NAME_PATH));
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select ID , status , createTime , pk, floatMinValue from "
        + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger + "\" --interactive=false";
    CommandResult commandResult = gfsh.executeCommand(query);
    validateSelectResult(commandResult, true, (randomInteger + 1),
        new String[] {"ID", "status", "createTime", "pk", "floatMinValue"});
  }

  @Test
  public void testSimpleQueryOnLocator() {
    server1.invoke(() -> prepareDataForRegion(DATA_PAR_REGION_NAME_PATH));

    locator.invoke(() -> {
      String query = "query --query=\"select ID , status , createTime , pk, floatMinValue from "
          + DATA_PAR_REGION_NAME_PATH + " where ID <= 4"
          + "\" --interactive=false";
      ManagementService service =
          ManagementService.getExistingManagementService(ClusterStartupRule.getCache());
      MemberMXBean member = service.getMemberMXBean();
      String cmdResult = member.processCommand(query);

      assertThat(cmdResult).contains("ID");
      assertThat(cmdResult).contains("status");
      assertThat(cmdResult).contains("createTime");
      assertThat(cmdResult).contains("pk");
      assertThat(cmdResult).contains("floatMinValue");
      assertThat(cmdResult).contains("\"Rows\":\"5\"");
    });
  }

  @Test
  public void testQueryEvictedDataDeserializable() {
    server1.invoke(() -> setupReplicatedRegionWithEviction(DATA_REGION_WITH_EVICTION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_WITH_EVICTION_NAME_PATH, 1);
    server1.invoke(() -> prepareDeserializableDataForRegion(DATA_REGION_WITH_EVICTION_NAME_PATH));

    String query = "query --query=\"select Value from " + DATA_REGION_WITH_EVICTION_NAME_PATH
        + "\" --interactive=false";
    CommandResult commandResult = gfsh.executeCommand(query);
    validateSelectResult(commandResult, Boolean.TRUE, 10, new String[] {"Value"});
  }

  @Test
  public void testQueryEvictedDataNotDeserializable() {
    IgnoredException ex =
        addIgnoredException(Exception.class.getSimpleName(), locator.getVM());

    server1.invoke(() -> setupReplicatedRegionWithEviction(DATA_REGION_WITH_EVICTION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_WITH_EVICTION_NAME_PATH, 1);
    server1
        .invoke(() -> prepareNotDeserializableDataForRegion(DATA_REGION_WITH_EVICTION_NAME_PATH));

    String query = "query --query=\"select Value from " + DATA_REGION_WITH_EVICTION_NAME_PATH
        + "\" --interactive=false";
    CommandResult commandResult = gfsh.executeCommand(query);
    validateSelectResult(commandResult, Boolean.FALSE, -1, new String[] {"Value"});
    assertThat(commandResult.getResultData().toString())
        .contains("An IOException was thrown while deserializing");

    ex.remove();
  }

  private static void prepareDataForRegion(String regionPath) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region dataRegion = cache.getRegion(regionPath);

    for (int j = 0; j < 10; j++) {
      dataRegion.put(new Integer(j), new Portfolio(j));
    }
  }

  private static void prepareNotDeserializableDataForRegion(String regionPath) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region dataRegion = cache.getRegion(regionPath);

    for (int j = 0; j < 10; j++) {
      dataRegion.put(new Integer(j), new shouldFailSerializationFilter(j));
    }
  }

  private static void prepareDeserializableDataForRegion(String regionPath) {
    InternalCache cache = ClusterStartupRule.getCache();
    Region dataRegion = cache.getRegion(regionPath);

    for (int j = 0; j < 10; j++) {
      dataRegion.put(new Integer(j), new Value1(j));
    }
  }

  private static void setupReplicatedRegionWithEviction(String regionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();
    evictionAttributes.setMaximum(1).setAction(EvictionAction.OVERFLOW_TO_DISK)
        .setAlgorithm(EvictionAlgorithm.LRU_ENTRY);
    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setEvictionAttributes(evictionAttributes);

    Region dataRegion = regionFactory.create(regionName);
    assertThat(dataRegion).isNotNull();
    assertThat(dataRegion.getFullPath()).contains(regionName);
  }

  private static void setupPartitionedRegion(String regionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    PartitionAttributes partitionAttrs =
        new PartitionAttributesFactory().setRedundantCopies(2).create();
    RegionFactory<Object, Object> partitionRegionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);

    partitionRegionFactory.setPartitionAttributes(partitionAttrs);
    Region dataParRegion = partitionRegionFactory.create(regionName);

    assertThat(dataParRegion).isNotNull();
    assertThat(dataParRegion.getFullPath()).contains(regionName);
  }

  private static void setupReplicatedRegion(String regionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);

    Region dataRegion = regionFactory.create(regionName);
    assertThat(dataRegion).isNotNull();
    assertThat(dataRegion.getFullPath()).contains(regionName);
  }

  private void validateSelectResult(CommandResult cmdResult, Boolean expectSuccess,
      Integer expectedRows, String[] cols) {
    if (ResultData.TYPE_MODEL.equals(cmdResult.getType())) {
      ResultModel rd = (ResultModel) cmdResult.getResultData();

      Map<String, String> data =
          rd.getDataSection(DataCommandResult.DATA_INFO_SECTION).getContent();
      assertThat(data.get("Result")).isEqualTo(expectSuccess.toString());

      if (expectSuccess && expectedRows != -1) {
        assertThat(data.get("Rows")).isEqualTo(expectedRows.toString());

        if (expectedRows > 0 && cols != null) {
          Map<String, List<String>> table =
              rd.getTableSection(DataCommandResult.QUERY_SECTION).getContent();
          assertThat(table.keySet()).contains(cols);
        }
      }
    } else {
      fail("Expected CompositeResult Returned Result Type " + cmdResult.getType());
    }
  }

  private Properties locatorProperties() {
    int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);

    return props;
  }

  private Properties serverProperties() {
    Properties props = new Properties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);

    return props;
  }

  public static class shouldFailSerializationFilter extends Value1 {
    private Value1 value1 = null;

    public shouldFailSerializationFilter(int i) {
      super(i);
    }

    public Value1 getValue1() {
      return value1;
    }

    public void setValue1(Value1 value1) {
      this.value1 = value1;
    }
  }
}
