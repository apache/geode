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

import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.management.ManagementService.getExistingManagementService;
import static org.apache.geode.management.internal.cli.domain.DataCommandResult.DATA_INFO_SECTION;
import static org.apache.geode.management.internal.cli.domain.DataCommandResult.QUERY_SECTION;
import static org.apache.geode.management.internal.i18n.CliStrings.MEMBER;
import static org.apache.geode.management.internal.i18n.CliStrings.QUERY;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

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
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public abstract class QueryCommandDUnitTestBase {

  private static final String SERIALIZATION_FILTER = Value.class.getName();

  private static final String REGION_NAME = "region";
  private static final String REGION_NAME_PATH = SEPARATOR + REGION_NAME;

  private static final String REGION_EVICTION_NAME = "regionWithEviction";
  private static final String REGION_EVICTION_NAME_PATH = SEPARATOR + REGION_EVICTION_NAME;

  private static final String REGION_PROXY_NAME = "regionWithProxy";
  private static final String REGION_PROXY_NAME_PATH = SEPARATOR + REGION_PROXY_NAME;

  private static final String PARTITIONED_REGION_NAME = "partitionedRegion";
  private static final String PARTITIONED_REGION_NAME_PATH = SEPARATOR + PARTITIONED_REGION_NAME;

  private static final int MAX_RANDOM_INTEGER = 5;

  protected MemberVM locator;
  protected MemberVM server1;
  protected MemberVM server2;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setUp() throws Exception {
    Properties locatorProps = locatorProperties();

    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(locatorProps));
    server1 = cluster.startServerVM(1, serverProperties(), locator.getPort());
    server2 = cluster.startServerVM(2, serverProperties(), locator.getPort());

    server1.invoke(() -> createReplicatedRegion(REGION_NAME));
    server2.invoke(() -> createReplicatedRegion(REGION_NAME));
    server1.invoke(() -> createPartitionedRegion(PARTITIONED_REGION_NAME));

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(PARTITIONED_REGION_NAME_PATH, 1);

    connectToLocator();
  }

  /**
   * Override to add additional locator config properties. {@code serializable-object-filter} has
   * already been set in the config properties.
   */
  protected abstract Properties locatorProperties(Properties configProperties);

  /**
   * Override to specify the mechanism used to connect to the Locator.
   */
  protected abstract void connectToLocator() throws Exception;

  private Properties locatorProperties() {
    Properties configProperties = new Properties();
    configProperties.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    return locatorProperties(configProperties);
  }

  private Properties serverProperties() {
    Properties configProperties = new Properties();
    configProperties.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    return configProperties;
  }

  @Test
  public void testWithGfshEnvironmentVariables() {
    gfsh.executeAndAssertThat("set variable --name=DATA_REGION --value=" + REGION_NAME_PATH)
        .statusIsSuccess();
    gfsh.executeAndAssertThat("set variable --name=PORTFOLIO_ID --value=3")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("set variable --name=STATUS --value=inactive")
        .statusIsSuccess();

    String query = "query --query=\"" +
        "select ID , status , createTime , pk, floatMinValue " +
        "from ${DATA_REGION} " +
        "where ID <= ${PORTFOLIO_ID} and status=${STATUS}" + "\" --interactive=false";

    gfsh.executeAndAssertThat(query)
        .statusIsSuccess();
  }

  @Test
  public void testWithUnsetGfshEnvironmentVariables() {
    addIgnoredException(QueryInvalidException.class);

    String query = "query --query=\"" +
        "select ID , status , createTime , pk, floatMinValue " +
        "from ${UNSET_REGION} " +
        "where ID <= ${UNSET_PORTFOLIO_ID} and status=${UNSET_STATUS}" + "\" --interactive=false";

    gfsh.executeAndAssertThat(query)
        .statusIsError()
        .containsOutput(String.format("Syntax error in query: %s", ""));
  }

  @Test
  public void testSimpleQuery() {
    server1.invoke(() -> setUpDataForRegion(PARTITIONED_REGION_NAME_PATH));

    int randomInteger = new Random(nanoTime()).nextInt(MAX_RANDOM_INTEGER);

    String query = "query --query=\"" +
        "select ID , status , createTime , pk, floatMinValue " +
        "from " + PARTITIONED_REGION_NAME_PATH + " " +
        "where ID <= " + randomInteger + "\" --interactive=false";

    CommandResult commandResult = gfsh.executeCommand(query);

    validateSelectResult(commandResult, true, randomInteger + 1,
        "ID", "status", "createTime", "pk", "floatMinValue");
  }

  @Test
  public void testSimpleQueryWithEscapingCharacter() {
    server1.invoke(() -> setUpDataForRegionWithSpecialCharacters(PARTITIONED_REGION_NAME_PATH));

    String query1 = "query --query=\"" +
        "select * from " + PARTITIONED_REGION_NAME_PATH + " e " +
        "where e LIKE 'value\\$'" + "\"";

    gfsh.executeAndAssertThat(query1)
        .statusIsSuccess()
        .containsOutput("value$");

    String query2 = "query --query=\"" +
        "select * from " + PARTITIONED_REGION_NAME_PATH + " e " +
        "where e LIKE 'value\\%'" + "\"";

    gfsh.executeAndAssertThat(query2)
        .statusIsSuccess()
        .containsOutput("value%");
  }

  @Test
  public void testSimpleQueryOnLocator() {
    server1.invoke(() -> setUpDataForRegion(PARTITIONED_REGION_NAME_PATH));

    locator.invoke(() -> {
      String query = "query --query=\"" +
          "select ID , status , createTime , pk, floatMinValue " +
          "from " + PARTITIONED_REGION_NAME_PATH + " " +
          "where ID <= 4" + "\" --interactive=false";

      String result = getMemberMXBean().processCommand(query);

      assertThat(result).contains("ID");
      assertThat(result).contains("status");
      assertThat(result).contains("createTime");
      assertThat(result).contains("pk");
      assertThat(result).contains("floatMinValue");
      assertThat(result).contains("\"Rows\":\"5\"");
    });
  }

  @Test
  public void testSimpleQueryWithUUID() {
    server1.invoke(() -> setUpDataForRegionWithUUID(PARTITIONED_REGION_NAME_PATH));

    String uuidKey = String.valueOf(new UUID(1, 1));

    String query1 = "query --query=\"" +
        "select key from " + PARTITIONED_REGION_NAME_PATH + ".entries" + "\"";

    gfsh.executeAndAssertThat(query1)
        .statusIsSuccess()
        .containsOutput(uuidKey);

    String query2 = "query --query=\"" +
        "select key,value from " + PARTITIONED_REGION_NAME_PATH + ".entries" + "\"";

    gfsh.executeAndAssertThat(query2)
        .statusIsSuccess()
        .containsOutput(uuidKey, "value");
  }

  @Test
  public void testQueryEvictedDataDeserializable() {
    server1.invoke(() -> createReplicatedRegionWithEviction(REGION_EVICTION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_EVICTION_NAME_PATH, 1);
    server1.invoke(() -> setUpDeserializableDataForRegion(REGION_EVICTION_NAME_PATH));

    String query = "query --query=\"" +
        "select Value from " + REGION_EVICTION_NAME_PATH + "\" --interactive=false";

    CommandResult commandResult = gfsh.executeCommand(query);

    validateSelectResult(commandResult, true, 10, "Value");
  }

  @Test
  public void testQueryEvictedDataNotDeserializable() {
    addIgnoredException(InvalidClassException.class);

    server1.invoke(() -> createReplicatedRegionWithEviction(REGION_EVICTION_NAME));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_EVICTION_NAME_PATH, 1);
    server1.invoke(() -> setUpNotDeserializableDataForRegion(REGION_EVICTION_NAME_PATH));

    String query = "query --query=\"" +
        "select Value from " + REGION_EVICTION_NAME_PATH + "\" --interactive=false";

    CommandResult commandResult = gfsh.executeCommand(query);

    validateSelectResult(commandResult, false, -1, "Value");

    assertThat(commandResult.asString()).contains("An IOException was thrown while deserializing");
  }

  @Test
  public void testSimpleQueryWithProxyRegion() {
    server1.invoke(() -> createReplicatedProxyRegion(REGION_PROXY_NAME));
    server2.invoke(() -> createReplicatedRegion(REGION_PROXY_NAME));

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(REGION_PROXY_NAME_PATH, 2);

    server1.invoke(() -> setUpDataForRegion(REGION_PROXY_NAME_PATH));

    int randomInteger = new Random(nanoTime()).nextInt(MAX_RANDOM_INTEGER);

    String queryString = "\"" +
        "select ID , status , createTime , pk, floatMinValue " +
        "from " + REGION_PROXY_NAME_PATH
        + " where ID <= " + randomInteger + "\"";

    String command = new CommandStringBuilder(QUERY)
        .addOption(MEMBER, "server-2")
        .addOption(QUERY, queryString)
        .getCommandString();

    CommandResult commandResult = gfsh.executeAndAssertThat(command).getCommandResult();

    validateSelectResult(commandResult, true, randomInteger + 1,
        "ID", "status", "createTime", "pk", "floatMinValue");
  }

  private static void createReplicatedRegion(String regionName) {
    createRegionFactory(REPLICATE)
        .create(regionName);
  }

  private static void createReplicatedRegionWithEviction(String regionName) {
    EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl()
        .setAction(EvictionAction.OVERFLOW_TO_DISK)
        .setAlgorithm(EvictionAlgorithm.LRU_ENTRY)
        .setMaximum(1);

    createRegionFactory(REPLICATE)
        .setEvictionAttributes(evictionAttributes)
        .create(regionName);
  }

  private static void createReplicatedProxyRegion(String regionName) {
    createRegionFactory(REPLICATE_PROXY)
        .create(regionName);
  }

  private static void createPartitionedRegion(String regionName) {
    PartitionAttributes<?, ?> partitionAttributes = new PartitionAttributesFactory<>()
        .setRedundantCopies(2)
        .create();

    createRegionFactory(PARTITION)
        .setPartitionAttributes(partitionAttributes)
        .create(regionName);
  }

  private static void setUpDataForRegion(String regionPath) {
    Region<Integer, Portfolio> region = getRegion(regionPath);

    for (int index = 0; index < 10; index++) {
      region.put(index, new Portfolio(index));
    }
  }

  private static void setUpDataForRegionWithSpecialCharacters(String regionPath) {
    Region<Integer, String> region = getRegion(regionPath);

    region.put(1, "value$");
    region.put(2, "value%");
  }

  private static void setUpDataForRegionWithUUID(String regionPath) {
    Region<UUID, String> region = getRegion(regionPath);

    region.put(new UUID(1, 1), "value");
  }

  private static void setUpNotDeserializableDataForRegion(String regionPath) {
    Region<Integer, FailsSerializationFilter> region = getRegion(regionPath);

    for (int index = 0; index < 10; index++) {
      region.put(index, new FailsSerializationFilter(index));
    }
  }

  private static void setUpDeserializableDataForRegion(String regionPath) {
    Region<Integer, Value> region = getRegion(regionPath);

    for (int index = 0; index < 10; index++) {
      region.put(index, new Value(index));
    }
  }

  private static void validateSelectResult(CommandResult commandResult, boolean expectSuccess,
      int expectedRows, String... columns) {
    ResultModel resultData = commandResult.getResultData();

    Map<String, String> data = resultData.getDataSection(DATA_INFO_SECTION).getContent();
    assertThat(data.get("Result")).isEqualTo(String.valueOf(expectSuccess));

    if (expectSuccess && expectedRows != -1) {
      assertThat(data.get("Rows")).isEqualTo(String.valueOf(expectedRows));

      if (expectedRows > 0 && columns != null) {
        Map<String, List<String>> table = resultData.getTableSection(QUERY_SECTION).getContent();
        assertThat(table.keySet()).contains(columns);
      }
    }
  }

  private static MemberMXBean getMemberMXBean() {
    return getManagementService().getMemberMXBean();
  }

  private static ManagementService getManagementService() {
    return getExistingManagementService(getCache());
  }

  private static <K, V> RegionFactory<K, V> createRegionFactory(RegionShortcut shortcut) {
    return requireNonNull(getCache()).createRegionFactory(shortcut);
  }

  private static <K, V> Region<K, V> getRegion(String regionPath) {
    return requireNonNull(getCache()).getRegion(regionPath);
  }

  /**
   * Sample class for Data DUnit tests with JSON keys and values
   */
  @SuppressWarnings("serial")
  public static class Value implements Serializable {

    private final String name;
    private final String lastName;
    private final String department;
    private final int age;
    private final int employeeId;

    public Value(int suffix) {
      employeeId = suffix;
      name = "Name" + suffix;
      lastName = "lastName" + suffix;
      department = "department" + suffix;
      age = suffix;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Value) {
        Value value = (Value) other;
        return value.employeeId == employeeId;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return employeeId % 7;
    }

    @Override
    public String toString() {
      return new StringBuilder("Value [")
          .append(" name : ").append(name)
          .append(" lastName : ").append(lastName)
          .append(" department : ").append(department)
          .append(" age : ").append(age)
          .append(" employeeId : ").append(employeeId)
          .append(" ]")
          .toString();
    }
  }

  @SuppressWarnings("serial")
  public static class FailsSerializationFilter extends Value {

    public FailsSerializationFilter(int suffix) {
      super(suffix);
    }
  }
}
