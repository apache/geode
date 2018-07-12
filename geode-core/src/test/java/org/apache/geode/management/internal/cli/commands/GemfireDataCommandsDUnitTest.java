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
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.assertj.core.api.Java6Assertions.assertThat;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.dto.Value1;
import org.apache.geode.management.internal.cli.dto.Value2;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

/**
 * Dunit class for testing gemfire data commands : get, put, remove, select, rebalance
 */
@Category({DistributedTest.class})
@SuppressWarnings("serial")
public class GemfireDataCommandsDUnitTest {


  private static final String DATA_REGION_NAME = "GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_VM1 = "GemfireDataCommandsTestRegion_Vm1";
  private static final String DATA_REGION_NAME_VM2 = "GemfireDataCommandsTestRegion_Vm2";
  private static final String DATA_REGION_NAME_PATH = "/GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_VM1_PATH = "/GemfireDataCommandsTestRegion_Vm1";
  private static final String DATA_REGION_NAME_VM2_PATH = "/GemfireDataCommandsTestRegion_Vm2";

  private static final String DATA_PAR_REGION_NAME = "GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_VM1 = "GemfireDataCommandsTestParRegion_Vm1";
  private static final String DATA_PAR_REGION_NAME_VM2 = "GemfireDataCommandsTestParRegion_Vm2";
  private static final String DATA_PAR_REGION_NAME_PATH = "/GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_VM1_PATH =
      "/GemfireDataCommandsTestParRegion_Vm1";
  private static final String DATA_PAR_REGION_NAME_VM2_PATH =
      "/GemfireDataCommandsTestParRegion_Vm2";

  private static final String DATA_REGION_NAME_CHILD_1 = "ChildRegionRegion1";
  private static final String DATA_REGION_NAME_CHILD_1_PATH =
      "/GemfireDataCommandsTestRegion/ChildRegionRegion1";
  private static final String DATA_REGION_NAME_CHILD_1_2 = "ChildRegionRegion12";
  private static final String DATA_REGION_NAME_CHILD_1_2_PATH =
      "/GemfireDataCommandsTestRegion/ChildRegionRegion1/ChildRegionRegion12";

  private static final String SERIALIZATION_FILTER =
      "org.apache.geode.management.internal.cli.dto.**";

  static final int COUNT = 5;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator, server1, server2;

  public void before() throws Exception {
    locator = cluster.startLocatorVM(0, locatorProperties());
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  private static void setupRegions(String regionName, String parRegionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    RegionFactory regionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE).setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));

    Region dataRegion = regionFactory.create(DATA_REGION_NAME);
    assertNotNull(dataRegion);

    dataRegion =
        dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1, dataRegion.getAttributes());
    assertNotNull(dataRegion);

    dataRegion =
        dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1_2, dataRegion.getAttributes());
    assertNotNull(dataRegion);

    dataRegion = regionFactory.create(regionName);
    assertNotNull(dataRegion);

    PartitionAttributes partitionAttrs =
        new PartitionAttributesFactory().setRedundantCopies(2).create();
    RegionFactory<Object, Object> partitionRegionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    partitionRegionFactory.setPartitionAttributes(partitionAttrs);
    Region dataParRegion = partitionRegionFactory.create(DATA_PAR_REGION_NAME);
    assertNotNull(dataParRegion);
    dataParRegion = partitionRegionFactory.create(parRegionName);
    assertNotNull(dataParRegion);
  }



  void setupForGetPutRemoveLocateEntry(String testName) throws Exception {
    Properties props = locatorProperties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    props.setProperty(NAME, testName + "Manager");

    Properties serverProps = new Properties();
    serverProps.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    locator = cluster.startLocatorVM(0, props);
    server1 = cluster.startServerVM(1, serverProps, locator.getPort());
    server2 = cluster.startServerVM(2, serverProps, locator.getPort());

    gfsh.connectAndVerify(locator);

    server1.invoke(() -> setupRegions(DATA_REGION_NAME_VM1, DATA_PAR_REGION_NAME_VM1));

    server2.invoke(() -> {
      setupRegions(DATA_REGION_NAME_VM2, DATA_PAR_REGION_NAME_VM2);
    });

    locator.waitTillRegionsAreReadyOnServers(DATA_REGION_NAME_PATH, 2);
    locator.waitTillRegionsAreReadyOnServers(DATA_REGION_NAME_PATH, 2);
    locator.waitTillRegionsAreReadyOnServers(DATA_REGION_NAME_VM1_PATH, 1);
    locator.waitTillRegionsAreReadyOnServers(DATA_REGION_NAME_VM2_PATH, 1);
    locator.waitTillRegionsAreReadyOnServers(DATA_PAR_REGION_NAME_PATH, 2);
    locator.waitTillRegionsAreReadyOnServers(DATA_PAR_REGION_NAME_VM1_PATH, 1);
    locator.waitTillRegionsAreReadyOnServers(DATA_PAR_REGION_NAME_VM2_PATH, 1);

    locator.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      final ManagementService service = ManagementService.getManagementService(cache);

      assertNotNull(service.getMemberMXBean());
      assertNotNull(service.getManagerMXBean());
      DistributedRegionMXBean bean = service.getDistributedRegionMXBean(DATA_REGION_NAME_PATH);
      assertNotNull(bean);

      String regions[] = {DATA_REGION_NAME_PATH, DATA_REGION_NAME_VM1_PATH,
          DATA_REGION_NAME_VM2_PATH, DATA_PAR_REGION_NAME_PATH, DATA_PAR_REGION_NAME_VM1_PATH,
          DATA_PAR_REGION_NAME_VM2_PATH};

      for (String region : regions) {
        bean = service.getDistributedRegionMXBean(region);
        assertNotNull(bean);

        if (bean.getMemberCount() < 1) {
          fail("Even after waiting mbean reports number of member hosting region "
              + DATA_REGION_NAME_VM1_PATH + " is less than one");
        }
      }
    });
  }

  void setupForSelect() throws Exception {
    setupForGetPutRemoveLocateEntry("setupForSelect");

    // To avoid pagination issues and Gfsh waiting for user input
    gfsh.executeAndAssertThat("set variable --name=APP_FETCH_SIZE --value=" + COUNT)
        .statusIsSuccess();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      String regions[] = {DATA_PAR_REGION_NAME_PATH, DATA_PAR_REGION_NAME_VM1_PATH,
          DATA_REGION_NAME_CHILD_1_PATH, DATA_REGION_NAME_CHILD_1_2_PATH};
      for (String r : regions) {
        Region dataRegion = cache.getRegion(r);
        for (int j = 0; j < 10; j++) {
          dataRegion.put(new Integer(j), new Portfolio(j));
        }
      }
      Region dataRegion = cache.getRegion(DATA_REGION_NAME_PATH);
      for (int j = 0; j < 10; j++) {
        dataRegion.put(new Integer(j), new Value1(j));
      }

      dataRegion = cache.getRegion(DATA_REGION_NAME_VM1_PATH);
      for (int j = 0; j < 10; j++) {
        dataRegion.put(new Integer(j), new Value1WithValue2(j));
      }
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      String regions[] = {DATA_REGION_NAME_VM2_PATH, DATA_PAR_REGION_NAME_VM2_PATH};
      for (String r : regions) {
        Region dataRegion = cache.getRegion(r);
        for (int j = 0; j < 10; j++) {
          dataRegion.put(new Integer(j), new Portfolio(j));
        }
      }
    });
  }

  @Test
  public void testSelectCommand() throws Exception {
    setupForSelect();
    doTestGetRegionAssociatedMembersForSelect();
    doTestSelectWithGfshEnvironmentVariables(true);
    doTestSelectWithGfshEnvironmentVariables(false);
    doTestSelectProjection();
    selectWithMalformedQuery();
    doTestSelectProjectionProcessCommand();
    doTestSelectProjectionWithNestedField();
    doTestSelectBeansAsResult();
    doTestSelectBeansWithNestedFieldAsResult();
  }

  @Test
  public void testPrimitivesWithDataCommands() throws Exception {
    setupForGetPutRemoveLocateEntry("testPrimitives");
    Byte byteKey = Byte.parseByte("41");
    Byte byteValue = Byte.parseByte("31");
    Short shortKey = Short.parseShort("123");
    Short shortValue = Short.parseShort("121");
    Integer integerKey = Integer.parseInt("123456");
    Integer integerValue = Integer.parseInt("12345678");
    Float floatKey = Float.valueOf("12432.2325");
    Float floatValue = Float.valueOf("111111.1111");
    Double doubleKey = Double.valueOf("12432.235425");
    Double doubleValue = Double.valueOf("111111.111111");

    // Testing Byte Wrappers
    testGetPutLocateEntryFromShellAndGemfire(byteKey, byteValue, Byte.class, true, true);
    // Testing Short Wrappers
    testGetPutLocateEntryFromShellAndGemfire(shortKey, shortValue, Short.class, true, true);
    // Testing Integer Wrappers
    testGetPutLocateEntryFromShellAndGemfire(integerKey, integerValue, Integer.class, true, true);
    // Testing Float Wrappers
    testGetPutLocateEntryFromShellAndGemfire(floatKey, floatValue, Float.class, true, true);
    // Testing Double Wrappers
    testGetPutLocateEntryFromShellAndGemfire(doubleKey, doubleValue, Double.class, true, true);
  }



  private void testGetPutLocateEntryFromShellAndGemfire(final Serializable key,
      final Serializable value, Class klass, boolean addRegionPath, boolean expResult) {

    SerializableRunnableIF putTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      region.clear();
      region.put(key, value);
    };

    SerializableRunnableIF getTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      assertEquals(true, region.containsKey(key));
      assertEquals(value, region.get(key));
    };

    SerializableRunnableIF removeTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      assertEquals(true, region.containsKey(key));
      region.remove(key);
    };


    SerializableRunnableIF clearTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      region.clear();
    };

    String canonicalName = klass.getCanonicalName();
    String getCommand = "get --key=" + key + " --key-class=" + canonicalName
        + " --value-class=" + canonicalName;
    if (addRegionPath)
      getCommand += " --region=" + DATA_REGION_NAME_PATH;

    String locateEntryCommand = "locate entry --key=" + key + " --key-class="
        + canonicalName + " --value-class=" + canonicalName;
    if (addRegionPath)
      locateEntryCommand += " --region=" + DATA_REGION_NAME_PATH;

    String removeCommand = "remove --key=" + key + " --key-class=" + canonicalName;
    if (addRegionPath)
      removeCommand += " --region=" + DATA_REGION_NAME_PATH;
    String putCommand = "put --key=" + key + " --key-class=" + canonicalName
        + " --value=" + value + " --value-class=" + canonicalName;
    if (addRegionPath)
      putCommand += " --region=" + DATA_REGION_NAME_PATH;

    if (expResult) {
      // Do put from shell check gemfire get do gemfire remove
      CommandResult cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, true);
      server1.invoke(getTask);
      server1.invoke(removeTask);

      server1.invoke(clearTask);

      // Do put from gemfire check from shell do gemfire remove
      server1.invoke(putTask);
      cmdResult = gfsh.executeCommand(getCommand);
      validateResult(cmdResult, true);
      cmdResult = gfsh.executeCommand(locateEntryCommand);
      validateResult(cmdResult, true);
      server1.invoke(removeTask);

      server1.invoke(clearTask);

      // Do put from shell check from gemfire do remove from shell get from shell expect false
      cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, true);
      server1.invoke(getTask);
      cmdResult = gfsh.executeCommand(removeCommand);
      validateResult(cmdResult, true);
      cmdResult = gfsh.executeCommand(getCommand);
      validateResult(cmdResult, false);
      cmdResult = gfsh.executeCommand(locateEntryCommand);
      validateResult(cmdResult, false);
    } else {
      // Do put from shell check gemfire get do gemfire remove
      CommandResult cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, false);
      server1.invoke(clearTask);

      // Do put from gemfire check from shell do gemfire remove
      server1.invoke(putTask);
      cmdResult = gfsh.executeCommand(getCommand);
      validateResult(cmdResult, false);
      cmdResult = gfsh.executeCommand(locateEntryCommand);
      validateResult(cmdResult, false);
      server1.invoke(removeTask);
      server1.invoke(clearTask);

      // Do put from shell check from gemfire do remove from shell get from shell exepct false
      cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, false);
    }
  }

  private static void doQueryRegionsAssociatedMembers(String queryTemplate, int expectedMembers,
      boolean returnAll, String... regions) {
    InternalCache cache = ClusterStartupRule.getCache();

    String query = queryTemplate;
    int i = 1;
    for (String r : regions) {
      query = query.replace("?" + i, r);
      i++;
    }

    QCompiler compiler = new QCompiler();
    Set<String> regionsInQuery = null;
    try {
      CompiledValue compiledQuery = compiler.compileQuery(query);
      Set regionSet = new HashSet();
      compiledQuery.getRegionsInQuery(regionSet, null);// GFSH ENV VARIBLES
      regionsInQuery = Collections.unmodifiableSet(regionSet);
      getLogWriter().info("Region in query : " + regionsInQuery);
      if (regionsInQuery.size() > 0) {
        Set<DistributedMember> members =
            CliUtil.getQueryRegionsAssociatedMembers(regionsInQuery, cache, returnAll);
        getLogWriter().info("Members for Region in query : " + members);
        if (expectedMembers != -1) {
          assertNotNull(members);
          assertEquals(expectedMembers, members.size());
        } else
          assertEquals(0, members.size());
      } else {
        assertEquals(-1, expectedMembers);// Regions do not exist at all
      }
    } catch (QueryInvalidException qe) {
      fail("Invalid Query", qe);
    }
  }

  public void doTestGetRegionAssociatedMembersForSelect() {
    final String queryTemplate1 = "select * from ?1, ?2 ";

    locator.invoke(() -> {
      doQueryRegionsAssociatedMembers(queryTemplate1, 0, true, DATA_REGION_NAME_VM1_PATH,
          DATA_REGION_NAME_VM2_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 2, true, DATA_REGION_NAME_PATH,
          DATA_REGION_NAME_CHILD_1_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 1, false, DATA_REGION_NAME_PATH,
          DATA_REGION_NAME_CHILD_1_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 1, true, DATA_REGION_NAME_VM1_PATH,
          DATA_REGION_NAME_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 1, false, DATA_REGION_NAME_VM1_PATH,
          DATA_REGION_NAME_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 1, true, DATA_REGION_NAME_VM2_PATH,
          DATA_REGION_NAME_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 1, false, DATA_REGION_NAME_VM2_PATH,
          DATA_REGION_NAME_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 0, true, DATA_PAR_REGION_NAME_VM2_PATH,
          DATA_PAR_REGION_NAME_VM1_PATH);
      doQueryRegionsAssociatedMembers(queryTemplate1, 0, false, DATA_PAR_REGION_NAME_VM2_PATH,
          DATA_PAR_REGION_NAME_VM1_PATH);
      // query with one valid region name and one invalid region name
      doQueryRegionsAssociatedMembers(queryTemplate1, -1, true, DATA_PAR_REGION_NAME_VM2_PATH,
          "/jfgkdfjgkd");
      // query with one valid region name and one invalid region name
      doQueryRegionsAssociatedMembers(queryTemplate1, -1, false, DATA_PAR_REGION_NAME_VM2_PATH,
          "/jfgkdfjgkd");
      // query with two invalid region names
      doQueryRegionsAssociatedMembers(queryTemplate1, -1, true, "/dhgfdhgf", "/dhgddhd");
      // query with two invalid region names
      doQueryRegionsAssociatedMembers(queryTemplate1, -1, false, "/dhgfdhgf", "/dhgddhd");
    });
  }

  public void doTestSelectProjection() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select ID , status , createTime , pk, floatMinValue from "
        + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger + "\" --interactive=false";
    CommandResult commandResult = gfsh.executeCommand(query);
    validateSelectResult(commandResult, true, (randomInteger + 1),
        new String[] {"ID", "status", "createTime", "pk", "floatMinValue"});
  }

  public void doTestSelectProjectionProcessCommand() {
    locator.invoke(() -> {
      Random random = new Random(System.nanoTime());
      int randomInteger = random.nextInt(COUNT);
      String query = "query --query=\"select ID , status , createTime , pk, floatMinValue from "
          + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger
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
    });
  }

  public void doTestSelectProjectionWithNestedField() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select employeeId, name, department, value2 from "
        + DATA_REGION_NAME_VM1_PATH + " where employeeId <= " + randomInteger
        + "\" --interactive=false";
    CommandResult cmdResult = gfsh.executeCommand(query);
    String expectedCols[] = {"employeeId", "name", "department", "value2"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);

    // Test with collections
    query =
        "query --query=\"select ID , status , createTime , pk, floatMinValue, collectionHolderMap from "
            + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger
            + "\" --interactive=false";
    cmdResult = gfsh.executeCommand(query);
    expectedCols =
        new String[] {"ID", "status", "createTime", "pk", "floatMinValue", "collectionHolderMap"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);
  }

  public void doTestSelectBeansAsResult() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select * from " + DATA_REGION_NAME_PATH
        + " where employeeId <= " + randomInteger + "\" --interactive=false";
    CommandResult cmdResult = gfsh.executeCommand(query);
    String expectedCols[] = {"name", "lastName", "department", "age", "employeeId"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);
  }

  public void doTestSelectBeansWithNestedFieldAsResult() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select employeeId, name, department, value2 from "
        + DATA_REGION_NAME_VM1_PATH + " where employeeId <= " + randomInteger
        + "\" --interactive=false";
    CommandResult cmdResult = gfsh.executeCommand(query);
    String expectedCols[] = {"employeeId", "name", "department", "value2"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);
  }

  public void doTestSelectWithGfshEnvironmentVariables(boolean statusActive) {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query =
        "query --query=\"select ID , status , createTime , pk, floatMinValue from ${DATA_REGION} where ID <= ${PORTFOLIO_ID}"
            + " and status=${STATUS}" + "\" --interactive=false";
    gfsh.executeCommand("set variable --name=DATA_REGION --value=" + DATA_REGION_NAME_PATH);
    gfsh.executeCommand("set variable --name=PORTFOLIO_ID --value=" + randomInteger);
    gfsh.executeCommand(
        "set variable --name=STATUS --value=" + (statusActive ? "active" : "inactive"));
    CommandResult cmdResult = gfsh.executeCommand(query);
    validateSelectResult(cmdResult, true, -1, null);
    IgnoredException ex =
        addIgnoredException(QueryInvalidException.class.getSimpleName(), locator.getVM());
    try {
      query =
          "query --query=\"select ID , status , createTime , pk, floatMinValue from ${DATA_REGION2} where ID <= ${PORTFOLIO_ID2}"
              + " and status=${STATUS2}" + "\" --interactive=false";
      cmdResult = gfsh.executeCommand(query);
      validateSelectResult(cmdResult, false, 0, null);
    } finally {
      ex.remove();
    }
  }

  public void selectWithMalformedQuery() {
    String query = "query --query=\"SELECT e FROM " + DATA_REGION_NAME_PATH
        + ".entries e\" --interactive=false";
    CommandResult cmdResult = gfsh.executeCommand(query);
    validateResult(cmdResult, true);
  }

  private void validateResult(CommandResult cmdResult, Boolean expected) {
    if (ResultData.TYPE_MODEL.equals(cmdResult.getType())) {
      ResultModel rd = (ResultModel) cmdResult.getResultData();
      DataResultModel result = rd.getDataSection(DataCommandResult.DATA_INFO_SECTION);
      assertThat(result.getContent().get("Result")).isEqualTo(expected.toString());
    } else
      fail("Expected CompositeResult Returned Result Type " + cmdResult.getType());
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
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);

    return props;
  }

  public static class Value1WithValue2 extends Value1 {
    private Value2 value2 = null;

    public Value1WithValue2(int i) {
      super(i);
      value2 = new Value2(i);
    }

    public Value2 getValue2() {
      return value2;
    }

    public void setValue2(Value2 value2) {
      this.value2 = value2;
    }
  }

}
