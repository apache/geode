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

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNotSame;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.dto.Value1;
import org.apache.geode.management.internal.cli.dto.Value2;
import org.apache.geode.management.internal.cli.json.GfJsonArray;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 * Dunit class for testing gemfire data commands : get, put, remove, select, rebalance
 */
@Category({DistributedTest.class, FlakyTest.class}) // GEODE-1182 GEODE-1249 GEODE-1404 GEODE-1430
                                                    // GEODE-1487 GEODE-1496 GEODE-1561 GEODE-1822
                                                    // GEODE-2006 GEODE-3530
@SuppressWarnings("serial")
public class GemfireDataCommandsDUnitTest extends CliCommandTestBase {

  private static final String REGION_NAME = "FunctionCommandsReplicatedRegion";
  private static final String REBALANCE_REGION_NAME = "GemfireDataCommandsDUnitTestRegion";
  private static final String REBALANCE_REGION2_NAME = "GemfireDataCommandsDUnitTestRegion2";
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


  private static final String keyTemplate = "('id':'?','name':'name?')";
  private static final String valueTemplate =
      "('stateName':'State?','population':?1,'capitalCity':'capital?','areaInSqKm':?2)";
  private static final String carTemplate =
      "\"('attributes':?map,'make':'?make','model':'?model','colors':?list,'attributeSet':?set)\"";

  static final int COUNT = 5;

  public String getMemberId() {
    InternalCache cache = getCache();
    return cache.getDistributedSystem().getDistributedMember().getId();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.cli.dto.**");
    return properties;
  }

  void setupForGetPutRemoveLocateEntry(String testName) {
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    Properties props = getDistributedSystemProperties();
    props.setProperty(NAME, testName + "Manager");
    HeadlessGfsh gfsh = setUpJmxManagerOnVm0ThenConnect(props);
    assertNotNull(gfsh);
    assertEquals(true, gfsh.isConnectedAndReady());

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        InternalCache cache = getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
        Region dataRegion = regionFactory.create(DATA_REGION_NAME);
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        dataRegion =
            dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1, dataRegion.getAttributes());
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        dataRegion =
            dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1_2, dataRegion.getAttributes());
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        dataRegion = regionFactory.create(DATA_REGION_NAME_VM1);
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        PartitionAttributes partitionAttrs =
            new PartitionAttributesFactory().setRedundantCopies(2).create();
        RegionFactory<Object, Object> partitionRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        partitionRegionFactory.setPartitionAttributes(partitionAttrs);
        Region dataParRegion = partitionRegionFactory.create(DATA_PAR_REGION_NAME);
        assertNotNull(dataParRegion);
        getLogWriter().info("Created Region " + dataParRegion);
        dataParRegion = partitionRegionFactory.create(DATA_PAR_REGION_NAME_VM1);
        assertNotNull(dataParRegion);
        getLogWriter().info("Created Region " + dataParRegion);

      }
    });

    vm2.invoke(new SerializableRunnable() {
      public void run() {
        InternalCache cache = getCache();
        RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
        Region dataRegion = regionFactory.create(DATA_REGION_NAME);
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        dataRegion =
            dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1, dataRegion.getAttributes());
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        dataRegion =
            dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1_2, dataRegion.getAttributes());
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);

        dataRegion = regionFactory.create(DATA_REGION_NAME_VM2);
        assertNotNull(dataRegion);
        getLogWriter().info("Created Region " + dataRegion);


        PartitionAttributes partitionAttrs =
            new PartitionAttributesFactory().setRedundantCopies(2).create();
        RegionFactory<Object, Object> partitionRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        partitionRegionFactory.setPartitionAttributes(partitionAttrs);
        Region dataParRegion = partitionRegionFactory.create(DATA_PAR_REGION_NAME);
        assertNotNull(dataParRegion);
        getLogWriter().info("Created Region " + dataParRegion);
        dataParRegion = partitionRegionFactory.create(DATA_PAR_REGION_NAME_VM2);
        assertNotNull(dataParRegion);
        getLogWriter().info("Created Region " + dataParRegion);

      }
    });

    final String vm1MemberId = vm1.invoke(() -> getMemberId());
    final String vm2MemberId = vm2.invoke(() -> getMemberId());
    getLogWriter().info("Vm1 ID : " + vm1MemberId);
    getLogWriter().info("Vm2 ID : " + vm2MemberId);

    final VM manager = Host.getHost(0).getVM(0);

    SerializableRunnable checkRegionMBeans = new SerializableRunnable() {
      @Override
      public void run() {
        InternalCache cache = getCache();
        final ManagementService service = ManagementService.getManagementService(cache);

        final WaitCriterion waitForMaangerMBean = new WaitCriterion() {
          @Override
          public boolean done() {
            ManagerMXBean bean1 = service.getManagerMXBean();
            DistributedRegionMXBean bean2 =
                service.getDistributedRegionMXBean(DATA_REGION_NAME_PATH);
            if (bean1 == null) {
              getLogWriter().info("Still probing for ManagerMBean");
              return false;
            } else {
              getLogWriter().info("Still probing for DistributedRegionMXBean=" + bean2);
              return (bean2 != null);
            }
          }

          @Override
          public String description() {
            return "Probing for ManagerMBean";
          }
        };

        waitForCriterion(waitForMaangerMBean, 30000, 2000, true);

        assertNotNull(service.getMemberMXBean());
        assertNotNull(service.getManagerMXBean());
        DistributedRegionMXBean bean = service.getDistributedRegionMXBean(DATA_REGION_NAME_PATH);
        assertNotNull(bean);

        WaitCriterion waitForRegionMBeans = new WaitCriterion() {
          @Override
          public boolean done() {

            DistributedRegionMXBean beans[] = new DistributedRegionMXBean[6];
            beans[0] = service.getDistributedRegionMXBean(DATA_REGION_NAME_PATH);
            beans[1] = service.getDistributedRegionMXBean(DATA_REGION_NAME_VM1_PATH);
            beans[2] = service.getDistributedRegionMXBean(DATA_REGION_NAME_VM2_PATH);
            beans[3] = service.getDistributedRegionMXBean(DATA_PAR_REGION_NAME_PATH);
            beans[4] = service.getDistributedRegionMXBean(DATA_PAR_REGION_NAME_VM1_PATH);
            beans[5] = service.getDistributedRegionMXBean(DATA_PAR_REGION_NAME_VM2_PATH);
            // SubRegion Bug : Proxy creation has some issues.
            // beans[6] = service.getDistributedRegionMXBean(DATA_REGION_NAME_CHILD_1_PATH);
            // beans[7] = service.getDistributedRegionMXBean(DATA_REGION_NAME_CHILD_1_2_PATH);
            boolean flag = true;
            for (DistributedRegionMXBean b : beans) {
              if (b == null) {
                flag = false;
                break;
              }
            }

            if (!flag) {
              getLogWriter().info("Still probing for regionMbeans " + DATA_REGION_NAME_PATH + "="
                  + beans[0] + " " + DATA_REGION_NAME_VM1_PATH + "=" + beans[1] + " "
                  + DATA_REGION_NAME_VM2_PATH + "=" + beans[2] + " " + DATA_PAR_REGION_NAME_PATH
                  + "=" + beans[3] + " " + DATA_PAR_REGION_NAME_VM1_PATH + "=" + beans[4] + " "
                  + DATA_PAR_REGION_NAME_VM2_PATH + "=" + beans[5] + " "
              // + DATA_REGION_NAME_CHILD_1_PATH
              // +"="+ beans[6] + " " + DATA_REGION_NAME_CHILD_1_2_PATH
              // +"=" + beans[7]
              );
              return false;
            } else {
              getLogWriter().info("Probing complete for regionMbeans " + DATA_REGION_NAME_PATH + "="
                  + beans[0] + " " + DATA_REGION_NAME_VM1_PATH + "=" + beans[1] + " "
                  + DATA_REGION_NAME_VM2_PATH + "=" + beans[2] + " " + DATA_PAR_REGION_NAME_PATH
                  + "=" + beans[3] + " " + DATA_PAR_REGION_NAME_VM1_PATH + "=" + beans[4] + " "
                  + DATA_PAR_REGION_NAME_VM2_PATH + "=" + beans[5] + " "
              // + DATA_REGION_NAME_CHILD_1_PATH
              // +"="+ beans[6] + " " + DATA_REGION_NAME_CHILD_1_2_PATH
              // +"=" + beans[7]
              );
              // Fails here Rishi Need Fix here
              // if(bean1.getMemberCount()==2 && bean1.getMemberCount()==1 &&
              // bean1.getMemberCount()==1)
              return true;
              // else{
              // getLogWriter().info("Still probing for regionMbeans for aggregation bean1=" +
              // bean1.getMemberCount() + " bean2="+ bean2.getMemberCount() + " bean3" +
              // bean3.getMemberCount());
              // return false;
              // }
            }
          }

          @Override
          public String description() {
            return "Probing for regionMbeans";
          }
        };

        waitForCriterion(waitForRegionMBeans, 30000, 2000, true);

        String regions[] = {DATA_REGION_NAME_PATH, DATA_REGION_NAME_VM1_PATH,
            DATA_REGION_NAME_VM2_PATH, DATA_PAR_REGION_NAME_PATH, DATA_PAR_REGION_NAME_VM1_PATH,
            DATA_PAR_REGION_NAME_VM2_PATH, /*
                                            * DATA_REGION_NAME_CHILD_1_PATH,
                                            * DATA_REGION_NAME_CHILD_1_2_PATH
                                            */};

        for (String region : regions) {
          bean = service.getDistributedRegionMXBean(region);
          assertNotNull(bean);
          String[] membersName = bean.getMembers();
          getLogWriter().info("Members Array for region " + region + " : "
              + StringUtils.objectToString(membersName, true, 10));
          if (bean.getMemberCount() < 1)
            fail("Even after waiting mbean reports number of member hosting region "
                + DATA_REGION_NAME_VM1_PATH + " is less than one");
          // assertIndexDetailsEquals(1, membersName.length); //exists in one members vm1
        }
      }
    };
    manager.invoke(checkRegionMBeans);
  }

  void setupForSelect() {
    setupForGetPutRemoveLocateEntry("setupForSelect");
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);

    // To avoid pagination issues and Gfsh waiting for user input
    executeCommand("set variable --name=APP_FETCH_SIZE --value=" + COUNT);

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = CacheFactory.getAnyInstance();
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
      }
    });

    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = CacheFactory.getAnyInstance();
        String regions[] = {DATA_REGION_NAME_VM2_PATH, DATA_PAR_REGION_NAME_VM2_PATH};
        for (String r : regions) {
          Region dataRegion = cache.getRegion(r);
          for (int j = 0; j < 10; j++) {
            dataRegion.put(new Integer(j), new Portfolio(j));
          }
        }
      }
    });
  }

  private void doQueryRegionsAssociatedMembers(String queryTemplate, int expectedMembers,
      boolean returnAll, String... regions) {
    InternalCache cache = getCache();

    String query = queryTemplate;
    int i = 1;
    for (String r : regions) {
      query = query.replace("?" + i, r);
      i++;
    }
    getLogWriter().info("Checking members for query : " + query);
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
    final VM manager = Host.getHost(0).getVM(0);
    final String queryTemplate1 = "select * from ?1, ?2 ";
    // final String queryTemplate2 = "select * from ?1, ?2, ?3";
    manager.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
        doQueryRegionsAssociatedMembers(queryTemplate1, -1, true, DATA_PAR_REGION_NAME_VM2_PATH,
            "/jfgkdfjgkd"); // one wrong region
        doQueryRegionsAssociatedMembers(queryTemplate1, -1, false, DATA_PAR_REGION_NAME_VM2_PATH,
            "/jfgkdfjgkd"); // one wrong region
        doQueryRegionsAssociatedMembers(queryTemplate1, -1, true, "/dhgfdhgf", "/dhgddhd"); // both
        // regions
        // wrong
        doQueryRegionsAssociatedMembers(queryTemplate1, -1, false, "/dhgfdhgf", "/dhgddhd"); // both
        // regions
        // wrong
      }
    });
  }

  public void doTestSelectProjection() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select ID , status , createTime , pk, floatMinValue from "
        + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger + "\" --interactive=false";
    CommandResult cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    validateSelectResult(cmdResult, true, (randomInteger + 1),
        new String[] {"ID", "status", "createTime", "pk", "floatMinValue"});
  }

  public void doTestSelectProjectionProcessCommand() {
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Random random = new Random(System.nanoTime());
        int randomInteger = random.nextInt(COUNT);
        String query = "query --query=\"select ID , status , createTime , pk, floatMinValue from "
            + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger
            + "\" --interactive=false";
        ManagementService service = ManagementService.getExistingManagementService(getCache());
        MemberMXBean member = service.getMemberMXBean();
        String cmdResult = member.processCommand(query);
        assertNotNull(cmdResult);
        getLogWriter().info("Text Command Output : " + cmdResult);
      }
    });
  }

  public void doTestSelectProjectionWithNestedField() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select employeeId, name, department, value2 from "
        + DATA_REGION_NAME_VM1_PATH + " where employeeId <= " + randomInteger
        + "\" --interactive=false";
    CommandResult cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    String expectedCols[] = {"employeeId", "name", "department", "value2"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);

    // Test with collections
    query =
        "query --query=\"select ID , status , createTime , pk, floatMinValue, collectionHolderMap from "
            + DATA_PAR_REGION_NAME_PATH + " where ID <= " + randomInteger
            + "\" --interactive=false";
    cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    expectedCols =
        new String[] {"ID", "status", "createTime", "pk", "floatMinValue", "collectionHolderMap"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);
  }

  public void doTestSelectBeansAsResult() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select * from " + DATA_REGION_NAME_PATH
        + " where employeeId <= " + randomInteger + "\" --interactive=false";
    CommandResult cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    String expectedCols[] = {"name", "lastName", "department", "age", "employeeId"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);
  }

  public void doTestSelectBeansWithNestedFieldAsResult() {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query = "query --query=\"select employeeId, name, department, value2 from "
        + DATA_REGION_NAME_VM1_PATH + " where employeeId <= " + randomInteger
        + "\" --interactive=false";
    CommandResult cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    String expectedCols[] = {"employeeId", "name", "department", "value2"};
    validateSelectResult(cmdResult, true, (randomInteger + 1), expectedCols);
  }

  public void doTestSelectWithGfshEnvVariables(boolean statusActive) {
    Random random = new Random(System.nanoTime());
    int randomInteger = random.nextInt(COUNT);
    String query =
        "query --query=\"select ID , status , createTime , pk, floatMinValue from ${DATA_REGION} where ID <= ${PORTFOLIO_ID}"
            + " and status=${STATUS}" + "\" --interactive=false";
    executeCommand("set variable --name=DATA_REGION --value=" + DATA_REGION_NAME_PATH);
    executeCommand("set variable --name=PORTFOLIO_ID --value=" + randomInteger);
    executeCommand("set variable --name=STATUS --value=" + (statusActive ? "active" : "inactive"));
    CommandResult cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    validateSelectResult(cmdResult, true, -1, null);
    IgnoredException ex =
        addIgnoredException(QueryInvalidException.class.getSimpleName(), Host.getHost(0).getVM(0));
    try {
      query =
          "query --query=\"select ID , status , createTime , pk, floatMinValue from ${DATA_REGION2} where ID <= ${PORTFOLIO_ID2}"
              + " and status=${STATUS2}" + "\" --interactive=false";
      cmdResult = executeCommand(query);
      printCommandOutput(cmdResult);
      validateSelectResult(cmdResult, false, 0, null);
    } finally {
      ex.remove();
    }
  }

  public void doTestBug48013() {
    String query = "query --query=\"SELECT e FROM " + DATA_REGION_NAME_PATH
        + ".entries e\" --interactive=false";
    CommandResult cmdResult = executeCommand(query);
    printCommandOutput(cmdResult);
    validateResult(cmdResult, true);
  }

  @Test
  public void testSelectCommand() {
    setupForSelect();
    doTestGetRegionAssociatedMembersForSelect();
    doTestSelectWithGfshEnvVariables(true);
    doTestSelectWithGfshEnvVariables(false);
    doTestSelectProjection();
    doTestBug48013();
    doTestSelectProjectionProcessCommand();
    doTestSelectProjectionWithNestedField();
    doTestSelectBeansAsResult();
    doTestSelectBeansWithNestedFieldAsResult();
  }

  @Test
  public void testPrimitivesWithDataCommands() {
    setupForGetPutRemoveLocateEntry("testPrimitives");
    Byte byteKey = Byte.parseByte("41");
    Byte byteValue = Byte.parseByte("31");
    Short shortKey = Short.parseShort("123");
    Short shortValue = Short.parseShort("121");
    Integer integerKey = Integer.parseInt("123456");
    Integer integerValue = Integer.parseInt("12345678");
    Float floatKey = Float.valueOf("12432.2325");
    Float flaotValue = Float.valueOf("111111.1111");
    Double doubleKey = Double.valueOf("12432.235425");
    Double doubleValue = Double.valueOf("111111.111111");

    getLogWriter().info("Testing Byte Wrappers");
    testGetPutLocateEntryFromShellAndGemfire(byteKey, byteValue, Byte.class, true, true);
    getLogWriter().info("Testing Short Wrappers");
    testGetPutLocateEntryFromShellAndGemfire(shortKey, shortValue, Short.class, true, true);
    getLogWriter().info("Testing Integer Wrappers");
    testGetPutLocateEntryFromShellAndGemfire(integerKey, integerValue, Integer.class, true, true);
    getLogWriter().info("Testing Float Wrappers");
    testGetPutLocateEntryFromShellAndGemfire(floatKey, flaotValue, Float.class, true, true);
    getLogWriter().info("Testing Double Wrappers");
    testGetPutLocateEntryFromShellAndGemfire(doubleKey, doubleValue, Double.class, true, true);
  }

  private void testGetPutLocateEntryFromShellAndGemfire(final Serializable key,
      final Serializable value, Class klass, boolean addRegionPath, boolean expResult) {

    final VM vm1 = Host.getHost(0).getVM(1);

    SerializableRunnable putTask = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(DATA_REGION_NAME_PATH);
        assertNotNull(region);
        region.clear();
        region.put(key, value);
      }
    };

    SerializableRunnable getTask = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(DATA_REGION_NAME_PATH);
        assertNotNull(region);
        assertEquals(true, region.containsKey(key));
        assertEquals(value, region.get(key));
      }
    };

    SerializableRunnable removeTask = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(DATA_REGION_NAME_PATH);
        assertNotNull(region);
        assertEquals(true, region.containsKey(key));
        region.remove(key);
      }
    };


    SerializableRunnable clearTask = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(DATA_REGION_NAME_PATH);
        assertNotNull(region);
        region.clear();
      }
    };

    String getCommand = "get --key=" + key + " --key-class=" + klass.getCanonicalName()
        + " --value-class=" + klass.getCanonicalName();
    if (addRegionPath)
      getCommand += " --region=" + DATA_REGION_NAME_PATH;

    String locateEntryCommand = "locate entry --key=" + key + " --key-class="
        + klass.getCanonicalName() + " --value-class=" + klass.getCanonicalName();
    if (addRegionPath)
      locateEntryCommand += " --region=" + DATA_REGION_NAME_PATH;

    String removeCommand = "remove --key=" + key + " --key-class=" + klass.getCanonicalName();
    if (addRegionPath)
      removeCommand += " --region=" + DATA_REGION_NAME_PATH;
    String putCommand = "put --key=" + key + " --key-class=" + klass.getCanonicalName()
        + " --value=" + value + " --value-class=" + klass.getCanonicalName();
    if (addRegionPath)
      putCommand += " --region=" + DATA_REGION_NAME_PATH;

    if (expResult) {
      // Do put from shell check gemfire get do gemfire remove
      CommandResult cmdResult = executeCommand(putCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, true);
      vm1.invoke(getTask);
      vm1.invoke(removeTask);

      vm1.invoke(clearTask);

      // Do put from gemfire check from shell do gemfire remove
      vm1.invoke(putTask);
      cmdResult = executeCommand(getCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, true);
      cmdResult = executeCommand(locateEntryCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, true);
      vm1.invoke(removeTask);

      vm1.invoke(clearTask);

      // Do put from shell check from gemfire do remove from shell get from shell exepct false
      cmdResult = executeCommand(putCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, true);
      vm1.invoke(getTask);
      cmdResult = executeCommand(removeCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, true);
      cmdResult = executeCommand(getCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, false);
      cmdResult = executeCommand(locateEntryCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, false);
    } else {
      // Do put from shell check gemfire get do gemfire remove
      CommandResult cmdResult = executeCommand(putCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, false);
      vm1.invoke(clearTask);

      // Do put from gemfire check from shell do gemfire remove
      vm1.invoke(putTask);
      cmdResult = executeCommand(getCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, false);
      cmdResult = executeCommand(locateEntryCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, false);
      vm1.invoke(removeTask);
      vm1.invoke(clearTask);

      // Do put from shell check from gemfire do remove from shell get from shell exepct false
      cmdResult = executeCommand(putCommand);
      printCommandOutput(cmdResult);
      validateResult(cmdResult, false);
    }
  }


  private void validateResult(CommandResult cmdResult, boolean expected) {
    if (ResultData.TYPE_COMPOSITE.equals(cmdResult.getType())) {
      CompositeResultData rd = (CompositeResultData) cmdResult.getResultData();
      SectionResultData section = rd.retrieveSectionByIndex(0);
      boolean result = (Boolean) section.retrieveObject("Result");
      assertEquals(expected, result);
    } else
      fail("Expected CompositeResult Returned Result Type " + cmdResult.getType());
  }

  private void validateLocationsResult(CommandResult cmdResult, int expected) {
    if (ResultData.TYPE_COMPOSITE.equals(cmdResult.getType())) {
      CompositeResultData rd = (CompositeResultData) cmdResult.getResultData();
      SectionResultData section = rd.retrieveSectionByIndex(0);
      int result = (Integer) section.retrieveObject("Locations Found");
      assertEquals(expected, result);
    } else
      fail("Expected CompositeResult Returned Result Type " + cmdResult.getType());
  }

  private void validateJSONGetResult(CommandResult cmdResult, String[] expectedCols) {
    CompositeResultData rd = (CompositeResultData) cmdResult.getResultData();
    SectionResultData section = rd.retrieveSectionByIndex(0);
    TabularResultData table = section.retrieveTableByIndex(0);
    GfJsonArray array = table.getHeaders();
    assertEquals(expectedCols.length, array.size());
    try {
      for (String col : expectedCols) {
        boolean found = false;
        getLogWriter().info("Validating column " + col);
        for (int i = 0; i < array.size(); i++) {
          String header = (String) array.get(i);
          if (col.equals(header))
            found = true;
        }
        assertEquals(true, found);
      }
    } catch (GfJsonException e) {
      fail("Error accessing table data", e);
    }
  }

  private void validateJSONGetResultValues(CommandResult cmdResult, String[] expectedCols) {
    CompositeResultData rd = (CompositeResultData) cmdResult.getResultData();
    SectionResultData section = rd.retrieveSectionByIndex(0);
    TabularResultData table = section.retrieveTableByIndex(0);
    GfJsonArray array = table.getHeaders();
    assertEquals(expectedCols.length, array.size());
    try {
      for (String col : expectedCols) {
        boolean found = false;
        getLogWriter().info("Validating column " + col);
        for (int i = 0; i < array.size(); i++) {
          String header = (String) array.get(i);
          if (col.equals(header))
            found = true;
        }
        assertEquals(true, found);

        List<String> values = table.retrieveAllValues(col);
        for (String value : values) {
          assertNotSame("null", value);
        }

      }
    } catch (GfJsonException e) {
      fail("Error accessing table data", e);
    }
  }

  private void validateSelectResult(CommandResult cmdResult, boolean expectedFlag, int expectedRows,
      String[] cols) {
    if (ResultData.TYPE_COMPOSITE.equals(cmdResult.getType())) {
      CompositeResultData rd = (CompositeResultData) cmdResult.getResultData();
      SectionResultData section = rd.retrieveSectionByIndex(0);
      boolean result = (Boolean) section.retrieveObject("Result");
      assertEquals(expectedFlag, result);
      if (expectedFlag && expectedRows != -1) {
        int rowsReturned = (Integer) section.retrieveObject("Rows");
        assertEquals(expectedRows, rowsReturned);
        if (rowsReturned > 0 && cols != null) {
          try {
            TabularResultData table = section.retrieveTableByIndex(0);
            GfJsonArray array = table.getHeaders();
            assertEquals(cols.length, array.size());
            for (String col : cols) {
              boolean found = false;
              getLogWriter().info("Validating column " + col);
              for (int i = 0; i < array.size(); i++) {
                String header = (String) array.get(i);
                if (col.equals(header))
                  found = true;
              }
              assertEquals(true, found);
            }
          } catch (GfJsonException e) {
            fail("Error accessing table data", e);
          }
        }
      }
    } else
      fail("Expected CompositeResult Returned Result Type " + cmdResult.getType());
  }

  private Region<?, ?> createParReg(String regionName, Cache cache) {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    return regionFactory.create(regionName);
  }

  private Region<?, ?> createReplicatedRegion(String regionName, Cache cache) {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    return regionFactory.create(regionName);
  }

  private SerializableRunnable addCacheListenerInvocations(final String regionName) {
    return new SerializableRunnable() {
      public void run() {
        Region region = getCache().getRegion(regionName);
        region.getAttributesMutator().addCacheListener(new CountingCacheListener());
      }
    };
  }

  private SerializableRunnable verifyCacheListenerInvocations(final String regionName,
      boolean callbacksShouldHaveBeenInvoked) {
    return new SerializableRunnable() {
      public void run() {
        Region region = getCache().getRegion(regionName);
        CacheListener<?, ?>[] listeners = region.getAttributes().getCacheListeners();
        for (CacheListener<?, ?> listener : listeners) {
          if (listener instanceof CountingCacheListener) {
            CountingCacheListener ccl = (CountingCacheListener) listener;
            if (callbacksShouldHaveBeenInvoked) {
              assertNotEquals(0, ccl.getEvents());
            } else {
              assertEquals(0, ccl.getEvents());
            }
          }
        }
      }
    };
  }

  void setupWith2Regions() {
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    setUpJmxManagerOnVm0ThenConnect(null);

    vm1.invoke(new SerializableRunnable() {
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REBALANCE_REGION_NAME);
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
        region = dataRegionFactory.create(REBALANCE_REGION2_NAME);
        for (int i = 0; i < 50; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });

    vm2.invoke(new SerializableRunnable() {
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REBALANCE_REGION_NAME);
        for (int i = 0; i < 150; i++) {
          region.put("key" + (i + 400), "value" + (i + 400));
        }
        region = dataRegionFactory.create(REBALANCE_REGION2_NAME);
        for (int i = 0; i < 100; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
  }

  SerializableRunnable checkRegionMBeans = new SerializableRunnable() {
    @Override
    public void run() {
      final WaitCriterion waitForMaangerMBean = new WaitCriterion() {
        @Override
        public boolean done() {
          final ManagementService service = ManagementService.getManagementService(getCache());
          final DistributedRegionMXBean bean =
              service.getDistributedRegionMXBean(Region.SEPARATOR + REBALANCE_REGION_NAME);
          if (bean == null) {
            getLogWriter().info("Still probing for checkRegionMBeans ManagerMBean");
            return false;
          } else {
            // verify that bean is proper before executing tests
            return bean.getMembers() != null && bean.getMembers().length > 1
                && bean.getMemberCount() > 0
                && service.getDistributedSystemMXBean().listRegions().length >= 2;
          }
        }

        @Override
        public String description() {
          return "Probing for testRebalanceCommandForSimulateWithNoMember ManagerMBean";
        }
      };
      waitForCriterion(waitForMaangerMBean, 2 * 60 * 1000, 2000, true);
      DistributedRegionMXBean bean = ManagementService.getManagementService(getCache())
          .getDistributedRegionMXBean("/" + REBALANCE_REGION_NAME);
      assertNotNull(bean);
    }
  };

  @Test
  public void testRebalanceCommandForTimeOut() {
    setupTestRebalanceForEntireDS();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);
    getLogWriter().info("testRebalanceCommandForTimeOut verified Mbean and executin command");
    String command = "rebalance --time-out=1";
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testRebalanceCommandForTimeOut just after executing " + cmdResult);
    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testRebalanceCommandForTimeOut stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceCommandForTimeOut failed as did not get CommandResult");
    }
  }

  @Test
  public void testRebalanceCommandForTimeOutForRegion() {
    setupTestRebalanceForEntireDS();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter()
        .info("testRebalanceCommandForTimeOutForRegion verified Mbean and executin command");

    String command = "rebalance --time-out=1 --include-region=" + "/" + REBALANCE_REGION_NAME;
    CommandResult cmdResult = executeCommand(command);

    getLogWriter()
        .info("testRebalanceCommandForTimeOutForRegion just after executing " + cmdResult);

    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testRebalanceCommandForTimeOutForRegion stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceCommandForTimeOut failed as did not get CommandResult");
    }
  }

  @Test
  public void testRebalanceCommandForSimulate() {
    setupTestRebalanceForEntireDS();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter().info("testRebalanceCommandForSimulate verified Mbean and executing command");
    String command = "rebalance --simulate=true --include-region=" + "/" + REBALANCE_REGION_NAME;
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testRebalanceCommandForSimulate just after executing " + cmdResult);
    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testRebalanceCommandForSimulate stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceCommandForSimulate failed as did not get CommandResult");
    }
  }

  @Test
  public void testRebalanceCommandForSimulateWithNoMember() {
    setupTestRebalanceForEntireDS();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter()
        .info("testRebalanceCommandForSimulateWithNoMember verified Mbean and executin command");

    String command = "rebalance --simulate=true";
    CommandResult cmdResult = executeCommand(command);

    getLogWriter()
        .info("testRebalanceCommandForSimulateWithNoMember just after executing " + cmdResult);

    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testRebalanceCommandForSimulateWithNoMember stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceCommandForSimulateWithNoMember failed as did not get CommandResult");
    }
  }

  @Test
  public void testRebalanceForIncludeRegionFunction() {
    // setup();
    setupWith2Regions();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);
    getLogWriter()
        .info("testRebalanceForIncludeRegionFunction verified Mbean and executin command");
    String command =
        "rebalance --include-region=" + "/" + REBALANCE_REGION_NAME + ",/" + REBALANCE_REGION2_NAME;
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testRebalanceForIncludeRegionFunction just after executing " + cmdResult);
    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testRebalanceForIncludeRegionFunction stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceForIncludeRegionFunction failed as did not get CommandResult");
    }
  }

  @Test // FlakyTest: GEODE-1561
  public void testSimulateForEntireDS() {
    setupTestRebalanceForEntireDS();
    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter().info("testSimulateForEntireDS verified MBean and executing command");

    String command = "rebalance --simulate=true";

    CommandResult cmdResult = executeCommand(command);

    getLogWriter().info("testSimulateForEntireDS just after executing " + cmdResult);

    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testSimulateForEntireDS stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceForIncludeRegionFunction failed as did not get CommandResult");
    }
  }

  @Test
  public void testSimulateForEntireDSWithTimeout() {
    setupTestRebalanceForEntireDS();
    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter().info("testSimulateForEntireDS verified MBean and executing command");

    String command = "rebalance --simulate=true --time-out=-1";

    CommandResult cmdResult = executeCommand(command);

    getLogWriter().info("testSimulateForEntireDS just after executing " + cmdResult);

    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testSimulateForEntireDS stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceForIncludeRegionFunction failed as did not get CommandResult");
    }
  }

  @Test // FlakyTest: GEODE-1487
  public void testRebalanceForEntireDS() {
    setupTestRebalanceForEntireDS();
    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);
    getLogWriter().info("testRebalanceForEntireDS verified Mbean and executin command");
    String command = "rebalance";
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testRebalanceForEntireDS just after executing " + cmdResult);
    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testRebalanceForEntireDS stringResult : " + stringResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceForIncludeRegionFunction failed as did not get CommandResult");
    }
  }

  void setupTestRebalanceForEntireDS() {
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    setUpJmxManagerOnVm0ThenConnect(null);

    vm1.invoke(new SerializableRunnable() {
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REBALANCE_REGION_NAME);
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
        region = dataRegionFactory.create(REBALANCE_REGION_NAME + "Another1");
        for (int i = 0; i < 100; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });

    vm2.invoke(new SerializableRunnable() {
      public void run() {

        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REBALANCE_REGION_NAME);
        for (int i = 0; i < 100; i++) {
          region.put("key" + (i + 400), "value" + (i + 400));
        }
        region = dataRegionFactory.create(REBALANCE_REGION_NAME + "Another2");
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
  }

  private static void printCommandOutput(CommandResult cmdResult) {
    assertNotNull(cmdResult);
    getLogWriter().info("Command Output : ");
    StringBuilder sb = new StringBuilder();
    cmdResult.resetToFirstLine();
    while (cmdResult.hasNextLine()) {
      sb.append(cmdResult.nextLine()).append(DataCommandRequest.NEW_LINE);
    }
    getLogWriter().info(sb.toString());
    getLogWriter().info("");
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

  @Test
  public void testRebalanceForExcludeRegionFunction() {
    setupWith2Regions();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    getLogWriter()
        .info("testRebalanceForExcludeRegionFunction verified Mbean and executing command");

    String command = "rebalance --exclude-region=" + "/" + REBALANCE_REGION2_NAME;
    getLogWriter().info("testRebalanceForExcludeRegionFunction command : " + command);
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testRebalanceForExcludeRegionFunction just after executing " + cmdResult);
    if (cmdResult != null) {
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testRebalanceForExcludeRegionFunction stringResult : " + stringResult);
      assertEquals("CommandResult=" + cmdResult, Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testRebalanceForIncludeRegionFunction failed as did not get CommandResult");
    }
  }

  public void waitForListClientMbean(final String regionName) {
    final VM manager = Host.getHost(0).getVM(0);

    manager.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        final ManagementService service = ManagementService.getManagementService(cache);

        final WaitCriterion waitForMaangerMBean = new WaitCriterion() {
          @Override
          public boolean done() {
            ManagerMXBean bean1 = service.getManagerMXBean();
            DistributedRegionMXBean bean2 = service.getDistributedRegionMXBean(regionName);
            if (bean1 == null) {
              getLogWriter().info("waitForListClientMbean Still probing for ManagerMBean");
              return false;
            } else {
              getLogWriter().info(
                  "waitForListClientMbean Still probing for DistributedRegionMXBean=" + bean2);
              if (bean2 == null) {
                bean2 = service.getDistributedRegionMXBean(Region.SEPARATOR + regionName);
              }
              if (bean2 == null) {
                getLogWriter().info(
                    "waitForListClientMbean Still probing for DistributedRegionMXBean with separator = "
                        + bean2);
                return false;
              } else {
                getLogWriter().info(
                    "waitForListClientMbean Still probing for DistributedRegionMXBean with separator Not null  "
                        + bean2.getMembers().length);
                return bean2.getMembers().length > 1;
              }
            }
          }

          @Override
          public String description() {
            return "waitForListClientMbean Probing for ManagerMBean";
          }
        };

        waitForCriterion(waitForMaangerMBean, 30000, 2000, true);
        DistributedRegionMXBean bean = service.getDistributedRegionMXBean(regionName);
        if (bean == null) {
          bean = service.getDistributedRegionMXBean(Region.SEPARATOR + regionName);
        }
        assertNotNull(bean);
      }
    });

  }

  @Test
  public void testRegionsViaMbeanAndFunctions() {
    setupForGetPutRemoveLocateEntry("tesSimplePut");
    waitForListClientMbean(DATA_REGION_NAME_PATH);
    final VM manager = Host.getHost(0).getVM(0);

    String memSizeFromMbean = (String) manager.invoke(new SerializableCallable() {
      public Object call() {
        Cache cache = GemFireCacheImpl.getInstance();
        DistributedRegionMXBean bean = ManagementService.getManagementService(cache)
            .getDistributedRegionMXBean(DATA_REGION_NAME_PATH);

        if (bean == null)// try with slash ahead
          bean = ManagementService.getManagementService(cache)
              .getDistributedRegionMXBean(Region.SEPARATOR + DATA_REGION_NAME_PATH);

        if (bean == null) {
          return null;
        }

        String[] membersName = bean.getMembers();
        return "" + membersName.length;
      }
    });

    getLogWriter().info("testRegionsViaMbeanAndFunctions memSizeFromMbean= " + memSizeFromMbean);

    String memSizeFromFunctionCall = (String) manager.invoke(new SerializableCallable() {
      public Object call() {
        InternalCache cache = GemFireCacheImpl.getInstance();
        CliUtil.getRegionAssociatedMembers(DATA_REGION_NAME_PATH, cache, true);
        return "" + CliUtil.getRegionAssociatedMembers(DATA_REGION_NAME_PATH, cache, true).size();
      }
    });

    getLogWriter().info(
        "testRegionsViaMbeanAndFunctions memSizeFromFunctionCall= " + memSizeFromFunctionCall);
    assertTrue(memSizeFromFunctionCall.equals(memSizeFromMbean));
  }

  @Test
  public void testRegionsViaMbeanAndFunctionsForPartRgn() {
    setupWith2Regions();
    waitForListClientMbean(REBALANCE_REGION_NAME);
    final VM manager = Host.getHost(0).getVM(0);

    String memSizeFromMbean = (String) manager.invoke(new SerializableCallable() {
      public Object call() {
        Cache cache = GemFireCacheImpl.getInstance();
        DistributedRegionMXBean bean = ManagementService.getManagementService(cache)
            .getDistributedRegionMXBean(REBALANCE_REGION_NAME);

        if (bean == null) {
          bean = ManagementService.getManagementService(cache)
              .getDistributedRegionMXBean(Region.SEPARATOR + REBALANCE_REGION_NAME);
        }

        if (bean == null) {
          return null;
        }

        String[] membersName = bean.getMembers();
        return "" + membersName.length;
      }
    });

    getLogWriter()
        .info("testRegionsViaMbeanAndFunctionsForPartRgn memSizeFromMbean= " + memSizeFromMbean);

    String memSizeFromFunctionCall = (String) manager.invoke(new SerializableCallable() {
      public Object call() {
        InternalCache cache = GemFireCacheImpl.getInstance();
        return "" + CliUtil.getRegionAssociatedMembers(REBALANCE_REGION_NAME, cache, true).size();
      }
    });

    getLogWriter().info("testRegionsViaMbeanAndFunctionsForPartRgn memSizeFromFunctionCall= "
        + memSizeFromFunctionCall);
    assertTrue(memSizeFromFunctionCall.equals(memSizeFromMbean));
  }

  private static class CountingCacheListener extends CacheListenerAdapter {

    private final AtomicInteger events = new AtomicInteger();

    @Override
    public void afterCreate(EntryEvent event) {
      events.incrementAndGet();
    }

    private int getEvents() {
      return events.get();
    }
  }
}
