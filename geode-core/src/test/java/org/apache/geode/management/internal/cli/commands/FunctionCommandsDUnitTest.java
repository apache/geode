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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.*;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;
import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * Dunit class for testing gemfire function commands : execute function, destroy function, list
 * function
 */
@Category(DistributedTest.class)
public class FunctionCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;
  private static final String REGION_NAME = "FunctionCommandsReplicatedRegion";
  private static final String REGION_ONE = "RegionOne";
  private static final String REGION_TWO = "RegionTwo";

  void setupWith2Regions() {
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    setUpJmxManagerOnVm0ThenConnect(null);

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        final Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create("RegionOne");
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
        region = dataRegionFactory.create("RegionTwo");
        for (int i = 0; i < 1000; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });


    vm2.invoke(new SerializableRunnable() {
      public void run() {
        final Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create("RegionOne");
        for (int i = 0; i < 10000; i++) {
          region.put("key" + (i + 400), "value" + (i + 400));
        }
        region = dataRegionFactory.create("Regiontwo");
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
  }

  @Test
  public void testExecuteFunctionWithNoRegionOnManager() throws Exception {
    setupWith2Regions();
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    FunctionService.registerFunction(function);
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
      }
    });
    Thread.sleep(2500);
    String command = "execute function --id=" + function.getId() + " --region=" + "/" + "RegionOne";
    getLogWriter().info("testExecuteFunctionWithNoRegionOnManager command : " + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testExecuteFunctionWithNoRegionOnManager stringResult : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("Execution summary"));
    } else {
      fail("testExecuteFunctionWithNoRegionOnManager failed as did not get CommandResult");
    }

  }

  public String getMemberId() {
    Cache cache = getCache();
    return cache.getDistributedSystem().getDistributedMember().getId();
  }

  @Test
  public void testExecuteFunctionOnRegion() {
    setUpJmxManagerOnVm0ThenConnect(null);

    final Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });

    String command = "execute function --id=" + function.getId() + " --region=" + REGION_NAME;
    getLogWriter().info("testExecuteFunctionOnRegion command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testExecuteFunctionOnRegion cmdResult=" + cmdResult);
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testExecuteFunctionOnRegion stringResult=" + stringResult);
      assertTrue(stringResult.contains("Execution summary"));
    } else {
      fail("testExecuteFunctionOnRegion did not return CommandResult");
    }
  }

  @Test
  public void testExecuteFunctionOnRegionWithCustomResultCollector() {
    setUpJmxManagerOnVm0ThenConnect(null);

    final Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RETURN_ARGS);
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });

    String command = "execute function --id=" + function.getId() + " --region=" + REGION_NAME
        + " --arguments=arg1,arg2" + " --result-collector="
        + ToUpperResultCollector.class.getName();
    getLogWriter().info("testExecuteFunctionOnRegion command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testExecuteFunctionOnRegion cmdResult=" + cmdResult);
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testExecuteFunctionOnRegion stringResult=" + stringResult);
      assertTrue(stringResult.contains("Execution summary"));
      assertTrue(stringResult.contains("ARG1"));
    } else {
      fail("testExecuteFunctionOnRegion did not return CommandResult");
    }
  }

  void setupForBug51480() {
    final VM vm1 = Host.getHost(0).getVM(1);
    setUpJmxManagerOnVm0ThenConnect(null);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        final Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create(REGION_ONE);
        for (int i = 0; i < 10; i++) {
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
              service.getDistributedRegionMXBean(Region.SEPARATOR + REGION_ONE);
          if (bean == null) {
            return false;
          } else {
            getLogWriter()
                .info("Probing for checkRegionMBeans testExecuteFunctionOnRegionBug51480 finished");
            return true;
          }
        }

        @Override
        public String description() {
          return "Probing for testExecuteFunctionOnRegionBug51480";
        }
      };
      waitForCriterion(waitForMaangerMBean, 2 * 60 * 1000, 2000, true);
      DistributedRegionMXBean bean = ManagementService.getManagementService(getCache())
          .getDistributedRegionMXBean(Region.SEPARATOR + REGION_ONE);
      assertNotNull(bean);
    }
  };

  @Test
  public void testExecuteFunctionOnRegionBug51480() {
    setupForBug51480();

    // check if DistributedRegionMXBean is available so that command will not fail
    final VM manager = Host.getHost(0).getVM(0);
    manager.invoke(checkRegionMBeans);

    final Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        FunctionService.registerFunction(function);
      }
    });

    String command = "execute function --id=" + function.getId() + " --region=" + REGION_ONE;

    getLogWriter().info("testExecuteFunctionOnRegionBug51480 command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      getLogWriter().info("testExecuteFunctionOnRegionBug51480 cmdResult=" + cmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testExecuteFunctionOnRegionBug51480 stringResult=" + stringResult);
      assertTrue(stringResult.contains("Execution summary"));
    } else {
      fail("testExecuteFunctionOnRegionBug51480 did not return CommandResult");

    }
  }

  @Test
  public void testExecuteFunctionOnMember() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    FunctionService.registerFunction(function);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1MemberId = (String) vm1.invoke(() -> getMemberId());

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });

    String command = "execute function --id=" + function.getId() + " --member=" + vm1MemberId;
    getLogWriter().info("testExecuteFunctionOnMember command=" + command);
    CommandResult cmdResult = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String stringResult = commandResultToString(cmdResult);
    getLogWriter().info("testExecuteFunctionOnMember stringResult:" + stringResult);
    assertTrue(stringResult.contains("Execution summary"));
  }

  @Test
  public void testExecuteFunctionOnMembers() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    FunctionService.registerFunction(function);
    final VM vm1 = Host.getHost(0).getVM(1);


    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });
    String command = "execute function --id=" + function.getId();
    getLogWriter().info("testExecuteFunctionOnMembers command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testExecuteFunctionOnMembers cmdResult:" + cmdResult);
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testExecuteFunctionOnMembers stringResult:" + stringResult);
      assertTrue(stringResult.contains("Execution summary"));
    } else {
      fail("testExecuteFunctionOnMembers did not return CommandResult");
    }
  }

  @Test
  public void testExecuteFunctionOnMembersWithArgs() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RETURN_ARGS);
    FunctionService.registerFunction(function);


    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RETURN_ARGS);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });

    String command = "execute function --id=" + function.getId() + " --arguments=arg1,arg2";

    getLogWriter().info("testExecuteFunctionOnMembersWithArgs command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testExecuteFunctionOnMembersWithArgs cmdResult:" + cmdResult);
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testExecuteFunctionOnMembersWithArgs stringResult:" + stringResult);
      assertTrue(stringResult.contains("Execution summary"));
      assertTrue(stringResult.contains("arg1"));
    } else {
      fail("testExecuteFunctionOnMembersWithArgs did not return CommandResult");
    }
  }

  @Test
  public void testExecuteFunctionOnMembersWithArgsAndCustomResultCollector() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RETURN_ARGS);
    FunctionService.registerFunction(function);


    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RETURN_ARGS);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });

    String command = "execute function --id=" + function.getId() + " --arguments=\"arg1,arg2\""
        + " --result-collector=" + ToUpperResultCollector.class.getName();

    getLogWriter().info("testExecuteFunctionOnMembersWithArgs command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testExecuteFunctionOnMembersWithArgs cmdResult:" + cmdResult);
      String stringResult = commandResultToString(cmdResult);
      getLogWriter().info("testExecuteFunctionOnMembersWithArgs stringResult:" + stringResult);
      assertTrue(stringResult.contains("Execution summary"));
      assertTrue(stringResult.contains("ARG1"));
    } else {
      fail("testExecuteFunctionOnMembersWithArgs did not return CommandResult");
    }
  }

  @Category(FlakyTest.class) // GEODE-1563: JMX RMI (java.rmi.NoSuchObjectException: no such object
                             // in table)
  @Test
  public void testExecuteFunctionOnGroups() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group0");
    setUpJmxManagerOnVm0ThenConnect(localProps);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    FunctionService.registerFunction(function);

    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    String vm1id = (String) vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties localProps = new Properties();
        localProps.setProperty(GROUPS, "Group1");
        getSystem(localProps);
        Cache cache = getCache();
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        return cache.getDistributedSystem().getDistributedMember().getId();
      }
    });

    String vm2id = (String) vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties localProps = new Properties();
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        Cache cache = getCache();
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        return cache.getDistributedSystem().getDistributedMember().getId();
      }
    });

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        RegionFactory<Integer, Integer> dataRegionFactory =
            getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create(REGION_NAME);
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        assertNotNull(region);
        FunctionService.registerFunction(function);
      }
    });

    String command =
        "execute function --id=" + TestFunction.TEST_FUNCTION1 + " --groups=Group1,Group2";
    getLogWriter().info("testExecuteFunctionOnGroups command=" + command);
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testExecuteFunctionOnGroups cmdResult=" + cmdResult);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
    List<String> members = resultData.retrieveAllValues("Member ID/Name");
    getLogWriter().info("testExecuteFunctionOnGroups members=" + members);
    assertTrue(members.size() == 2 && members.contains(vm1id) && members.contains(vm2id));
  }

  @Test
  public void testDestroyOnMember() {
    setUpJmxManagerOnVm0ThenConnect(null);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    FunctionService.registerFunction(function);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1MemberId = (String) vm1.invoke(() -> getMemberId());
    String command = "destroy function --id=" + function.getId() + " --member=" + vm1MemberId;
    getLogWriter().info("testDestroyOnMember command=" + command);
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      getLogWriter().info("testDestroyOnMember strCmdResult=" + strCmdResult);
      assertTrue(strCmdResult.contains("Destroyed TestFunction1 Successfully"));
    } else {
      fail("testDestroyOnMember failed as did not get CommandResult");
    }
  }

  @Test
  public void testDestroyOnGroups() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group0");
    setUpJmxManagerOnVm0ThenConnect(localProps);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    FunctionService.registerFunction(function);

    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    String vm1id = (String) vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties localProps = new Properties();
        localProps.setProperty(GROUPS, "Group1");
        getSystem(localProps);
        Cache cache = getCache();
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        return cache.getDistributedSystem().getDistributedMember().getId();
      }
    });


    String vm2id = (String) vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties localProps = new Properties();
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        Cache cache = getCache();
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
        return cache.getDistributedSystem().getDistributedMember().getId();
      }
    });

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);
        FunctionService.registerFunction(function);
      }
    });

    String command =
        "destroy function --id=" + TestFunction.TEST_FUNCTION1 + " --groups=Group1,Group2";
    getLogWriter().info("testDestroyOnGroups command=" + command);
    CommandResult cmdResult = executeCommand(command);
    getLogWriter().info("testDestroyOnGroups cmdResult=" + cmdResult);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String content = null;
    try {
      content = cmdResult.getContent().get("message").toString();
      getLogWriter().info("testDestroyOnGroups content = " + content);
    } catch (GfJsonException e) {
      fail("testDestroyOnGroups exception=" + e);
    }
    assertNotNull(content);
    assertTrue(content
        .equals("[\"Destroyed " + TestFunction.TEST_FUNCTION1 + " Successfully on " + vm1id + ","
            + vm2id + "\"]")
        || content.equals("[\"Destroyed " + TestFunction.TEST_FUNCTION1 + " Successfully on "
            + vm2id + "," + vm1id + "\"]"));
  }

  @Test
  public void testListFunction() {
    // Create the default setup, putting the Manager VM into Group1
    Properties localProps = new Properties();
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);

    // Find no functions
    CommandResult cmdResult = executeCommand(CliStrings.LIST_FUNCTION);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Functions Found"));

    // Add a function in the manager VM (VM 0)
    final Function function1 = new TestFunction(true, TestFunction.TEST_FUNCTION1);
    final VM managerVm = Host.getHost(0).getVM(0);
    managerVm.invoke(new SerializableRunnable() {
      public void run() {
        FunctionService.registerFunction(function1);
      }
    });

    // Add functions in another VM (VM 1)
    final Function function2 = new TestFunction(true, TestFunction.TEST_FUNCTION2);
    final Function function3 = new TestFunction(true, TestFunction.TEST_FUNCTION3);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        getCache();

        FunctionService.registerFunction(function2);
        FunctionService.registerFunction(function3);
      }
    });

    // Add functions in a third VM (VM 2)
    final Function function4 = new TestFunction(true, TestFunction.TEST_FUNCTION4);
    final Function function5 = new TestFunction(true, TestFunction.TEST_FUNCTION5);
    final Function function6 = new TestFunction(true, TestFunction.TEST_FUNCTION6);
    final VM vm2 = Host.getHost(0).getVM(2);
    final String vm2Name = "VM" + vm2.getPid();
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm2Name);
        localProps.setProperty(GROUPS, "Group3");
        getSystem(localProps);
        getCache();

        FunctionService.registerFunction(function4);
        FunctionService.registerFunction(function5);
        FunctionService.registerFunction(function6);
      }
    });

    // Find all functions
    cmdResult = executeCommand(CliStrings.LIST_FUNCTION);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    String stringResult = commandResultToString(cmdResult);
    assertEquals(8, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*Function"));
    assertTrue(stringContainsLine(stringResult, "Manager.*" + function1.getId()));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + function2.getId()));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + function3.getId()));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function4.getId()));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function5.getId()));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function6.getId()));

    // Find functions in group Group3
    cmdResult = executeCommand(CliStrings.LIST_FUNCTION + " --group=Group1,Group3");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(6, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*Function"));
    assertTrue(stringContainsLine(stringResult, "Manager.*" + function1.getId()));
    assertFalse(stringContainsLine(stringResult, vm1Name + ".*"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function4.getId()));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function5.getId()));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function6.getId()));

    // Find functions for Manager member
    cmdResult = executeCommand(CliStrings.LIST_FUNCTION + " --member=Manager," + vm1Name);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*Function"));
    assertTrue(stringContainsLine(stringResult, "Manager.*" + function1.getId()));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + function2.getId()));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + function3.getId()));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*"));

    // Find functions that match a pattern
    cmdResult = executeCommand(CliStrings.LIST_FUNCTION + " --matches=.*[135]$");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Member.*Function"));
    assertTrue(stringContainsLine(stringResult, "Manager.*" + function1.getId()));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + function2.getId()));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*" + function3.getId()));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + function4.getId()));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*" + function5.getId()));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + function6.getId()));
  }
}
