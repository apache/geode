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
package org.apache.geode.distributed;

import static java.util.Arrays.asList;
import static org.apache.geode.test.dunit.Disconnect.disconnectFromDS;
import static org.apache.geode.test.dunit.DistributedTestUtils.getAllDistributedSystemProperties;
import static org.apache.geode.test.dunit.standalone.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.SystemAdmin;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Distributed tests for {@link SystemAdmin}.
 */
public class SystemAdminDUnitTest {

  private static final String STACK_DUMP_FILE = "stack dump file created by printStacks";
  private static final String GF_GC_THREAD = "GemFire Garbage Collection";
  private static final String MANAGEMENT_TASK_THREAD = "Management Task";
  private static final String FUNCTION_THREAD = "Function Execution Processor";

  private Properties config;
  private String filename2;
  private String filename1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    // create a gemfire.properties that lets SystemAdmin find the dunit locator
    config = getAllDistributedSystemProperties(getDistributedSystemProperties());

    filename2 = new File(temporaryFolder.getRoot(), uniqueName + "2.txt").getAbsolutePath();
    filename1 = new File(temporaryFolder.getRoot(), uniqueName + "1.txt").getAbsolutePath();
  }

  @After
  public void tearDown() {
    disconnectFromDS();

    // SystemAdmin calls methods that set these static variables
    ClusterDistributionManager.setIsDedicatedAdminVM(false);
    SystemAdmin.setDistributedSystemProperties(null);
  }

  @Test
  public void testPrintStacks() {
    SystemAdmin.setDistributedSystemProperties(config);

    SystemAdmin.printStacks(asList(filename2), true);
    checkStackDumps(filename2, false);

    disconnectFromDS();

    SystemAdmin.printStacks(asList(filename1), false);
    checkStackDumps(filename1, true);
  }

  private void checkStackDumps(String filename, boolean isPruned) {
    File file = new File(filename);
    assertThat(file).as(STACK_DUMP_FILE).exists();

    if (isPruned) {
      LogFileAssert.assertThat(file).as(STACK_DUMP_FILE).doesNotContain(GF_GC_THREAD);
      LogFileAssert.assertThat(file).as(STACK_DUMP_FILE).doesNotContain(MANAGEMENT_TASK_THREAD);
      LogFileAssert.assertThat(file).as(STACK_DUMP_FILE).doesNotContain(FUNCTION_THREAD);

    } else {
      // the old code set foundGCThread = !isPruned, but the GF_GC_THREAD was never actually there
      LogFileAssert.assertThat(file).as(STACK_DUMP_FILE).doesNotContain(GF_GC_THREAD);
      LogFileAssert.assertThat(file).as(STACK_DUMP_FILE).contains(MANAGEMENT_TASK_THREAD);
      LogFileAssert.assertThat(file).as(STACK_DUMP_FILE).contains(FUNCTION_THREAD);
    }
  }
}
