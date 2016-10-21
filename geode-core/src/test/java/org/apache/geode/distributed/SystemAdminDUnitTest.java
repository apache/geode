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

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemAdmin;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class SystemAdminDUnitTest extends JUnit4DistributedTestCase {

  @Override
  public final void postSetUp() throws Exception {
    disconnect();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnect();
  }

  public void disconnect() {
    // get rid of the command-line distributed system created by SystemAdmin
    nullSystem();
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && sys.isConnected()) {
      LogWriterUtils.getLogWriter().info("disconnecting(3)");
      sys.disconnect();
    }
  }

  @Category(FlakyTest.class) // GEODE-1585
  @Test
  public void testPrintStacks() throws Exception {

    // create a gemfire.properties that lets SystemAdmin find the dunit locator
    Properties p =
        DistributedTestUtils.getAllDistributedSystemProperties(getDistributedSystemProperties());
    try {

      SystemAdmin.setDistributedSystemProperties(p);

      String filename2 = getUniqueName() + "2.txt";
      List<String> options = new ArrayList<String>(1);
      options.add(filename2);
      SystemAdmin.printStacks(options, true);
      checkStackDumps(filename2, false);

      disconnect();

      String filename1 = getUniqueName() + "1.txt";
      options.clear();
      options.add(filename1);
      SystemAdmin.printStacks(options, false);
      checkStackDumps(filename1, true);

    } finally {
      // SystemAdmin calls methods that set these static variables
      DistributionManager.isDedicatedAdminVM = false;
      DistributionManager.isCommandLineAdminVM = false;
      SystemAdmin.setDistributedSystemProperties(null);
    }
  }

  private void checkStackDumps(String filename, boolean isPruned) throws IOException {
    File file = new File(filename);
    if (!file.exists()) {
      fail("printStacks did not create a stack dump");
    }
    BufferedReader in = new BufferedReader(new FileReader(file));
    // look for some threads that shouldn't be there
    boolean setting = !isPruned;
    boolean foundManagementTask = setting;
    boolean foundGCThread = setting;
    boolean foundFunctionThread = setting;
    String line;
    do {
      line = in.readLine();
      if (line != null) {
        if (line.contains("GemFire Garbage Collection"))
          foundGCThread = true;
        else if (line.contains("Management Task"))
          foundManagementTask = true;
        else if (line.contains("Function Execution Processor"))
          foundFunctionThread = true;
      }
    } while (line != null);

    if (isPruned) {
      assertFalse("found a GemFire Garbage Collection thread in stack dump in " + filename,
          foundGCThread);
      assertFalse("found a Management Task thread in stack dump in " + filename,
          foundManagementTask);
      assertFalse("found a Function Excecution Processor thread in stack dump in " + filename,
          foundFunctionThread);
    } else {
      assertTrue("found no GemFire Garbage Collection thread in stack dump in " + filename,
          foundGCThread);
      assertTrue("found no Management Task thread in stack dump in " + filename,
          foundManagementTask);
      assertTrue("found no Function Excecution Processor thread in stack dump in " + filename,
          foundFunctionThread);
    }
    file.delete();
  }
}
