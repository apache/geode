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
package org.apache.geode.internal.util;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.management.internal.cli.commands.ExportStackTraceCommand;
import org.apache.geode.management.internal.cli.domain.StackTracesPerMember;
import org.apache.geode.management.internal.cli.functions.GetStackTracesFunction;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class PluckStacksDUnitTest extends JUnit4CacheTestCase {

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.NAME,
        "vm " + Integer.getInteger("gemfire.DUnitLauncher.VM_NUM", -1));
    return properties;
  }


  /**
   * Gfsh has an "export stack-traces" command that PluckStacks should be able to parse
   */
  @Test
  public void testPluckingStacksFromGfshExport() throws Exception {
    Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    server1.invoke(() -> {
      getCache();
    });
    server2.invoke(() -> {
      getCache();
    });
    DistributedSystem system = getSystem();

    Set<DistributedMember> targetMembers = system.getAllOtherMembers();
    assertEquals("expected 3 members but found " + targetMembers, 3, targetMembers.size());

    String outputFileName = "PluckStacksdUnitTest.out";
    File outputFile = new File(outputFileName);
    if (outputFile.exists()) {
      outputFile.delete();
    }

    System.out.println("dumping stacks for " + targetMembers);
    dumpStacksToFile(targetMembers, outputFileName);
    assertTrue(outputFile.exists());
    assertNotEquals(0, outputFile.length());

    Map<String, List<PluckStacks.ThreadStack>> result = new PluckStacks()
        .getThreadDumps(new LineNumberReader(new FileReader(outputFile)), outputFileName);
    assertEquals(targetMembers.size(), result.size());
    for (List<PluckStacks.ThreadStack> dump : result.values()) {
      // there should be thread stacks in the list
      assertNotEquals(0, dump.size());
    }

    // if the test fails the file will be left behind for examination
    if (outputFile.exists()) {
      outputFile.delete();
    }
  }

  private void dumpStacksToFile(Set<DistributedMember> targetMembers, String fileName)
      throws Exception {
    Map<String, byte[]> dumps = new HashMap<>();

    ResultCollector<?, ?> rc =
        ManagementUtils.executeFunction(new GetStackTracesFunction(), null, targetMembers);
    ArrayList<Object> resultList = (ArrayList<Object>) rc.getResult();
    assertEquals(targetMembers.size(), resultList.size());

    for (Object resultObj : resultList) {
      if (resultObj instanceof StackTracesPerMember) {
        StackTracesPerMember stackTracePerMember = (StackTracesPerMember) resultObj;
        dumps.put(stackTracePerMember.getMemberNameOrId(),
            stackTracePerMember.getStackTraces());
      } else {
        fail("expected a stack trace but found " + resultObj);
      }
    }
    assertEquals(targetMembers.size(), dumps.size());

    new ExportStackTraceCommand().writeStacksToFile(dumps, fileName);
  }
}
