/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli;

import static com.gemstone.gemfire.distributed.internal.DistributionConfig.*;
import static com.gemstone.gemfire.internal.AvailablePort.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * TODO : Add more tests for error-catch, different type of results etc
 */
@Category(IntegrationTest.class)
public class HeadlessGfshIntegrationTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @SuppressWarnings({"deprecation"})
  @Test
  public void testHeadlessGfshTest() throws ClassNotFoundException, IOException, InterruptedException {
    int port = getRandomAvailablePort(SOCKET);

    Properties properties = new Properties();
    properties.put(NAME_NAME, this.testName.getMethodName());
    properties.put(JMX_MANAGER_NAME, "true");
    properties.put(JMX_MANAGER_START_NAME, "true");
    properties.put(JMX_MANAGER_PORT_NAME, String.valueOf(port));
    properties.put(HTTP_SERVICE_PORT_NAME, "0");
    properties.put(MCAST_PORT_NAME, "0");

    DistributedSystem ds = DistributedSystem.connect(properties);
    GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.create(ds);

    HeadlessGfsh gfsh = new HeadlessGfsh("Test", 25, this.temporaryFolder.newFolder("gfsh_files").getCanonicalPath());
    for (int i = 0; i < 5; i++) {
      gfsh.executeCommand("connect --jmx-manager=localhost[" + port + "]");
      Object result = gfsh.getResult();
      assertTrue(gfsh.isConnectedAndReady());
      assertNotNull(result);
      gfsh.clear();
      gfsh.executeCommand("list members");
      result = gfsh.getResult();
      assertNotNull(result);
      gfsh.executeCommand("disconnect");
      gfsh.getResult();
    }

    long l1 = System.currentTimeMillis();
    gfsh.executeCommand("exit");
    long l2 = System.currentTimeMillis();
    gfsh.getResult();
    long l3 = System.currentTimeMillis();
    System.out.println("L3-l2=" + (l3 - l2) + " Total time= " + (l3 - l1) / 1000);
    gfsh.terminate();
    cache.close();
    ds.disconnect();
  }

}
