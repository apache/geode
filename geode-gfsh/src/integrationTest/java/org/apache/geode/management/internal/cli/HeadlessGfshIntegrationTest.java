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
package org.apache.geode.management.internal.cli;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.GfshTest;

/**
 * TODO : Add more tests for error-catch, different type of results etc
 */
@Category({GfshTest.class})
public class HeadlessGfshIntegrationTest {
  private int port;
  private DistributedSystem ds;
  private GemFireCacheImpl cache;
  private HeadlessGfsh gfsh;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() throws IOException {
    port = getRandomAvailablePort(SOCKET);

    Properties properties = new Properties();
    properties.put(NAME, this.testName.getMethodName());
    properties.put(JMX_MANAGER, "true");
    properties.put(JMX_MANAGER_START, "true");
    properties.put(JMX_MANAGER_PORT, String.valueOf(port));
    properties.put(HTTP_SERVICE_PORT, "0");
    properties.put(MCAST_PORT, "0");

    legacyConnect(properties);

    gfsh = new HeadlessGfsh("Test", 25,
        this.temporaryFolder.newFolder("gfsh_files").getCanonicalPath());
  }

  @SuppressWarnings("deprecation")
  private void legacyConnect(Properties properties) {
    ds = DistributedSystem.connect(properties);
    cache = (GemFireCacheImpl) CacheFactory.create(ds);
  }

  @SuppressWarnings({"deprecation"})
  @After
  public void cleanup() {
    gfsh.terminate();
    cache.close();
    ds.disconnect();
  }

  @Test
  public void testHeadlessGfshTest() throws InterruptedException {

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
  }

  @Test
  public void testStringResultReturnedAsCommandResult() throws InterruptedException {
    gfsh.clear();
    gfsh.executeCommand("list members");

    LinkedBlockingQueue<Object> headlessQueue = gfsh.getQueue();
    headlessQueue.clear();
    headlessQueue.put("ERROR RESULT");
    CommandResult result = gfsh.getResult();
    assertNotNull(result);
    assertSame(result.getStatus(), Result.Status.ERROR);
    System.out.println(result.toString());
  }
}
