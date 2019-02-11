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
package org.apache.geode.cache30;

import static org.apache.geode.cache30.ReconnectDUnitTest.locator;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class ReconnectWithClusterConfigurationDUnitTest implements Serializable {
  static final int NUM_VMS = 2;
  static DistributedSystem system;
  static Cache cache;
  static int locatorPort;
  static Properties dsProperties;

  @Rule
  public DistributedRule distributedRule = DistributedRule.builder().withVMCount(NUM_VMS).build();

  @Before
  public void setup() throws Exception {
    locatorPort = (int) VM.getVM(0).invoke("start locator", () -> {
      try {
        Disconnect.disconnectFromDS();
        dsProperties = null;
        Properties props = getDistributedSystemProperties();
        dsProperties.remove(LOCATORS);
        locator = Locator.startLocatorAndDS(0, new File(""), props);
        system = locator.getDistributedSystem();
        cache = ((InternalLocator) locator).getCache();
        ReconnectDUnitTest.savedSystem = locator.getDistributedSystem();
        IgnoredException.addIgnoredException(
            "org.apache.geode.ForcedDisconnectException||Possible loss of quorum");
      } catch (IOException e) {
        Assert.fail("unable to start locator", e);
      }
      return locator.getPort();
    });
    final int locPort = locatorPort;
    Invoke.invokeInEveryVM("set locator port", () -> locatorPort = locPort);
  }

  @After
  public void teardown() {
    Invoke.invokeInEveryVM(() -> {
      if (system != null) {
        system.disconnect();
      }
      system = null;
      cache = null;
    });
  }

  public Properties getDistributedSystemProperties() {
    dsProperties = new Properties();
    dsProperties.put(MAX_WAIT_TIME_RECONNECT, "10000");
    dsProperties.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    dsProperties.put(DISABLE_AUTO_RECONNECT, "false");
    dsProperties.put(ENABLE_CLUSTER_CONFIGURATION, "true");
    dsProperties.put(USE_CLUSTER_CONFIGURATION, "true");
    dsProperties.put(LOCATORS, "localHost[" + locatorPort + "]");
    dsProperties.put(MCAST_PORT, "0");
    dsProperties.put(MEMBER_TIMEOUT, "5000");
    dsProperties.put(LOG_LEVEL, "info");
    dsProperties.put(NAME, "vm" + VM.getCurrentVMNum());
    return dsProperties;
  }


  @Test
  public void testReconnectAfterMeltdown() throws InterruptedException {

    for (int i = 1; i < NUM_VMS; i++) {
      VM.getVM(i).invoke("create cache", () -> {
        cache = new CacheFactory(getDistributedSystemProperties()).create();
        system = cache.getDistributedSystem();
      });
    }
    AsyncInvocation[] crashers = new AsyncInvocation[NUM_VMS];
    for (int i = 0; i < NUM_VMS; i++) {
      crashers[i] = VM.getVM(i).invokeAsync("crash",
          () -> MembershipManagerHelper.crashDistributedSystem(system));
    }
    for (AsyncInvocation crasher : crashers) {
      crasher.join();
    }
    for (int i = NUM_VMS - 1; i >= 0; i--) {
      VM.getVM(i).invoke("wait for reconnect", () -> {
        system.waitUntilReconnected(GeodeAwaitility.getTimeout().getValueInMS(),
            TimeUnit.MILLISECONDS);
        system = system.getReconnectedSystem();
        cache = cache.getReconnectedCache();
      });
    }
  }

}
