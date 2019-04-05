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
package org.apache.geode.internal.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper.getMembershipManager;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class MeterSubregistryReconnectDistributedTest implements Serializable {

  private static final long TIMEOUT = getTimeout().getValueInMS();

  private static LocatorLauncher locatorLauncher;

  private static InternalDistributedSystem system;
  private static MeterRegistry addedSubregistry;
  private static MeterRegistry discoveredSubregistry;

  private VM locatorVM;
  private VM server1VM;
  private VM server2VM;

  private String locatorName;
  private String server1Name;
  private String server2Name;

  private File locatorDir;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    locatorName = "locator-" + testName.getMethodName();
    server1Name = "server-" + testName.getMethodName() + "-1";
    server2Name = "server-" + testName.getMethodName() + "-2";

    locatorVM = getVM(0);
    server1VM = getVM(1);
    server2VM = getController();

    locatorDir = temporaryFolder.newFolder(locatorName);

    int locatorPort = locatorVM.invoke(() -> createLocator());

    server1VM.invoke(() -> createServer(server1Name, locatorPort));
    server2VM.invoke(() -> createServer(server2Name, locatorPort));

    addIgnoredException(ForcedDisconnectException.class);
    addIgnoredException("Possible loss of quorum");
  }

  @After
  public void tearDown() {
    locatorVM.invoke(() -> {
      locatorLauncher.stop();
      locatorLauncher = null;
      system = null;
    });

    for (VM vm : toArray(server1VM, server2VM)) {
      vm.invoke(() -> {
        system.disconnect();
        system = null;
      });
    }
  }

  @Test
  public void meterSubregistryIsUsedAfterReconnect() {
    locatorVM.invoke(() -> {
      assertThat(system.getDistributionManager().getDistributionManagerIds()).hasSize(3);
    });

    server2VM.invoke(() -> {
      GMSMembershipManager membershipManager = (GMSMembershipManager) getMembershipManager(system);
      membershipManager.forceDisconnect("Forcing disconnect in " + testName.getMethodName());

      await().until(() -> system.isReconnecting());
      system.waitUntilReconnected(TIMEOUT, MILLISECONDS);
      assertThat(system.getReconnectedSystem()).isNotSameAs(system);
    });

    locatorVM.invoke(() -> {
      assertThat(system.getDistributionManager().getDistributionManagerIds()).hasSize(3);
    });

    server2VM.invoke(() -> {
      system = (InternalDistributedSystem) system.getReconnectedSystem();
      InternalCache cache = system.getCache();
      CompositeMeterRegistry compositeMeterRegistry =
          (CompositeMeterRegistry) cache.getMeterRegistry();
      assertThat(compositeMeterRegistry.getRegistries()).containsOnly(addedSubregistry);
    });
  }

  private int createLocator() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();
    builder.setMemberName(locatorName);
    builder.setWorkingDirectory(locatorDir.getAbsolutePath());
    builder.setPort(0);
    builder.set(DISABLE_AUTO_RECONNECT, "false");
    builder.set(ENABLE_CLUSTER_CONFIGURATION, "false");
    builder.set(MAX_WAIT_TIME_RECONNECT, "1000");
    builder.set(MEMBER_TIMEOUT, "2000");

    locatorLauncher = builder.build();
    locatorLauncher.start();

    system = (InternalDistributedSystem) locatorLauncher.getCache().getDistributedSystem();

    return locatorLauncher.getPort();
  }

  private void createServer(String serverName, int locatorPort) {
    Properties configProperties = new Properties();
    configProperties.setProperty(LOCATORS, "localHost[" + locatorPort + "]");
    configProperties.setProperty(DISABLE_AUTO_RECONNECT, "false");
    configProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    configProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "1000");
    configProperties.setProperty(MEMBER_TIMEOUT, "2000");
    configProperties.setProperty(NAME, serverName);

    addedSubregistry = new SimpleMeterRegistry();

    CacheFactory cacheFactory = new CacheFactory(configProperties);
    cacheFactory.addMeterSubregistry(addedSubregistry);

    InternalCache cache = (InternalCache) cacheFactory.create();

    CompositeMeterRegistry compositeMeterRegistry =
        (CompositeMeterRegistry) cache.getMeterRegistry();
    assertThat(compositeMeterRegistry.getRegistries()).contains(addedSubregistry);

    // same as a discovered ServerLoader that created a subregistry
    discoveredSubregistry = new SimpleMeterRegistry();
    compositeMeterRegistry.add(discoveredSubregistry);

    system = cache.getInternalDistributedSystem();
  }
}
