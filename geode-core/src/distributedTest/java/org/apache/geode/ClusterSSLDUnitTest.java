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
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_LEASE_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.util.test.TestUtil;

@Category({MembershipTest.class, BackwardCompatibilityTest.class})
public class ClusterSSLDUnitTest implements java.io.Serializable {

  private static final int NUM_SERVERS = 2;
  private static final int SMALL_BUFFER_SIZE = 8000;

  private static final long serialVersionUID = -3438183140385150550L;

  @Rule
  public DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(NUM_SERVERS + 1).build();

  @Rule
  public final SerializableTestName testName = new SerializableTestName();

  final String regionName = testName.getMethodName() + "_Region";


  @Test
  public void createEntryWithConserveSockets() throws Exception {
    int locatorPort = createLocator(VM.getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createSSLEnabledCacheAndRegion(VM.getVM(i), locatorPort, true);
    }
    performCreate(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyCreatedEntry(VM.getVM(i));
    }
    performUpdate(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyUpdatedEntry(VM.getVM(i));
    }
  }

  @Test
  public void createEntryWithThreadOwnedSockets() throws Exception {
    int locatorPort = createLocator(VM.getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createSSLEnabledCacheAndRegion(VM.getVM(i), locatorPort, false);
    }
    performCreate(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyCreatedEntry(VM.getVM(i));
    }
    performUpdate(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyUpdatedEntry(VM.getVM(i));
    }
  }

  @Test
  public void createEntryWithThreadOwnedSocketsAndBigMessage() throws Exception {
    int locatorPort = createLocator(VM.getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createSSLEnabledCacheAndRegion(VM.getVM(i), locatorPort, false);
    }
    performCreateWithLargeValue(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyCreatedEntry(VM.getVM(i));
    }
  }

  @Test
  public void performARollingUpgrade() throws Exception {
    List<String> testVersions = VersionManager.getInstance().getVersionsWithoutCurrent();
    Collections.sort(testVersions);
    String testVersion = testVersions.get(testVersions.size() - 1);

    // create a cluster with the previous version of Geode
    VM locatorVM = Host.getHost(0).getVM(testVersion, 0);
    VM server1VM = Host.getHost(0).getVM(testVersion, 1);
    int locatorPort = createLocator(locatorVM);
    createSSLEnabledCacheAndRegion(server1VM, locatorPort, true);
    performCreate(VM.getVM(1));

    // roll the locator to the current version
    locatorVM.invoke("stop locator", () -> Locator.getLocator().stop());
    locatorVM = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 0);
    locatorVM.invoke("roll locator to current version", () -> {
      // System.setProperty("javax.net.debug", "all");
      Properties props = getDistributedSystemProperties();
      // locator must restart with the same port so that it reconnects to the server
      GeodeAwaitility.await().atMost(15, TimeUnit.SECONDS)
          .until(() -> Locator.startLocatorAndDS(locatorPort, new File(""), props) != null);
      assertThat(Locator.getLocator().getDistributedSystem().getAllOtherMembers().size())
          .isGreaterThan(0);
    });

    // start server2 with current version
    VM server2VM = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 2);
    createSSLEnabledCacheAndRegion(server2VM, locatorPort, true);

    // roll server1 to the current version
    server1VM.invoke("stop server1", () -> {
      CacheFactory.getAnyInstance().getDistributedSystem().disconnect();
    });
    server1VM = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 1);
    createSSLEnabledCacheAndRegion(server1VM, locatorPort, true);


    verifyCreatedEntry(server1VM);
    verifyCreatedEntry(server2VM);
  }

  private void createSSLEnabledCacheAndRegion(VM memberVM, int locatorPort,
      boolean conserveSockets) {
    memberVM.invoke("start cache and create region", () -> {
      Cache cache = createCache(locatorPort, conserveSockets);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    });
  }


  private void performCreate(VM memberVM) {
    memberVM.invoke("perform create", () -> CacheFactory.getAnyInstance()
        .getRegion(regionName).put("testKey", "testValue"));
  }

  private void performUpdate(VM memberVM) {
    memberVM.invoke("perform update", () -> CacheFactory.getAnyInstance()
        .getRegion(regionName).put("testKey", "updatedTestValue"));
  }

  private void performCreateWithLargeValue(VM memberVM) {
    memberVM.invoke("perform create", () -> {
      byte[] value = new byte[SMALL_BUFFER_SIZE];
      Arrays.fill(value, (byte) 1);
      CacheFactory.getAnyInstance().getRegion(regionName).put("testKey", value);
    });
  }

  private void verifyCreatedEntry(VM memberVM) {
    memberVM.invoke("verify entry created", () -> Assert.assertTrue(CacheFactory.getAnyInstance()
        .getRegion(regionName).containsKey("testKey")));
  }

  private void verifyUpdatedEntry(VM memberVM) {
    memberVM.invoke("verify entry updated", () -> Assert.assertTrue(CacheFactory.getAnyInstance()
        .getRegion(regionName).containsValue("updatedTestValue")));
  }

  private int createLocator(VM memberVM) {
    return memberVM.invoke("create locator", () -> {
      // System.setProperty("javax.net.debug", "all");
      return Locator.startLocatorAndDS(0, new File(""), getDistributedSystemProperties()).getPort();
    });
  }

  private Cache createCache(int locatorPort, boolean conserveSockets) {
    // System.setProperty("javax.net.debug", "all");
    Properties properties = getDistributedSystemProperties();
    properties.put(ConfigurationProperties.LOCATORS, "localhost[" + locatorPort + "]");
    properties.put(ConfigurationProperties.CONSERVE_SOCKETS, "" + conserveSockets);
    return new CacheFactory(properties).create();
  }

  public Properties getDistributedSystemProperties() {
    Properties properties = new Properties();
    // properties.put(ConfigurationProperties.LOG_LEVEL, "fine");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    properties.put(USE_CLUSTER_CONFIGURATION, "false");
    properties.put(SSL_ENABLED_COMPONENTS, "cluster");
    properties.put(SSL_KEYSTORE, TestUtil.getResourcePath(this.getClass(), "server.keystore"));
    properties.put(SSL_TRUSTSTORE, TestUtil.getResourcePath(this.getClass(), "server.keystore"));
    properties.put(SSL_PROTOCOLS, "TLSv1.2");
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.put(SOCKET_LEASE_TIME, "10000");
    // properties.put(JMX_MANAGER_UPDATE_RATE, "180000"); // eliminate noise for debugging
    properties.put(NAME, "vm" + VM.getCurrentVMNum());
    properties.put(SOCKET_BUFFER_SIZE, "" + SMALL_BUFFER_SIZE);
    return properties;
  }
}
