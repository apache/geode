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

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.test.TestUtil;

@Category({MembershipTest.class})
public class ClusterSSLDUnitTest extends JUnit4DistributedTestCase {

  private static final int NUM_SERVERS = 2;
  private static final int SMALL_BUFFER_SIZE = 8000;

  @After
  public void tearDown() throws Exception {
    for (int i = 1; i <= NUM_SERVERS; i++) {
      VM.getVM(i).invoke("close cache", () -> {
        CacheFactory.getAnyInstance().getDistributedSystem().disconnect();
      });
    }
    VM.getVM(0).invoke("stop locator", () -> {
      Locator.getLocator().stop();
    });
  }

  @Test
  public void createEntryWithSSLEnabledAndConserveSockets() throws Exception {
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
  public void createEntryWithSSLEnabledAndThreadOwnedSockets() throws Exception {
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
  public void createEntryWithSSLEnabledThreadOwnedSocketsAndBigMessage() throws Exception {
    int locatorPort = createLocator(VM.getVM(0));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      createSSLEnabledCacheAndRegion(VM.getVM(i), locatorPort, false);
    }
    performCreateWithLargeValue(VM.getVM(1));
    for (int i = 1; i <= NUM_SERVERS; i++) {
      verifyCreatedEntry(VM.getVM(i));
    }
  }

  private void createSSLEnabledCacheAndRegion(VM memberVM, int locatorPort,
      boolean conserveSockets) {
    memberVM.invoke("start cache and create region", () -> {
      Cache cache = createCache(locatorPort, conserveSockets);
      cache.createRegionFactory(RegionShortcut.REPLICATE).create("ClusterSSLDUnitTestRegion");
    });
  }


  private void performCreate(VM memberVM) {
    memberVM.invoke("perform create", () -> CacheFactory.getAnyInstance()
        .getRegion("ClusterSSLDUnitTestRegion").put("testKey", "testValue"));
  }

  private void performUpdate(VM memberVM) {
    memberVM.invoke("perform update", () -> CacheFactory.getAnyInstance()
        .getRegion("ClusterSSLDUnitTestRegion").put("testKey", "updatedTestValue"));
  }

  private void performCreateWithLargeValue(VM memberVM) {
    memberVM.invoke("perform create", () -> {
      byte[] value = new byte[SMALL_BUFFER_SIZE];
      Arrays.fill(value, (byte) 1);
      CacheFactory.getAnyInstance().getRegion("ClusterSSLDUnitTestRegion").put("testKey", value);
    });
  }

  private void verifyCreatedEntry(VM memberVM) {
    memberVM.invoke("verify entry created", () -> Assert.assertTrue(CacheFactory.getAnyInstance()
        .getRegion("ClusterSSLDUnitTestRegion").containsKey("testKey")));
  }

  private void verifyUpdatedEntry(VM memberVM) {
    memberVM.invoke("verify entry updated", () -> Assert.assertTrue(CacheFactory.getAnyInstance()
        .getRegion("ClusterSSLDUnitTestRegion").containsValue("updatedTestValue")));
  }

  private int createLocator(VM memberVM) {
    return memberVM.invoke("create locator", () -> {
      return Locator.startLocatorAndDS(0, new File(""), getDistributedSystemProperties()).getPort();
    });
  }

  private Cache createCache(int locatorPort, boolean conserveSockets) {
    Properties properties = getDistributedSystemProperties();
    properties.put(ConfigurationProperties.LOCATORS, "localhost[" + locatorPort + "]");
    properties.put(ConfigurationProperties.CONSERVE_SOCKETS, "" + conserveSockets);
    return new CacheFactory(properties).create();
  }

  public Properties getDistributedSystemProperties() {
    Properties properties = new Properties();
    properties.put(SSL_ENABLED_COMPONENTS, "cluster");
    properties.put(SSL_KEYSTORE, TestUtil.getResourcePath(this.getClass(), "server.keystore"));
    properties.put(SSL_TRUSTSTORE, TestUtil.getResourcePath(this.getClass(), "server.keystore"));
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    // properties.put(ConfigurationProperties.LOG_LEVEL, "fine");
    properties.put(NAME, "vm" + VM.getCurrentVMNum());
    properties.put(SOCKET_BUFFER_SIZE, "" + SMALL_BUFFER_SIZE);
    return properties;
  }
}
