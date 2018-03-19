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
package org.apache.geode.experimental.driver;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

@Category(IntegrationTest.class)
public class SSLTest {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private final String SERVER_KEY_STORE = TestUtil.getResourcePath(SSLTest.class, "cacheserver.keystore");
  private final String SERVER_TRUST_STORE = TestUtil.getResourcePath(SSLTest.class, "cacheserver.truststore");

  private final String CLIENT_KEY_STORE = TestUtil.getResourcePath(SSLTest.class, "client.keystore");
  private final String CLIENT_TRUST_STORE = TestUtil.getResourcePath(SSLTest.class, "client.truststore");
  private Locator locator;
  private Cache cache;
  private Driver driver;
  private int locatorPort;

  @Before
  public void createServer() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    // Create a cache
    Properties properties = new Properties();
    properties.put(SSL_ENABLED_COMPONENTS, "server,locator");
    properties.put(SSL_REQUIRE_AUTHENTICATION, String.valueOf(true));
    properties.put(SSL_KEYSTORE_TYPE, "jks");
    properties.put(SSL_KEYSTORE, SERVER_KEY_STORE);
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE, SERVER_TRUST_STORE);
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_REQUIRE_AUTHENTICATION, "true");
    CacheFactory cf = new CacheFactory(properties);
    cf.set(ConfigurationProperties.MCAST_PORT, "0");
    cache = cf.create();
    cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");

    // Start a locator
    locator = Locator.startLocatorAndDS(0, null, null);
    locatorPort = locator.getPort();

    // do not start a cache server
  }

  @After
  public void cleanup() {
    locator.stop();
    cache.close();
  }

  @Test
  public void driverFailsToConnectWhenThereAreNoServers() throws Exception {
    try {
      driver = new DriverFactory().addLocator("localhost", locatorPort).create();
    } catch (IOException e) {
      // success
      return;
    }
    throw new AssertionError("expected an IOException");
  }

  @Test
  public void driverCanConnectWhenThereAreServers() throws Exception {
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(CLIENT_TRUST_STORE)
        .setKeyStorePath(CLIENT_KEY_STORE)
        .create();
    Set<String> regionsOnServer = driver.getRegionNames();
    assertEquals(Collections.singleton("region"), regionsOnServer);
    assertTrue(driver.isConnected());
  }
}
