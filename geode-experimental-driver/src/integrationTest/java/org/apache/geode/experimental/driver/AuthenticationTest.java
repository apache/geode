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

import static org.apache.geode.internal.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class AuthenticationTest {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private static final String TEST_USERNAME = "";
  private static final String TEST_PASSWORD = TEST_USERNAME;
  private Locator locator;
  private Cache cache;
  private Driver driver;
  private int locatorPort;

  @Before
  public void createServer() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");

    // Create a cache
    CacheFactory cf = new CacheFactory();
    cf.set(ConfigurationProperties.MCAST_PORT, "0");
    cf.setSecurityManager(new SimpleSecurityManager());
    cache = cf.create();

    // Start a locator
    locator = Locator.startLocatorAndDS(0, null, new Properties());
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
    driver = new DriverFactory().addLocator("localhost", locatorPort).setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD).create();
    assertTrue(driver.isConnected());
  }

  @Test
  public void driverWithBadPasswordIsRejected() throws Exception {
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();
    DriverFactory factory =
        new DriverFactory().addLocator("localhost", locatorPort).setUsername(TEST_USERNAME)
            .setPassword("my my my");
    assertThatThrownBy(() -> factory.create()).isInstanceOf(IOException.class);
  }

}
