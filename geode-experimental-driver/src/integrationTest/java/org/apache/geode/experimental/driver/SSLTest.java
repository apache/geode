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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.SocketException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.SSLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.util.test.TestUtil;

@Category({ClientServerTest.class})
public class SSLTest {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final String DEFAULT_KEY_STORE =
      TestUtil.getResourcePath(SSLTest.class, "default.keystore");
  private final String SERVER_KEY_STORE =
      TestUtil.getResourcePath(SSLTest.class, "cacheserver.keystore");
  private final String SERVER_TRUST_STORE =
      TestUtil.getResourcePath(SSLTest.class, "cacheserver.truststore");
  private final String BOGUSSERVER_KEY_STORE =
      TestUtil.getResourcePath(SSLTest.class, "bogusserver.keystore");
  private final String BOGUSCLIENT_KEY_STORE =
      TestUtil.getResourcePath(SSLTest.class, "bogusclient.keystore");
  private final String CLIENT_KEY_STORE =
      TestUtil.getResourcePath(SSLTest.class, "client.keystore");
  private final String CLIENT_TRUST_STORE =
      TestUtil.getResourcePath(SSLTest.class, "client.truststore");
  private Locator locator;
  private Cache cache;
  private Driver driver;
  private int locatorPort;

  @Before
  public void enableProtobuf() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");
  }

  private void startLocator(String keyStore, String trustStore, boolean twoWayAuthentication,
      String protocols, String ciphers) throws IOException {
    // Create a cache
    Properties properties = new Properties();
    properties.put(SSL_ENABLED_COMPONENTS, "all");
    properties.put(SSL_KEYSTORE_TYPE, "jks");
    properties.put(SSL_KEYSTORE, keyStore);
    properties.put(SSL_PROTOCOLS, protocols);
    properties.put(SSL_CIPHERS, ciphers);
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE, trustStore);
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.put(SSL_REQUIRE_AUTHENTICATION, String.valueOf(twoWayAuthentication));

    CacheFactory cf = new CacheFactory(properties);
    cf.set(ConfigurationProperties.MCAST_PORT, "0");
    cache = cf.create();
    cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");

    // Start a locator
    locator = Locator.startLocatorAndDS(0, null, properties);
    locatorPort = locator.getPort();
  }

  private void startServer() throws IOException {
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();
  }

  @After
  public void cleanup() {
    cache.close();
    locator.stop();
    SocketCreatorFactory.close();
  }

  @Test
  public void driverFailsToConnectWhenThereAreNoServers() throws Exception {
    // using TLSv1.2 specifically so that in jdk11 (by default using TLSv1.3) this won't take
    // too long to finish.
    startLocator(SERVER_KEY_STORE, SERVER_TRUST_STORE, true, "TLSv1.2", "any");
    expectedException.expect(IOException.class);
    driver = new DriverFactory().addLocator("localhost", locatorPort).create();
  }

  @Test
  public void driverCanConnectWithTwoWayAuthentication() throws Exception {
    startLocator(DEFAULT_KEY_STORE, DEFAULT_KEY_STORE, true, "any", "any");
    startServer();
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(DEFAULT_KEY_STORE).setKeyStorePath(DEFAULT_KEY_STORE).create();
    Set<String> regionsOnServer = driver.getRegionNames();
    assertEquals(Collections.singleton("/region"), regionsOnServer);
    assertTrue(driver.isConnected());
  }

  @Test
  public void driverCannotConnectWithBogusClientKeystore() throws Exception {
    startLocator(DEFAULT_KEY_STORE, DEFAULT_KEY_STORE, true, "any", "any");
    startServer();
    expectedException
        .expect(anyOf(instanceOf(SSLException.class), instanceOf(SocketException.class)));
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(CLIENT_TRUST_STORE).setKeyStorePath(BOGUSCLIENT_KEY_STORE).create();
  }

  @Test
  public void driverCannotConnectWithBogusServerKeystore() throws Exception {
    // using TLSv1.2 specifically so that in jdk11 (by default using TLSv1.3) this won't take
    // too long to finish.
    startLocator(BOGUSSERVER_KEY_STORE, SERVER_TRUST_STORE, true, "TLSv1.2", "any");
    startServer();
    expectedException.expect(SSLException.class);
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(CLIENT_TRUST_STORE).setKeyStorePath(CLIENT_KEY_STORE).create();
  }

  @Test
  public void driverCanConnectWithOneWayAuthentication() throws Exception {
    startLocator(DEFAULT_KEY_STORE, DEFAULT_KEY_STORE, false, "any", "any");
    startServer();
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(DEFAULT_KEY_STORE).create();
    Set<String> regionsOnServer = driver.getRegionNames();
    assertEquals(Collections.singleton("/region"), regionsOnServer);
    assertTrue(driver.isConnected());
  }

  @Test
  public void driverCannotConnectIfProtocolsMismatch() throws Exception {
    startLocator(SERVER_KEY_STORE, SERVER_TRUST_STORE, true, "TLSv1.1", "any");
    startServer();
    expectedException.expect(SSLException.class);
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(CLIENT_TRUST_STORE).setKeyStorePath(CLIENT_KEY_STORE)
        .setProtocols("TLSv1.2").create();
  }

  @Test
  public void driverCannotConnectIfCiphersMismatch() throws Exception {
    startLocator(SERVER_KEY_STORE, SERVER_TRUST_STORE, true, "any",
        "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256");
    startServer();
    expectedException.expect(SSLException.class);
    driver = new DriverFactory().addLocator("localhost", locatorPort)
        .setTrustStorePath(CLIENT_TRUST_STORE).setKeyStorePath(CLIENT_KEY_STORE)
        .setCiphers("TLS_DHE_DSS_WITH_AES_128_CBC_SHA").create();
  }
}
