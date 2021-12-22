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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.Locator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class SSLDualServerNoClientAuthDUnitTest extends JUnit4CacheTestCase {

  private static final String SERVER_1_KEYSTORE = "geodeserver1.keystore";
  private static final String SERVER_1_TRUSTSTORE = "geodeserver1.truststore";

  private static final String SERVER_2_KEYSTORE = "geodeserver2.keystore";
  private static final String SERVER_2_TRUSTSTORE = "geodeserver2.truststore";

  private VM serverVM;
  private VM server2VM;
  private VM locator;

  @Before
  public void setUp() {
    disconnectAllFromDS();
    serverVM = getVM(1);
    server2VM = getVM(2);
    locator = getVM(3);
  }

  @After
  public void tearDown() {
    locator.invoke(() -> {
      if (Locator.hasLocator()) {
        Locator.getLocator().stop();
      }
    });
  }

  @Test
  public void testSSLServerWithNoAuth() {
    final int locatorPort = locator.invoke(this::setUpLocatorTask);

    serverVM.invoke(() -> setUpServerVMTask(locatorPort));
    server2VM.invoke(() -> setUpServerVMTask(locatorPort));

    server2VM.invoke(this::doServerRegionTestTask);
    serverVM.invoke(this::doServerRegionTestTask);
  }

  private int setUpLocator() throws Exception {
    final Properties gemFireProps = new Properties();

    final boolean cacheServerSslRequireAuth = false;

    System.setProperty("javax.net.debug", "all");

    final String keyStore =
        createTempFileFromResource(SSLDualServerNoClientAuthDUnitTest.class, SERVER_1_KEYSTORE)
            .getAbsolutePath();
    final String trustStore =
        createTempFileFromResource(SSLDualServerNoClientAuthDUnitTest.class, SERVER_1_TRUSTSTORE)
            .getAbsolutePath();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "cluster");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));
    gemFireProps.setProperty(SSL_KEYSTORE, keyStore);
    gemFireProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStore);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");

    final StringWriter sw = new StringWriter();
    final PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);

    Locator.startLocatorAndDS(0, new File(""), gemFireProps);

    return Locator.getLocator().getPort();
  }

  private void setUpAndConnectToDistributedSystem(final int locatorPort) throws Exception {
    final Properties gemFireProps = new Properties();

    final boolean cacheServerSslRequireAuth = false;

    String keyStore;
    String trustStore;
    if (VM.getCurrentVMNum() == 1) {
      keyStore =
          createTempFileFromResource(SSLDualServerNoClientAuthDUnitTest.class, SERVER_1_KEYSTORE)
              .getAbsolutePath();
      trustStore =
          createTempFileFromResource(SSLDualServerNoClientAuthDUnitTest.class,
              SERVER_1_TRUSTSTORE)
                  .getAbsolutePath();
    } else {
      keyStore =
          createTempFileFromResource(SSLDualServerNoClientAuthDUnitTest.class, SERVER_2_KEYSTORE)
              .getAbsolutePath();
      trustStore =
          createTempFileFromResource(SSLDualServerNoClientAuthDUnitTest.class,
              SERVER_2_TRUSTSTORE)
                  .getAbsolutePath();
    }
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "cluster");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "" + cacheServerSslRequireAuth);
    gemFireProps.setProperty(SSL_KEYSTORE, "" + keyStore);
    gemFireProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.setProperty(SSL_TRUSTSTORE, "" + trustStore);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");

    gemFireProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");


    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);
    gemFireProps.list(printWriter);

    final Cache cache = getCache(gemFireProps);
    final RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    final Region r = factory.create("serverRegion");
    r.put("serverkey", "servervalue");
  }

  private void doServerRegionTest() {
    Region<String, String> region = cache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
  }

  private int setUpLocatorTask() throws Exception {
    return setUpLocator();
  }

  private void setUpServerVMTask(final int locatorPort) throws Exception {
    setUpAndConnectToDistributedSystem(locatorPort);
  }

  private void doServerRegionTestTask() {
    doServerRegionTest();
  }

}
