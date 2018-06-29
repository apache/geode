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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

@Category({IntegrationTest.class, ClientServerTest.class})
public class LocatorSSLJUnitTest {
  private final String SERVER_KEY_STORE =
      TestUtil.getResourcePath(LocatorSSLJUnitTest.class, "cacheserver.keystore");
  private final String SERVER_TRUST_STORE =
      TestUtil.getResourcePath(LocatorSSLJUnitTest.class, "cacheserver.truststore");

  @After
  public void tearDownTest() {
    SSLConfigurationFactory.close();
  }

  @Test
  public void canStopLocatorWithSSL() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.put(SSL_ENABLED_COMPONENTS, "all");
    properties.put(SSL_KEYSTORE_TYPE, "jks");
    properties.put(SSL_KEYSTORE, SERVER_KEY_STORE);
    properties.put(SSL_KEYSTORE_PASSWORD, "password");
    properties.put(SSL_TRUSTSTORE, SERVER_TRUST_STORE);
    properties.put(SSL_TRUSTSTORE_PASSWORD, "password");

    Locator locator = Locator.startLocatorAndDS(0, null, properties);
    locator.stop();
  }
}
