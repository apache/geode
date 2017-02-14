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

package org.apache.geode.rest.internal.web;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.junit.Assert.assertEquals;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.util.Properties;

@Category({IntegrationTest.class, SecurityTest.class})
public class RestSecurityWithSSLTest {

  private static int restPort = AvailablePortHelper.getRandomAvailableTCPPort();
  ServerStarterRule serverStarter = null;

  @Test
  public void testRestSecurityWithSSL() throws Exception {
    URL keystoreUrl =
        RestSecurityWithSSLTest.class.getClassLoader().getResource("ssl/trusted.keystore");

    Properties properties = new Properties();
    properties.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    properties.setProperty(START_DEV_REST_API, "true");
    properties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    properties.setProperty(HTTP_SERVICE_PORT, restPort + "");
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    properties.setProperty(SSL_KEYSTORE, keystoreUrl.getPath());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, keystoreUrl.getPath());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_PROTOCOLS, "TLSv1.2,TLSv1.1");

    serverStarter = new ServerStarterRule(properties);
    serverStarter.startServer();

    GeodeRestClient restClient = new GeodeRestClient("localhost", restPort, true);
    HttpResponse response = restClient.doGet("/servers", "cluster", "cluster");

    assertEquals(200, GeodeRestClient.getCode(response));
  }

  @After
  public void after() {
    if (serverStarter != null) {
      serverStarter.after();
    }
  }

}
