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

package org.apache.geode.tools.pulse;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.util.test.TestUtil.getResourcePath;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


@Category({SecurityTest.class, PulseTest.class})
public class PulseSecurityWithSSLTest {

  private static File jks =
      new File(getResourcePath(PulseSecurityWithSSLTest.class, "/ssl/trusted.keystore"));

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties securityProps = new Properties();
    securityProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.ALL);
    securityProps.setProperty(SSL_KEYSTORE, jks.getCanonicalPath());
    securityProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    securityProps.setProperty(SSL_TRUSTSTORE, jks.getCanonicalPath());
    securityProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    securityProps.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    securityProps.setProperty(SSL_CIPHERS, "any");

    locator.withSecurityManager(SimpleTestSecurityManager.class).withProperties(securityProps)
        .startLocator();
  }

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locator::getHttpPort).withSSL();


  @Test
  public void loginWithIncorrectPassword() throws Exception {
    HttpResponse response = client.loginToPulse("data", "wrongPassword");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");

    client.loginToPulseAndVerify("data", "data");
  }

}
