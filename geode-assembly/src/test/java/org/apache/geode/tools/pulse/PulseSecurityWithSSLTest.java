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

import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.HttpClientRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.dunit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.http.HttpResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.util.test.TestUtil.getResourcePath;
import static org.assertj.core.api.Assertions.assertThat;


@Category(IntegrationTest.class)
public class PulseSecurityWithSSLTest {

  private static File jks =
      new File(getResourcePath(PulseSecurityWithSSLTest.class, "/ssl/trusted.keystore"));

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties securityProps = new Properties();
    securityProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.JMX);
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
  public HttpClientRule client = new HttpClientRule(locator::getHttpPort);


  @Test
  public void loginWithIncorrectPassword() throws Exception {
    HttpResponse response = client.loginToPulse("data", "wrongPassword");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");

    client.loginToPulseAndVerify("data", "data");
  }

}
