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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.rules.EmbeddedPulseRule;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.tools.pulse.internal.data.Cluster;

@Category({PulseTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class PulseConnectivityTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withHttpService();

  @Rule
  public EmbeddedPulseRule pulse = new EmbeddedPulseRule();

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locator::getHttpPort);

  @Parameterized.Parameter
  public static String jmxBindAddress;

  @Parameterized.Parameters(name = "JMXBindAddress: {0}")
  public static Collection<String> bindAddresses() throws Exception {
    String nonDefaultJmxBindAddress = InetAddress.getLocalHost().getHostName();
    if ("localhost".equals(nonDefaultJmxBindAddress)) {
      nonDefaultJmxBindAddress = InetAddress.getLocalHost().getHostAddress();
    }
    return Arrays.asList(new String[] {"localhost", nonDefaultJmxBindAddress});
  }

  @Before
  public void setUp() throws Exception {
    Properties locatorProperties = new Properties();
    if (!"localhost".equals(jmxBindAddress))
      locatorProperties.setProperty(JMX_MANAGER_BIND_ADDRESS, jmxBindAddress);
    locator.withProperties(locatorProperties).startLocator();
  }

  @Test
  public void testLogin() throws Exception {
    HttpResponse response = client.loginToPulse("admin", "wrongPassword");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");
    client.loginToPulseAndVerify("admin", "admin");
  }

  @Test
  public void testConnectToJmx() throws Exception {
    pulse.useJmxManager(jmxBindAddress, locator.getJmxPort());
    Cluster cluster = pulse.getRepository().getCluster("admin", null);
    assertThat(cluster.isConnectedFlag()).isTrue();
    assertThat(cluster.getServerCount()).isEqualTo(0);
  }

  @Test
  public void testConnectToLocator() throws Exception {
    pulse.useLocatorPort(locator.getPort());
    Cluster cluster = pulse.getRepository().getCluster("admin", null);
    assertThat(cluster.isConnectedFlag()).isTrue();
    assertThat(cluster.getServerCount()).isEqualTo(0);
  }
}
