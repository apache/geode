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

import org.apache.geode.test.dunit.rules.EmbeddedPulseRule;
import org.apache.geode.test.dunit.rules.HttpClientRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.dunit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.http.HttpResponse;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;


@Category(IntegrationTest.class)
public class PulseVerificationTest {

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withJMXManager().withAutoStart();

  @Rule
  public EmbeddedPulseRule pulse = new EmbeddedPulseRule();

  @Rule
  public HttpClientRule client = new HttpClientRule(locator::getHttpPort);

  @Test
  public void loginWithIncorrectPassword() throws Exception {
    HttpResponse response = client.loginToPulse("admin", "wrongPassword");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");

    client.loginToPulseAndVerify("admin", "admin");
  }

  @Test
  public void testConnectToJmx() throws Exception {
    pulse.useJmxPort(locator.getJmxPort());
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
