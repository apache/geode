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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.http.HttpResponse;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


@Category({SecurityTest.class, PulseTest.class})
public class EmbeddedPulseHttpSecurityTest {

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(SimpleSecurityManager.class)
      .withJMXManager()
      .withHttpService()
      .withRegion(REPLICATE, "regionA");

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(server::getHttpPort);

  @Test
  public void loginWithIncorrectPassword() throws Exception {
    HttpResponse response = client.loginToPulse("data", "wrongPassword");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");

    client.loginToPulseAndVerify("data", "data");
  }

  @Test
  public void loginWithDataOnly() throws Exception {
    client.loginToPulseAndVerify("data", "data");

    // this would request cluster permission
    HttpResponse response = client.get("/pulse/clusterDetail.html");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);

    // this would require both cluster and data permission
    response = client.get("/pulse/dataBrowser.html");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);
  }

  @Test
  public void loginAllAccess() throws Exception {
    client.loginToPulseAndVerify("CLUSTER,DATA", "CLUSTER,DATA");

    HttpResponse response = client.get("/pulse/clusterDetail.html");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    response = client.get("/pulse/dataBrowser.html");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
  }

  @Test
  public void loginWithClusterOnly() throws Exception {
    client.loginToPulseAndVerify("cluster", "cluster");

    HttpResponse response = client.get("/pulse/clusterDetail.html");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    // accessing data browser will be denied
    response = client.get("/pulse/dataBrowser.html");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(403);
  }

  @Test
  public void loginAfterLogout() throws Exception {
    client.loginToPulseAndVerify("data", "data");
    client.logoutFromPulse();
    client.loginToPulseAndVerify("data", "data");
    client.logoutFromPulse();
  }
}
