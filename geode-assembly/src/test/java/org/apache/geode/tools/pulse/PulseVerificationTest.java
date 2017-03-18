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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.HttpClientRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.http.HttpResponse;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(IntegrationTest.class)
public class PulseVerificationTest {

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName()).startLocator();

  @Rule
  public HttpClientRule client = new HttpClientRule(locator.getHttpPort());

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

    // this would requiest cluster permission
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


}
