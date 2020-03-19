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

import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@Category({PulseTest.class})
/**
 * this test just makes sure the property file in the locator's working dir
 * gets properly read and used in the oauth security configuration
 */
public class PulseSecurityConfigOAuthProfileTest {
  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withHttpService()
          .withSecurityManager(SimpleSecurityManager.class)
          .withProperty("security-auth-token-enabled-components", "pulse");

  private static File pulsePropertyFile;

  @BeforeClass
  public static void setupPulsePropertyFile() throws Exception {
    // put the pulse.properties to the locator's working dir. Pulse will use the locator's working
    // dir as classpath to search for this property file
    pulsePropertyFile = new File(locator.getWorkingDir(), "pulse.properties");
    Properties properties = new Properties();
    properties.setProperty("pulse.oauth.provider", "uaa");
    properties.setProperty("pulse.oauth.clientId", "pulse");
    properties.setProperty("pulse.oauth.clientSecret", "secret");
    // have the authorization uri point to a known uri that locator itself can serve
    properties.setProperty("pulse.oauth.authorizationUri",
        "http://localhost:" + locator.getHttpPort() + "/management");

    properties.store(new FileWriter(pulsePropertyFile), null);
    locator.startLocator();
  }

  @AfterClass
  public static void cleanup() {
    pulsePropertyFile.delete();
  }

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locator::getHttpPort);

  @Test
  public void redirectToAuthorizationUriInPulseProperty() throws Exception {
    HttpResponse response = client.get("/pulse/login.html");
    // the request is redirect to the authorization uri configured before
    assertResponse(response).hasStatusCode(200).hasResponseBody()
        .contains("latest")
        .contains("supported");
  }
}
