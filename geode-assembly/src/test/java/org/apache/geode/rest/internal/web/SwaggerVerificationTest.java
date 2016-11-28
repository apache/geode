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
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.http.HttpResponse;
import org.json.JSONObject;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({IntegrationTest.class, RestAPITest.class})
public class SwaggerVerificationTest {

  private static int restPort = AvailablePortHelper.getRandomAvailableTCPPort();
  static Properties properties = new Properties() {
    {
      setProperty(START_DEV_REST_API, "true");
      setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
      setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
      setProperty(HTTP_SERVICE_PORT, restPort + "");
    }
  };

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule(properties);
  private final GeodeRestClient restClient = new GeodeRestClient("localhost", restPort);

  @Test
  public void isSwaggerRunning() throws Exception {
    // Check the UI
    HttpResponse response = restClient.doGetRequest("/geode/swagger-ui.html");
    assertThat(GeodeRestClient.getCode(response), is(200));

    // Check the JSON
    response = restClient.doGetRequest("/geode/v2/api-docs");
    assertThat(GeodeRestClient.getCode(response), is(200));
    JSONObject json = GeodeRestClient.getJsonObject(response);
    assertThat(json.get("swagger"), is("2.0"));

    JSONObject info = json.getJSONObject("info");
    assertThat(info.getString("description"),
        is(LocalizedStrings.SwaggerConfig_DESCRIPTOR.toLocalizedString()));
    assertThat(info.getString("title"),
        is(LocalizedStrings.SwaggerConfig_VENDOR_PRODUCT_LINE.toLocalizedString()));

    JSONObject license = info.getJSONObject("license");
    assertThat(license.getString("name"), is("Apache License, version 2.0"));
    assertThat(license.getString("url"), is("http://www.apache.org/licenses/"));

  }
}
