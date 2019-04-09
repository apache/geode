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
package org.apache.geode.management.internal.rest;


import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

@Category({SecurityTest.class, RestAPITest.class})
public class SwaggerManagementVerificationIntegrationTest {

  @ClassRule
  public static LocatorStarterRule locatorStarter = new LocatorStarterRule()
      .withHttpService()
      .withAutoStart();

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locatorStarter::getHttpPort);

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Test
  public void isSwaggerRunning() throws Exception {
    // Check the UI
    assertResponse(client.get("/geode-management/swagger-ui.html")).hasStatusCode(200);

    // Check the JSON
    JsonNode json =
        assertResponse(client.get("/geode-management/v2/api-docs")).hasStatusCode(200)
            .getJsonObject();
    assertThat(json.get("swagger").asText(), is("2.0"));

    JsonNode info = json.get("info");
    assertThat(info.get("description").asText(),
        is("REST API and interface to manage Geode components."));
    assertThat(info.get("title").asText(),
        is("Apache Geode Management REST API"));

    JsonNode license = info.get("license");
    assertThat(license.get("name").asText(), is("Apache License, version 2.0"));
    assertThat(license.get("url").asText(), is("http://www.apache.org/licenses/"));

  }
}
