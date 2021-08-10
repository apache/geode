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
 *
 */
package org.apache.geode.rest.internal.web.controllers;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.util.internal.GeodeJsonMapper;

@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
public class RestAPICompatibilityTest {
  private final String oldVersion;
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.11.0") < 0);
    return result;
  }

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  public RestAPICompatibilityTest(String oldVersion) throws JsonProcessingException {
    this.oldVersion = oldVersion;
    DiskStore diskStore = new DiskStore();
    diskStore.setName("diskStore");
    postRESTAPICalls = new HashMap<>();
    // {REST endpoint,{Body, Successful Status Message, Introduced in version}}
    postRESTAPICalls.put("/management/v1/operations/rebalances",
        new String[] {mapper.writeValueAsString(new RebalanceOperation()), "Operation started",
            "1.11.0"});
    postRESTAPICalls.put("/management/v1/operations/restoreRedundancy",
        new String[] {mapper.writeValueAsString(new RestoreRedundancyRequest()),
            "Operation started", "1.13.1"});
  }

  private static Map<String, String[]> postRESTAPICalls;


  private static final String[][] getRESTAPICalls = {
      // REST endpoint , status
      {"/geode-mgmt/v1/management/commands?cmd=rebalance", "OK"}
  };

  @Test
  public void restCommandExecutedOnLatestLocatorShouldBeBackwardsCompatible() throws Exception {
    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int locatorPort1 = locatorPorts[0];
    int locatorPort2 = locatorPorts[1];

    // Initialize all cluster members with old versions
    cluster.startLocatorVM(0, locatorPort1, oldVersion, MemberStarterRule::withHttpService);
    cluster.startLocatorVM(1, locatorPort2, oldVersion,
        x -> x.withConnectionToLocator(locatorPort1).withHttpService());
    cluster
        .startServerVM(2, oldVersion, s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));
    cluster
        .startServerVM(3, oldVersion, s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));

    // Roll locators to the current version
    cluster.stop(0);
    // gradle sets a property telling us where the build is located
    final String buildDir = System.getProperty("geode.build.dir", System.getProperty("user.dir"));
    MemberVM locator1 = cluster.startLocatorVM(0, l -> l.withHttpService().withPort(locatorPort1)
        .withConnectionToLocator(locatorPort2)
        .withSystemProperty("geode.build.dir", buildDir));
    cluster.stop(1);

    cluster.startLocatorVM(1,
        x -> x.withConnectionToLocator(locatorPort1).withHttpService().withPort(locatorPort2)
            .withConnectionToLocator(locatorPort1)
            .withSystemProperty("geode.build.dir", buildDir));

    gfsh.connectAndVerify(locator1);
    gfsh.execute("list members");
    // Execute REST api calls to from the new locators to the old servers to ensure that backwards
    // compatibility is maintained

    executeAndValidatePOSTRESTCalls(locator1.getHttpPort());
    executeAndValidateGETRESTCalls(locator1.getHttpPort());

  }

  void executeAndValidatePOSTRESTCalls(int locator) throws Exception {

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      for (Map.Entry<String, String[]> entry : postRESTAPICalls.entrySet()) {
        // Skip the test is the version is before the REST api was introduced.
        if (TestVersion.compare(oldVersion, entry.getValue()[2]) < 0) {
          continue;
        }
        HttpPost post =
            new HttpPost("http://localhost:" + locator + entry.getKey());
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Accept", "application/json");
        StringEntity jsonStringEntity =
            new StringEntity(entry.getValue()[0], ContentType.DEFAULT_TEXT);
        post.setEntity(jsonStringEntity);
        CloseableHttpResponse response = httpClient.execute(post);

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(content))) {
          String line;
          StringBuilder sb = new StringBuilder();
          while ((line = reader.readLine()) != null) {
            sb.append(line);
          }
          JsonNode jsonObject = mapper.readTree(sb.toString());
          String statusCode = jsonObject.findValue("statusCode").textValue();
          assertThat(statusCode).satisfiesAnyOf(
              value -> assertThat(value).isEqualTo("ACCEPTED"),
              value -> assertThat(value).contains("OK"));
          String statusMessage = jsonObject.findValue("statusMessage").textValue();
          assertThat(statusMessage).contains(entry.getValue()[1]);
        }
      }
    }
  }

  public static void executeAndValidateGETRESTCalls(int locator) throws Exception {

    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      for (String[] commandExpectedResponsePair : getRESTAPICalls) {
        HttpGet get =
            new HttpGet("http://localhost:" + locator +
                commandExpectedResponsePair[0]);
        CloseableHttpResponse response = httpclient.execute(get);
        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(content))) {
          String line;
          StringBuilder sb = new StringBuilder();
          while ((line = reader.readLine()) != null) {
            sb.append(line);
          }
          JsonNode jsonObject = mapper.readTree(sb.toString());
          String statusCode = jsonObject.findValue("status").textValue();
          assertThat(statusCode).contains(commandExpectedResponsePair[1]);
        }
      }
    }
  }
}
