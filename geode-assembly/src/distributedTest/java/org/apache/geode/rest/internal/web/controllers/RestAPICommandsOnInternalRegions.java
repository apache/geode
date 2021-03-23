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

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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

import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RestAPICommandsOnInternalRegions {

  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void rebalanceRedisRegionsUsingRESTAPIs() throws IOException {
    final String buildDir = System.getProperty("geode.build.dir", System.getProperty("user.dir"));
    MemberVM locator1 = cluster.startLocatorVM(0, l -> l.withHttpService()
        .withSystemProperty("geode.build.dir", buildDir));
    int locatorPort = locator1.getPort();
    cluster.startServerVM(1,
        s -> s.withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withProperty(REDIS_PORT, "0")
            .withProperty(REDIS_ENABLED, "true")
            .withSystemProperty("enable-redis-unsupported-commands",
                "true")
            .withConnectionToLocator(locatorPort));
    cluster.startServerVM(2,
        s -> s.withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withProperty(REDIS_PORT, "0")
            .withProperty(REDIS_ENABLED, "true")
            .withSystemProperty("enable-redis-unsupported-commands",
                "true")
            .withConnectionToLocator(locatorPort));
    cluster.startServerVM(3,
        s -> s.withProperty(REDIS_BIND_ADDRESS, "localhost")
            .withProperty(REDIS_PORT, "0")
            .withProperty(REDIS_ENABLED, "true")
            .withSystemProperty("enable-redis-unsupported-commands",
                "true")
            .withConnectionToLocator(locatorPort));
    String rebalanceResultLink = executeRestAPIToInitiateRebalance(locator1.getHttpPort());
    validateResultsOfRebalanceRestAPICall(rebalanceResultLink);
  }

  void validateResultsOfRebalanceRestAPICall(String rebalanceResultLink) throws IOException {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet get = new HttpGet(rebalanceResultLink);
      CloseableHttpResponse getResponse = httpClient.execute(get);
      HttpEntity getEntity = getResponse.getEntity();
      InputStream getContent = getEntity.getContent();
      try (BufferedReader getReader = new BufferedReader(new InputStreamReader(getContent))) {
        String getLine;
        StringBuilder getSb = new StringBuilder();
        while ((getLine = getReader.readLine()) != null) {
          getSb.append(getLine);
        }
        JsonNode getJsonObject = mapper.readTree(getSb.toString());
        Boolean rebalanceResult =
            getJsonObject.get("operationResult").get("success").booleanValue();
        String redisRegion =
            getJsonObject.get("operationResult").get("rebalanceRegionResults").get(0)
                .get("regionName").textValue();

        assertThat(redisRegion).isNotNull();
        assertThat(rebalanceResult).isNotNull();
        assertThat(redisRegion).isEqualTo("__REDIS_DATA");
        assertThat(rebalanceResult).isEqualTo(true);
      }
    }
  }


  String executeRestAPIToInitiateRebalance(int httpPort) throws IOException {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost post =
          new HttpPost("http://localhost:" + httpPort
              + "/management/v1/operations/rebalances");
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      StringEntity jsonStringEntity =
          new StringEntity(mapper.writeValueAsString(new RebalanceOperation()),
              ContentType.DEFAULT_TEXT);
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
        assertThat(statusMessage).contains("Operation started");
        String rebalanceResultLink = jsonObject.path("links").findValue("self").textValue();
        assertThat(rebalanceResultLink).isNotNull();
        return rebalanceResultLink;
      }
    }
  }
}
