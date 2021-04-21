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
import java.util.HashMap;
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

import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RestUtilities {
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();
  protected static Map<String, String[]> postRESTAPICalls;
  static {
    RestUtilities.postRESTAPICalls = new HashMap<>();
    // {REST endpoint,{Body, Successful Status Message, Introduced in version}}
    try {
      postRESTAPICalls.put("/management/v1/operations/rebalances",
          new String[] {mapper.writeValueAsString(new RebalanceOperation()), "Operation started",
              "1.11.0"});
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  protected static final String[][] getRESTAPICalls = {
      // REST endpoint , status
      {"/geode-mgmt/v1/management/commands?cmd=rebalance", "OK"}
  };

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

  public static void executeAndValidatePOSTRESTCalls(int locator, String version) throws Exception {

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      for (Map.Entry<String, String[]> entry : postRESTAPICalls.entrySet()) {
        // Skip the test is the version is before the REST api was introduced.
        if (TestVersion.compare(version, entry.getValue()[2]) < 0) {
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

}
