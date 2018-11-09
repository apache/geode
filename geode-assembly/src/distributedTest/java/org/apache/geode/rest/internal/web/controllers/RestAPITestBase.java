/*
 * /* Licensed to the Apache Software Foundation (ASF) under one or more contributor license
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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.management.internal.AgentUtil;
import org.apache.geode.rest.internal.web.RestFunctionTemplate;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.RestAPITest;

@Category({RestAPITest.class})
class RestAPITestBase extends JUnit4DistributedTestCase {

  protected Cache cache = null;
  protected List<String> restURLs = new ArrayList<>();
  protected VM vm0 = null;
  protected VM vm1 = null;
  protected VM vm2 = null;
  protected VM vm3 = null;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    AgentUtil agentUtil = new AgentUtil(GemFireVersion.getGemFireVersion());
    if (agentUtil.findWarLocation("geode-web-api") == null) {
      fail("unable to locate geode-web-api WAR file");
    }
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    // gradle sets a property telling us where the build is located
    final String buildDir = System.getProperty("geode.build.dir", System.getProperty("user.dir"));
    Invoke.invokeInEveryVM(() -> System.setProperty("geode.build.dir", buildDir));
    postSetUpRestAPITestBase();
  }

  private void postSetUpRestAPITestBase() throws Exception {}

  /**
   * close the clients and the servers
   */
  @Override
  public final void preTearDown() throws Exception {
    vm0.invoke(() -> closeCache());
    vm1.invoke(() -> closeCache());
    vm2.invoke(() -> closeCache());
    vm3.invoke(() -> closeCache());
  }

  /**
   * close the cache
   */
  private void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  String createCacheWithGroups(final String hostName, final String groups, final String context) {
    RestAPITestBase test = new RestAPITestBase();

    final int servicePort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties props = new Properties();

    if (groups != null) {
      props.put(GROUPS, groups);
    }

    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, hostName);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(servicePort));

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    return "http://" + hostName + ":" + servicePort + context + "/v1";
  }

  private int getInvocationCount(String functionID) {
    RestFunctionTemplate function = (RestFunctionTemplate) FunctionService.getFunction(functionID);
    return function.invocationCount;
  }

  CloseableHttpResponse executeFunctionThroughRestCall(String function, String regionName,
      String filter, String jsonBody, String groups, String members) {
    System.out.println("Entering executeFunctionThroughRestCall");
    CloseableHttpResponse value = null;
    try {
      CloseableHttpClient httpclient = HttpClients.createDefault();
      Random randomGenerator = new Random();
      int restURLIndex = randomGenerator.nextInt(restURLs.size());

      HttpPost post =
          createHTTPPost(function, regionName, filter, restURLIndex, groups, members, jsonBody);

      System.out.println("Request: POST " + post.toString());
      value = httpclient.execute(post);
    } catch (Exception e) {
      fail("unexpected exception", e);
    }
    return value;
  }

  private HttpPost createHTTPPost(String function, String regionName, String filter,
      int restUrlIndex, String groups, String members, String jsonBody) {
    StringBuilder restURLBuilder = new StringBuilder();
    restURLBuilder.append(restURLs.get(restUrlIndex)).append("/functions/").append(function)
        .append("?");
    if (regionName != null && !regionName.isEmpty()) {
      restURLBuilder.append("onRegion=").append(regionName);
    } else if (groups != null && !groups.isEmpty()) {
      restURLBuilder.append("onGroups=").append(groups);
    } else if (members != null && !members.isEmpty()) {
      restURLBuilder.append("onMembers=").append(members);
    }
    if (filter != null && !filter.isEmpty()) {
      restURLBuilder.append("&filter=").append(filter);
    }
    String restString = restURLBuilder.toString();
    HttpPost post = new HttpPost(restString);
    post.addHeader("Content-Type", "application/json");
    post.addHeader("Accept", "application/json");
    if (StringUtils.isNotEmpty(jsonBody)) {
      StringEntity jsonStringEntity = new StringEntity(jsonBody, ContentType.DEFAULT_TEXT);
      post.setEntity(jsonStringEntity);
    }
    return post;
  }

  void assertHttpResponse(CloseableHttpResponse response, int httpCode,
      int expectedServerResponses) {
    assertEquals(httpCode, response.getStatusLine().getStatusCode());

    // verify response has body flag, expected is true.
    assertNotNull(response.getEntity());
    try {
      String httpResponseString = processHttpResponse(response);
      response.close();
      System.out.println("Response : " + httpResponseString);
      // verify function execution result
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(httpResponseString);

      if (json.isArray()) {
        assertEquals(expectedServerResponses, json.size());
      } else {
        assertThat(expectedServerResponses)
            .as("Did not receive an expected JSON array. Instead, received a %s type.",
                json.getNodeType().name())
            .isEqualTo(0);
      }
    } catch (Exception e) {
      // fail("exception", e);
    }
  }

  private String processHttpResponse(HttpResponse response) {
    try {
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(content));
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    } catch (IOException e) {
      fail("exception", e);
    }
    return "";
  }

  void assertCorrectInvocationCount(String functionID, int expectedInvocationCount, VM... vms) {
    int count = 0;
    for (final VM vm : vms) {
      count += vm.invoke("getInvocationCount", () -> getInvocationCount(functionID));
    }
    assertEquals(expectedInvocationCount, count);
  }

  protected void resetInvocationCount(String functionID) {
    RestFunctionTemplate f = (RestFunctionTemplate) FunctionService.getFunction(functionID);
    f.invocationCount = 0;
  }
}
