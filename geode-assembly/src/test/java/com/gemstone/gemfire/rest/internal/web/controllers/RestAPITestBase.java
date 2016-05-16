/*
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.AgentUtil;
import com.gemstone.gemfire.rest.internal.web.RestFunctionTemplate;
import com.gemstone.gemfire.test.dunit.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class RestAPITestBase extends DistributedTestCase {
  protected Cache cache = null;
  protected List<String> restURLs = new ArrayList();
  protected VM vm0 = null;
  protected VM vm1 = null;
  protected VM vm2 = null;
  protected VM vm3 = null;

  public RestAPITestBase(String name) {
    super(name);
  }

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

  protected void postSetUpRestAPITestBase() throws Exception {
  }

  /**
   * close the clients and teh servers
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

  public String createCacheWithGroups(final String hostName, final String groups) {
    RestAPITestBase test = new RestAPITestBase(getTestMethodName());

    final int servicePort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties props = new Properties();

    if (groups != null) {
      props.put("groups", groups);
    }

    props.setProperty(DistributionConfig.START_DEV_REST_API_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_BIND_ADDRESS_NAME, hostName);
    props.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(servicePort));

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    String restEndPoint = "http://" + hostName + ":" + servicePort + "/gemfire-api/v1";
    return restEndPoint;
  }

  protected int getInvocationCount() {
    RestFunctionTemplate function = (RestFunctionTemplate) FunctionService.getFunction(getFunctionID());
    return function.invocationCount;
  }

  protected CloseableHttpResponse executeFunctionThroughRestCall(String function, String regionName, String filter, String jsonBody, String groups,
                                                                 String members) {
    System.out.println("Entering executeFunctionThroughRestCall");
    try {
      CloseableHttpClient httpclient = HttpClients.createDefault();
      Random randomGenerator = new Random();
      int restURLIndex = randomGenerator.nextInt(restURLs.size());

      HttpPost post = createHTTPPost(function, regionName, filter, restURLIndex, groups, members, jsonBody);

      System.out.println("Request: POST " + post.toString());
      return httpclient.execute(post);
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }

  private HttpPost createHTTPPost(String function, String regionName, String filter, int restUrlIndex, String groups, String members, String jsonBody) {
    StringBuilder restURLBuilder = new StringBuilder();
    restURLBuilder.append(restURLs.get(restUrlIndex) + "/functions/" + function+"?");
    if (regionName != null && !regionName.isEmpty()) {
      restURLBuilder.append("onRegion=" + regionName);
    }
    else if (groups != null && !groups.isEmpty()) {
      restURLBuilder.append("onGroups=" + groups);
    }
    else if (members != null && !members.isEmpty()) {
      restURLBuilder.append("onMembers=" + members);
    }
    if (filter != null && !filter.isEmpty()) {
      restURLBuilder.append("&filter=" + filter);
    }
    String restString = restURLBuilder.toString();
    HttpPost post = new HttpPost(restString);
    post.addHeader("Content-Type", "application/json");
    post.addHeader("Accept", "application/json");
    if (jsonBody != null && !StringUtils.isEmpty(jsonBody)) {
      StringEntity jsonStringEntity = new StringEntity(jsonBody, ContentType.DEFAULT_TEXT);
      post.setEntity(jsonStringEntity);
    }
    return post;
  }

  protected String getFunctionID() {
    throw new RuntimeException("This method should be overridden");
  }

  protected void assertHttpResponse(CloseableHttpResponse response, int httpCode, int expectedServerResponses) {
    assertEquals(httpCode, response.getStatusLine().getStatusCode());

    //verify response has body flag, expected is true.
    assertNotNull(response.getEntity());
    try {
      String httpResponseString = processHttpResponse(response);
      response.close();
      System.out.println("Response : " + httpResponseString);
      //verify function execution result
      JSONArray resultArray = new JSONArray(httpResponseString);
      assertEquals(resultArray.length(), expectedServerResponses);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String processHttpResponse(HttpResponse response) {
    try {
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
              content));
      String line;
      StringBuffer sb = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    } catch (IOException e) {
      LogWriterUtils.getLogWriter().error("Error in processing Http Response", e);
    }
    return "";
  }

  protected void assertCorrectInvocationCount(int expectedInvocationCount, VM... vms) {
    int count = 0;
    for (int i = 0; i < vms.length; i++) {
      count += vms[i].invoke("getInvocationCount",() -> getInvocationCount());
    }
    assertEquals(expectedInvocationCount,count);
  }

  protected void resetInvocationCount() {
    RestFunctionTemplate f = (RestFunctionTemplate) FunctionService.getFunction(getFunctionID());
    f.invocationCount = 0;
  }

  protected void resetInvocationCounts(VM... vms) {
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke("resetInvocationCount", () -> resetInvocationCount());
    }
  }
}
