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

package org.apache.geode.management;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.arakelian.jq.ImmutableJqLibrary;
import com.arakelian.jq.ImmutableJqRequest;
import com.arakelian.jq.JqLibrary;
import com.arakelian.jq.JqRequest;
import com.arakelian.jq.JqResponse;
import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.DiskDir;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

/**
 * this test is to make sure: 1. we have a test to verify the jq filter we add to the controller 2.
 * the JQFilter we added is valid
 */
public class JQFilterVerificationDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  private static GeodeDevRestClient client;
  private static final Map<String, JsonNode> apiWithJQFilters = new HashMap<>();
  private static JqLibrary library;

  @BeforeClass
  public static void beforeClass() throws IOException {
    MemberVM locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    cluster.startServerVM(1, locator.getPort());
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setPort(locator.getHttpPort()).build();
    Region region = new Region();
    region.setName("regionA");
    region.setType(RegionType.REPLICATE);
    cms.create(region);

    Index index1 = new Index();
    index1.setName("index1");
    index1.setExpression("id");
    index1.setRegionPath(SEPARATOR + "regionA");
    index1.setIndexType(IndexType.RANGE);
    cms.create(index1);

    DiskStore diskStore = new DiskStore();
    diskStore.setName("diskstore1");
    DiskDir diskDir = new DiskDir("./diskDir", null);
    diskStore.setDirectories(Collections.singletonList(diskDir));
    cms.create(diskStore);

    client = new GeodeDevRestClient("/management", "localhost", locator.getHttpPort(), false);
    JsonNode jsonObject =
        client.doGetAndAssert("/v1/api-docs").getJsonObject().get("paths");
    Iterator<Map.Entry<String, JsonNode>> urls = jsonObject.fields();
    while (urls.hasNext()) {
      Map.Entry<String, JsonNode> url = urls.next();
      Iterator<Map.Entry<String, JsonNode>> methods = url.getValue().fields();
      while (methods.hasNext()) {
        Map.Entry<String, JsonNode> method = methods.next();
        // gather all the rest endpoint that has jqFilter defined.
        if (method.getValue().get("jqFilter") != null) {
          apiWithJQFilters.put(url.getKey(), method.getValue());
        }
      }
    }

    library = ImmutableJqLibrary.of();
  }

  @AfterClass
  public static void afterClass() {
    // after all the tests are done, the map has nothing left.
    assertThat(apiWithJQFilters).hasSize(0);
  }

  @Test
  public void getMembers() throws IOException {
    String uri = "/v1/members";
    JqResponse response =
        getJqResponse(uri, apiWithJQFilters.remove(uri).get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"locator-0\"");
  }

  @Test
  public void getMember() throws Exception {
    String uri = "/v1/members/locator-0";
    JqResponse response = getJqResponse(uri,
        apiWithJQFilters.remove("/v1/members/{id}").get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"locator-0\"");
  }

  @Test
  public void listRegions() throws Exception {
    String uri = "/v1/regions";
    JqResponse response =
        getJqResponse(uri, apiWithJQFilters.remove(uri).get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"regionA\"");
  }

  JqResponse getJqResponse(String uri, String jqFilter) throws IOException {
    JsonNode jsonObject = client.doGetAndAssert(uri).getJsonObject();
    final JqRequest request = ImmutableJqRequest.builder()
        .lib(library).input(jsonObject.toString()).filter(jqFilter).build();
    return request.execute();
  }

  @Test
  public void getRegion() throws Exception {
    String uri = "/v1/regions/regionA";
    JqResponse response = getJqResponse(uri,
        apiWithJQFilters.remove("/v1/regions/{id}").get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"regionA\"");
  }

  @Test
  public void listIndex() throws Exception {
    String uri = "/v1/indexes";
    JqResponse response =
        getJqResponse(uri, apiWithJQFilters.remove(uri).get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"index1\"");
  }

  @Test
  public void listRegionIndex() throws Exception {
    String uri = "/v1/regions/regionA/indexes";
    JqResponse response = getJqResponse(uri, apiWithJQFilters
        .remove("/v1/regions/{regionName}/indexes").get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"index1\"");
  }

  @Test
  public void getIndex() throws Exception {
    String uri = "/v1/regions/regionA/indexes/index1";
    JqResponse response = getJqResponse(uri, apiWithJQFilters
        .remove("/v1/regions/{regionName}/indexes/{id}").get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"name\":\"index1\"");
  }

  @Test
  public void listDiskStores() throws Exception {
    String uri = "/v1/diskstores";
    JqResponse response =
        getJqResponse(uri, apiWithJQFilters.remove(uri).get("jqFilter").textValue());
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"Member\":\"server-1\"");
    Assertions.assertThat(response.getOutput()).contains("\"Disk Store Name\":\"diskstore1\"");
  }

  @Test
  public void getDiskStore() throws Exception {
    String uri = "/v1/diskstores/diskstore1";
    JqResponse response =
        getJqResponse(uri,
            apiWithJQFilters.remove("/v1/diskstores/{id}").get("jqFilter").textValue());
    response.getErrors().forEach(System.out::println);
    Assertions.assertThat(response.hasErrors()).isFalse();
    System.out.println("JQ output: " + response.getOutput());
    Assertions.assertThat(response.getOutput()).contains("\"Member\":\"server-1\"");
    Assertions.assertThat(response.getOutput()).contains("\"Disk Store Name\":\"diskstore1\"");
  }
}
