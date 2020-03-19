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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.test.matchers.JsonEquivalence.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import com.jayway.jsonpath.JsonPath;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.GenericXmlWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.context.web.WebMergedContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-servlet.xml"},
    loader = TestContextLoader.class)
@WebAppConfiguration
public class RestAccessControllerTest {

  private static final String BASE_URL = "http://localhost/v1";

  private static final String ORDER1_JSON = "order1.json";
  private static final String ORDER1_ARRAY_JSON = "order1-array.json";
  private static final String ORDER1_NO_TYPE_JSON = "order1-no-type.json";
  private static final String ORDER2_JSON = "order2.json";
  private static final String ORDER2_UPDATED_JSON = "order2-updated.json";
  private static final String MALFORMED_JSON = "malformed.json";
  private static final String CUSTOMER_LIST_JSON = "customer-list.json";
  private static final String CUSTOMER_LIST_NO_TYPE_JSON = "customer-list-no-type.json";
  private static final String ORDER_CAS_JSON = "order-cas.json";
  private static final String ORDER_CAS_OLD_JSON = "order-cas-old.json";
  private static final String ORDER_CAS_NEW_JSON = "order-cas-new.json";
  private static final String ORDER_CAS_WRONG_OLD_JSON = "order-cas-wrong-old.json";

  private static Map<String, String> jsonResources = new HashMap<>();

  private static RequestPostProcessor POST_PROCESSOR = new StandardRequestPostProcessor();

  private MockMvc mockMvc;

  private static Region<?, ?> orderRegion;
  private static Region<?, ?> customerRegion;

  @ClassRule
  public static ServerStarterRule rule = new ServerStarterRule()
      .withProperty("log-level", "warn")
      .withPDXReadSerialized()
      .withRegion(RegionShortcut.REPLICATE, "customers")
      .withRegion(RegionShortcut.REPLICATE, "orders");

  @Autowired
  private WebApplicationContext webApplicationContext;

  @BeforeClass
  public static void setupCClass() throws Exception {
    loadResource(ORDER1_JSON);
    loadResource(ORDER1_ARRAY_JSON);
    loadResource(ORDER1_NO_TYPE_JSON);
    loadResource(ORDER2_JSON);
    loadResource(MALFORMED_JSON);
    loadResource(CUSTOMER_LIST_JSON);
    loadResource(CUSTOMER_LIST_NO_TYPE_JSON);
    loadResource(ORDER2_UPDATED_JSON);
    loadResource(ORDER_CAS_JSON);
    loadResource(ORDER_CAS_OLD_JSON);
    loadResource(ORDER_CAS_NEW_JSON);
    loadResource(ORDER_CAS_WRONG_OLD_JSON);

    RestAgent.createParameterizedQueryRegion();

    FunctionService.registerFunction(new AddFreeItemToOrders());
    FunctionService.registerFunction(new EchoArgumentFunction());
    FunctionService.registerFunction(new GetOrderDescriptionFunction());


    rule.createRegion(RegionShortcut.REPLICATE_PROXY, "empty",
        f -> f.setCacheLoader(new SimpleCacheLoader()).setCacheWriter(new SimpleCacheWriter()));

    customerRegion = rule.getCache().getRegion("customers");
    orderRegion = rule.getCache().getRegion("orders");
  }

  private static void loadResource(String name) throws Exception {
    URL orderCasJson = RestAccessControllerTest.class.getResource(name);
    jsonResources.put(name, new String(Files.readAllBytes(Paths.get(orderCasJson.toURI()))));
  }

  @Before
  public void setup() {
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();

    customerRegion.clear();
    orderRegion.clear();
  }

  @Test
  @WithMockUser
  public void postEntry() throws Exception {
    mockMvc.perform(post("/v1/orders?key=1")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/orders/1"));

    mockMvc.perform(post("/v1/orders?key=1")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isConflict());

    Order order = (Order) ((PdxInstance) orderRegion.get("1")).getObject();
    assertThat(order).as("order should not be null").isNotNull();
  }

  @Test
  @WithMockUser
  public void postEntryWithJsonArrayOfOrders() throws Exception {
    mockMvc.perform(post("/v1/orders?key=1")
        .content(jsonResources.get(ORDER1_ARRAY_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/orders/1"));

    mockMvc.perform(post("/v1/orders?key=1")
        .content(jsonResources.get(ORDER1_ARRAY_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isConflict());

    @SuppressWarnings("unchecked")
    List<PdxInstance> entries = (List<PdxInstance>) orderRegion.get("1");
    Order order = (Order) entries.get(0).getObject();
    assertThat(order).as("order should not be null").isNotNull();
  }

  @Test
  @WithMockUser
  public void failPostEntryWithInvalidJson() throws Exception {
    mockMvc.perform(post("/v1/orders?key=1")
        .content(jsonResources.get(MALFORMED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath("$.cause", is("Json doc specified is either not supported or invalid!")));
  }

  @Test
  @WithMockUser
  public void failPostEntryWithInvalidRegion() throws Exception {
    mockMvc.perform(post("/v1/unknown?key=1")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failPostEntryOnRegionWithDataPolicyEmpty() throws Exception {
    mockMvc.perform(post("/v1/empty?key=1")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.message",
                is("Resource (empty) configuration does not support the requested operation!")));
  }

  @Test
  @WithMockUser
  public void failGettingEntryFromUnknownRegion() throws Exception {
    mockMvc.perform(get("/v1/unknown/10")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failGettingEntryWhenCacheLoaderFails() throws Exception {
    mockMvc.perform(get("/v1/empty/10")
        .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.message",
                is("Server has encountered timeout error while processing this request!")));
  }

  @Test
  @WithMockUser
  public void putEntry() throws Exception {
    mockMvc.perform(put("/v1/orders/2")
        .content(jsonResources.get(ORDER2_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL + "/orders/2"));

    mockMvc.perform(put("/v1/orders/2")
        .content(jsonResources.get(ORDER2_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL + "/orders/2"));
  }

  @Test
  @WithMockUser
  public void failPutEntryWithInvalidJson() throws Exception {
    mockMvc.perform(put("/v1/orders/1")
        .content(jsonResources.get(MALFORMED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath("$.cause", is("Json doc specified is either not supported or invalid!")));
  }

  @Test
  @WithMockUser
  public void failPutEntryWithInvalidRegion() throws Exception {
    mockMvc.perform(put("/v1/unknown/1")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failPutEntryOnRegionWhenCacheWriterFails() throws Exception {
    mockMvc.perform(put("/v1/empty/1")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.message",
                is("Server has encountered CacheWriter error while processing this request!")));
  }

  @Test
  @WithMockUser
  public void putAll() throws Exception {
    mockMvc.perform(
        put("/v1/customers/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60")
            .content(jsonResources.get(CUSTOMER_LIST_JSON))
            .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL
            + "/customers/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60"));
  }

  @Test
  @WithMockUser
  public void failPutAllWithInvalidJson() throws Exception {
    mockMvc.perform(put("/v1/customers/1,2,3,4")
        .content(jsonResources.get(MALFORMED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.cause", is("JSON document specified in the request is incorrect")));
  }

  @Test
  @WithMockUser
  public void failPutAllWithInvalidRegion() throws Exception {
    mockMvc.perform(
        put("/v1/unknown/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60")
            .content(jsonResources.get(CUSTOMER_LIST_JSON))
            .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failPutAllWhenCacheWriterFails() throws Exception {
    mockMvc.perform(
        put("/v1/empty/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60")
            .content(jsonResources.get(CUSTOMER_LIST_JSON))
            .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.cause", is("Put request failed as gemfire has thrown an error.")));
  }

  @Test
  @WithMockUser
  public void putWithReplace() throws Exception {
    // First time through the key does not exist and we get a 404
    mockMvc.perform(put("/v1/orders/2?op=REPLACE")
        .content(jsonResources.get(ORDER2_UPDATED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());

    // Create an entry that we can subsequently update
    mockMvc.perform(put("/v1/orders/2")
        .content(jsonResources.get(ORDER2_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL
            + "/orders/2"));

    // Do the actual update
    mockMvc.perform(put("/v1/orders/2?op=REPLACE")
        .content(jsonResources.get(ORDER2_UPDATED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL
            + "/orders/2"));

    // Check the updated value
    mockMvc.perform(get("/v1/orders/2")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json(jsonResources.get(ORDER2_UPDATED_JSON)));
  }

  @Test
  @WithMockUser
  public void failPutWithReplaceWithInvalidJson() throws Exception {
    mockMvc.perform(put("/v1/orders/1?op=REPLACE")
        .content(jsonResources.get(MALFORMED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath("$.cause", is("Json doc specified is either not supported or invalid!")));
  }

  @Test
  @WithMockUser
  public void failPutWithReplaceWithInvalidRegion() throws Exception {
    mockMvc.perform(put("/v1/unknown/1?op=REPLACE")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failPutWithReplaceFailsOnEmptyRegion() throws Exception {
    mockMvc.perform(put("/v1/empty/10?op=REPLACE")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.message",
                is("Resource (empty) configuration does not support the requested operation!")));
  }

  @Test
  @WithMockUser
  public void putWithCas() throws Exception {
    // First time through the key does not exist and we get a 404
    mockMvc.perform(put("/v1/orders/3?op=CAS")
        .content(jsonResources.get(ORDER_CAS_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk()) // This is wrong!!!
        .andExpect(header().string("Location", BASE_URL
            + "/orders/3"));

    // Create an entry that we can subsequently update
    mockMvc.perform(put("/v1/orders/3")
        .content(jsonResources.get(ORDER_CAS_OLD_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL
            + "/orders/3"));

    // Check the value
    mockMvc.perform(get("/v1/orders/3")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json(jsonResources.get(ORDER_CAS_OLD_JSON)));

    // Try and update with an incorrect old value
    mockMvc.perform(put("/v1/orders/3?op=CAS")
        .content(jsonResources.get(ORDER_CAS_WRONG_OLD_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isConflict())
        .andExpect(header().string("Location", BASE_URL
            + "/orders/3"))
        .andExpect(content().json(jsonResources.get(ORDER_CAS_OLD_JSON)));

    // Check that the value has not changed
    mockMvc.perform(get("/v1/orders/3")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json(jsonResources.get(ORDER_CAS_OLD_JSON)));

    // Do the actual update
    mockMvc.perform(put("/v1/orders/3?op=CAS")
        .content(jsonResources.get(ORDER_CAS_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Location", BASE_URL + "/orders/3"));

    // Check the updated value
    mockMvc.perform(get("/v1/orders/3")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json(jsonResources.get(ORDER_CAS_NEW_JSON)));
  }

  @Test
  @WithMockUser
  public void failPutWithCasWithInvalidJson() throws Exception {
    mockMvc.perform(put("/v1/orders/1?op=CAS")
        .content(jsonResources.get(MALFORMED_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isBadRequest())
        .andExpect(
            jsonPath("$.cause", is("Json doc specified in request body is invalid!")));
  }

  @Test
  @WithMockUser
  public void failPutWithCasWithInvalidRegion() throws Exception {
    mockMvc.perform(put("/v1/unknown/10?op=CAS")
        .content(jsonResources.get(ORDER_CAS_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failPutWithCasFailsOnEmptyRegion() throws Exception {
    mockMvc.perform(put("/v1/empty/10?op=CAS")
        .content(jsonResources.get(ORDER_CAS_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.message",
                is("Resource (empty) configuration does not support the requested operation!")));
  }

  @Test
  @WithMockUser
  public void getRegions() throws Exception {
    mockMvc.perform(get("/v1")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$.regions[*].name", containsInAnyOrder("customers", "orders", "empty")))
        .andExpect(
            jsonPath("$.regions[*].type", containsInAnyOrder("REPLICATE", "REPLICATE", "EMPTY")));
  }

  @Test
  @WithMockUser
  public void postRegions() throws Exception {
    mockMvc.perform(post("/v1")
        .with(POST_PROCESSOR))
        .andExpect(status().isMethodNotAllowed())
        .andExpect(jsonPath("$.cause", is("Request method 'POST' not supported")));
  }

  @SuppressWarnings("unchecked")
  @Test
  @WithMockUser
  public void getCustomers() throws Exception {
    putAll();
    mockMvc.perform(get("/v1/customers?limit=ALL")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.customers", jsonEquals(jsonResources.get(CUSTOMER_LIST_JSON))));
  }

  @Test
  @WithMockUser
  public void getFromInvalidRegion() throws Exception {
    putAll();
    mockMvc.perform(get("/v1/unknown")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void getCustomersWithLimit() throws Exception {
    putAll();
    mockMvc.perform(get("/v1/customers?limit=10")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.customers.length()", is(10)));
  }

  @Test
  @WithMockUser
  public void getAllKeys() throws Exception {
    putAll();
    mockMvc.perform(get("/v1/customers/keys")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.keys",
            containsInAnyOrder(IntStream.rangeClosed(1, 60).mapToObj(i -> i + "").toArray())));
  }

  @Test
  @WithMockUser
  public void getAllKeysFromInvalidRegion() throws Exception {
    mockMvc.perform(get("/v1/unknown/keys")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void failPostAllKeys() throws Exception {
    mockMvc.perform(post("/v1/unknown/keys")
        .with(POST_PROCESSOR))
        .andExpect(status().isMethodNotAllowed())
        .andExpect(jsonPath("$.cause", is("Request method 'POST' not supported")));
  }

  @Test
  @WithMockUser
  public void getSpecificKeys() throws Exception {
    putAll();
    mockMvc.perform(get("/v1/customers/1,2,3,4,5")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$.customers[*].customerId", containsInAnyOrder(101, 102, 103, 104, 105)));
  }

  @Test
  @WithMockUser
  public void getSpecificKeysFromUnknownRegion() throws Exception {
    mockMvc.perform(get("/v1/unknown/1,2,3,4,5")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound())
        .andExpect(
            jsonPath("$.cause", is("The Region identified by name (unknown) could not be found!")));
  }

  @Test
  @WithMockUser
  public void deleteSingleKey() throws Exception {
    putAll();
    mockMvc.perform(delete("/v1/customers/1")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());
  }

  @Test
  @WithMockUser
  public void deleteFromInvalidRegion() throws Exception {
    mockMvc.perform(delete("/v1/unknown/1")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());
  }

  @Test
  @WithMockUser
  public void deleteMultipleKeys() throws Exception {
    putAll();
    mockMvc.perform(delete("/v1/customers/2,3,4,5")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());
  }

  @Test
  @WithMockUser
  public void deleteAllKeys() throws Exception {
    putAll();
    mockMvc.perform(delete("/v1/customers")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());

    mockMvc.perform(head("/v1/customers")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Resource-Count", "0"));
  }

  @Test
  @WithMockUser
  public void deleteMultipleKeysFromInvalidRegion() throws Exception {
    mockMvc.perform(delete("/v1/unknown/2,3,4,5")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());
  }

  @Test
  @WithMockUser
  public void deleteMultipleKeysFromEmptyRegion() throws Exception {
    mockMvc.perform(delete("/v1/empty/2,3,4,5")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());
  }

  @Test
  @WithMockUser
  public void createQueries() throws Exception {
    deleteAllQueries();

    mockMvc.perform(post(
        "/v1/queries?id=selectOrder&q=SELECT DISTINCT o FROM /orders o, o.items item WHERE item.quantity > $1 AND item.totalPrice > $2")
            .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/queries/selectOrder"));

    mockMvc.perform(
        post("/v1/queries?id=selectCustomer&q=SELECT c FROM /customers c WHERE c.customerId = $1")
            .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/queries/selectCustomer"));

    mockMvc.perform(post(
        "/v1/queries?id=selectHighRoller&q=SELECT DISTINCT c FROM /customers c, /orders o, o.items item WHERE item.totalprice > $1 AND c.customerId = o.customerId")
            .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/queries/selectHighRoller"));

    mockMvc.perform(get("/v1/queries")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$.queries[*].id",
                containsInAnyOrder("selectOrder", "selectCustomer", "selectHighRoller")));
  }

  @SuppressWarnings("unchecked")
  @Test
  @WithMockUser
  public void executeQueryWithParams() throws Exception {
    String QUERY_ARGS =
        "[{\"@type\": \"int\", \"@value\": 2}, {\"@type\": \"double\", \"@value\": 150.00}]";

    postEntry(); // order 1
    putEntry(); // order 2
    createQueries();
    mockMvc.perform(post("/v1/queries/selectOrder")
        .with(POST_PROCESSOR)
        .content(QUERY_ARGS))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.[0]", jsonEquals(jsonResources.get(ORDER1_NO_TYPE_JSON))));
  }

  @Test
  @WithMockUser
  public void executeAdhocQuery() throws Exception {
    putAll();
    mockMvc.perform(get("/v1/queries/adhoc?q=SELECT * FROM /customers LIMIT 100")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json(jsonResources.get(CUSTOMER_LIST_NO_TYPE_JSON)));
  }

  @Test
  @WithMockUser
  public void putAndUpdateQuery() throws Exception {
    final String QUERY_ARGS = "[{\"@type\": \"int\", \"@value\": 20},"
        + "{\"@type\": \"int\", \"@value\": 120},"
        + "{\"@type\": \"int\", \"@value\": 130}]";

    deleteAllQueries();
    putAll(); // Create customers
    mockMvc.perform(
        post("/v1/queries?id=testQuery&q=SELECT DISTINCT c from /customers c where lastName=$1")
            .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/queries/testQuery"));

    mockMvc.perform(
        put("/v1/queries/testQuery?q=SELECT DISTINCT c from /customers c where customerId IN SET ($1, $2, $3)")
            .with(POST_PROCESSOR))
        .andExpect(status().isOk());

    mockMvc.perform(post("/v1/queries/testQuery")
        .content(QUERY_ARGS)
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.[*].customerId", containsInAnyOrder(120, 130)))
        .andExpect(jsonPath("$.[*].firstName", containsInAnyOrder("Preeti", "Varun")))
        .andExpect(jsonPath("$.[*].lastName", containsInAnyOrder("Kumari", "Agrawal")));
  }

  @Test
  @WithMockUser
  public void deleteQuery() throws Exception {
    mockMvc.perform(
        post("/v1/queries?id=myQuery&q=SELECT DISTINCT c from /customers c where lastName=$1")
            .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/queries/myQuery"));

    mockMvc.perform(delete("/v1/queries/myQuery")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());

    mockMvc.perform(get("/v1/queries/myQuery")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());

    mockMvc.perform(delete("/v1/queries/myQuery")
        .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());
  }

  @Test
  @WithMockUser
  public void putUnknownQuery() throws Exception {
    mockMvc.perform(
        put("/v1/queries/unknown?q=SELECT DISTINCT c from /customers c where customerId IN SET ($1, $2, $3)")
            .with(POST_PROCESSOR))
        .andExpect(status().isNotFound());
  }

  @Test
  @WithMockUser
  public void createQueryFromRequestBody() throws Exception {
    deleteAllQueries();

    mockMvc.perform(post("/v1/queries?id=testQuery")
        .content("SELECT * from /customers")
        .with(POST_PROCESSOR))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", BASE_URL + "/queries/testQuery"));

    mockMvc.perform(get("/v1/queries")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.queries[*].id", containsInAnyOrder("testQuery")))
        .andExpect(jsonPath("$.queries[*].oql", containsInAnyOrder("SELECT * from /customers")));

    mockMvc.perform(put("/v1/queries/testQuery")
        .content("SELECT * from /orders")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());

    mockMvc.perform(get("/v1/queries")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.queries[*].id", containsInAnyOrder("testQuery")))
        .andExpect(jsonPath("$.queries[*].oql", containsInAnyOrder("SELECT * from /orders")));
  }

  @Test
  @WithMockUser
  public void listFunctions() throws Exception {
    mockMvc.perform(get("/v1/functions"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.functions",
            containsInAnyOrder("AddFreeItemToOrders", "EchoArgumentFunction",
                "GetOrderDescriptionFunction")));
  }

  @Test
  @WithMockUser
  public void executeFunction() throws Exception {
    final String FUNCTION_ARGS = "[{\"@type\": \"double\", \"@value\": 210},"
        + "{\"@type\": \"org.apache.geode.rest.internal.web.controllers.Item\","
        + "\"itemNo\": 599, \"description\": \"Part X Free on Bumper Offer\","
        + "\"quantity\": 2, \"unitprice\": 5, \"totalprice\": 10.00}]";

    putEntry(); // order 2
    mockMvc.perform(post("/v1/functions/AddFreeItemToOrders?onRegion=orders")
        .content(FUNCTION_ARGS)
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());

    mockMvc.perform(get("/v1/orders/2")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items[*].itemNo", containsInAnyOrder(1, 2, 599)))
        .andExpect(jsonPath("$.items[*].description",
            containsInAnyOrder("Product-3", "Product-4", "Part X Free on Bumper Offer")));
  }

  @Test
  @WithMockUser
  public void executeFunctionWithFilter() throws Exception {
    final String FUNCTION_ARGS = "[{\"@type\": \"String\", \"@value\": \"argument\"}]";

    putEntry(); // order 2
    mockMvc.perform(post("/v1/functions/GetOrderDescriptionFunction?onRegion=orders&filter=2")
        .content(FUNCTION_ARGS)
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("Purchase order for company - B")));
  }

  @Test
  @WithMockUser
  public void executeFunctionWithFilterKeyNotFound() throws Exception {
    final String FUNCTION_ARGS = "[{\"@type\": \"String\", \"@value\": \"argument\"}]";

    putEntry(); // order 2
    mockMvc.perform(post("/v1/functions/GetOrderDescriptionFunction?onRegion=orders&filter=1")
        .content(FUNCTION_ARGS)
        .with(POST_PROCESSOR))
        .andExpect(status().isInternalServerError())
        .andExpect(
            jsonPath("$.message",
                is("Server has encountered an error while processing function execution!")));

  }

  @Test
  @WithMockUser
  public void executeNoArgFunctionWithInvalidArg() throws Exception {
    mockMvc.perform(post("/v1/functions/EchoArgumentFunction")
        .content("{\"type\": \"int\"}")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json("[{}]"));
  }

  @Test
  @WithMockUser
  public void executeNoArgFunctionWithEmptyObject() throws Exception {
    mockMvc.perform(post("/v1/functions/EchoArgumentFunction")
        .content("[{ }]")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json("[{}]"));
  }

  @Test
  @WithMockUser
  public void executeNoArgFunctionWithEmptyArray() throws Exception {
    mockMvc.perform(post("/v1/functions/EchoArgumentFunction")
        .content("[]")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json("[[]]"));
  }

  @Test
  @WithMockUser
  public void executeNoArgFunctionWithNoContent() throws Exception {
    mockMvc.perform(post("/v1/functions/EchoArgumentFunction")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json("[null]"));
  }

  @Test
  @WithMockUser
  public void getEntryAfterCreation() throws Exception {
    mockMvc.perform(post("/v1/orders?key=10")
        .content(jsonResources.get(ORDER1_JSON))
        .with(POST_PROCESSOR))
        .andExpect(status().isCreated());

    mockMvc.perform(get("/v1/orders/10")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(content().json(jsonResources.get(ORDER1_JSON)));
  }

  @Test
  @WithMockUser
  public void getPing() throws Exception {
    mockMvc.perform(get("/v1/ping")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());
  }

  @Test
  @WithMockUser
  public void headPing() throws Exception {
    mockMvc.perform(head("/v1/ping")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk());
  }

  @Test
  @WithMockUser
  public void headCustomers() throws Exception {
    putAll(); // load customers
    mockMvc.perform(head("/v1/customers")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andExpect(header().string("Resource-Count", "60"));
  }


  private void deleteAllQueries() throws Exception {
    MvcResult result = mockMvc.perform(get("/v1/queries")
        .with(POST_PROCESSOR))
        .andExpect(status().isOk())
        .andReturn();

    String content = result.getResponse().getContentAsString();
    List<String> ids = JsonPath.read(content, "$.queries[*].id");

    for (String id : ids) {
      mockMvc.perform(delete("/v1/queries/" + id)
          .with(POST_PROCESSOR))
          .andExpect(status().isOk());
    }
  }

  private static class StandardRequestPostProcessor implements RequestPostProcessor {

    @SuppressWarnings("deprecation")
    private static final MediaType APPLICATION_JSON_UTF8 = MediaType.APPLICATION_JSON_UTF8;

    @Override
    public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
      request.addHeader(HttpHeaders.ACCEPT, APPLICATION_JSON_UTF8);
      request.addHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON_UTF8);
      return request;
    }
  }

  static class SimpleCacheLoader implements CacheLoader<Object, Object>, Declarable {
    @Override
    public Object load(LoaderHelper<Object, Object> helper) {
      // throws TimeoutException
      throw new TimeoutException("Could not load entry. Request timed out.");
    }

    @Override
    public void close() {
      // nothing
    }

    @SuppressWarnings("deprecation")
    @Deprecated
    @Override
    public void init(Properties props) {
      // nothing
    }
  }

  static class SimpleCacheWriter implements CacheWriter<Object, Object> {
    @Override
    public void close() {
      // nothing
    }

    @Override
    public void beforeUpdate(EntryEvent<Object, Object> event) throws CacheWriterException {
      // nothing
    }

    @Override
    public void beforeCreate(EntryEvent<Object, Object> event) throws CacheWriterException {
      throw new CacheWriterException("Put request failed as gemfire has thrown an error.");
    }

    @Override
    public void beforeDestroy(EntryEvent<Object, Object> event) throws CacheWriterException {
      throw new RegionDestroyedException("Region has already been destroyed.", "dummyRegion");
    }

    @Override
    public void beforeRegionDestroy(RegionEvent<Object, Object> event) throws CacheWriterException {
      // nothing
    }

    @Override
    public void beforeRegionClear(RegionEvent<Object, Object> event) throws CacheWriterException {
      // nothing
    }
  }
}


class TestContextLoader extends GenericXmlWebContextLoader {
  @Override
  protected void loadBeanDefinitions(GenericWebApplicationContext context,
      WebMergedContextConfiguration webMergedConfig) {
    super.loadBeanDefinitions(context, webMergedConfig);
    context.getServletContext().setAttribute(
        HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
        RestAccessControllerTest.rule.getCache().getSecurityService());
  }

}
