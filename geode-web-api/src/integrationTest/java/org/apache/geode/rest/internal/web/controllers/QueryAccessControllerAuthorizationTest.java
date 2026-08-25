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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.GenericXmlWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.context.web.WebMergedContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.internal.HttpService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Verifies the permissions the named-query endpoints of {@link QueryAccessController} require.
 *
 * <p>
 * The endpoints that read query state require {@code DATA:READ}; the endpoints that create, update
 * or remove a stored named query all require {@code DATA:WRITE}, matching the permission required
 * for the equivalent operations on ordinary region data.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-servlet.xml"},
    loader = SecuredTestContextLoader.class)
@WebAppConfiguration
public class QueryAccessControllerAuthorizationTest {

  private static final String QUERY_STORE = "__ParameterizedQueries__";
  private static final String REGION_NAME = "customers";

  private static final String READ_USER = "dataRead";
  private static final String WRITE_USER = "dataWrite";

  private static final String OQL = "SELECT * FROM " + Region.SEPARATOR + REGION_NAME;
  private static final String OTHER_OQL =
      "SELECT c.name FROM " + Region.SEPARATOR + REGION_NAME + " c";

  private static final RequestPostProcessor JSON = new JsonRequestPostProcessor();

  @ClassRule
  public static ServerStarterRule rule = new ServerStarterRule()
      .withProperty("log-level", "warn")
      .withSecurityManager(SimpleSecurityManager.class)
      .withRegion(RegionShortcut.REPLICATE, REGION_NAME);

  @Autowired
  private WebApplicationContext webApplicationContext;

  private MockMvc mockMvc;

  @BeforeClass
  public static void createQueryStore() {
    RestAgent.createParameterizedQueryRegion();
  }

  @Before
  public void setUp() {
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .apply(springSecurity())
        .build();
    queryStore().clear();
  }

  @SuppressWarnings("unchecked")
  private static Region<String, String> queryStore() {
    return rule.getCache().getInternalRegionByPath(Region.SEPARATOR + QUERY_STORE);
  }

  @Test
  public void createIsRefusedForAUserWithoutWritePermission() throws Exception {
    mockMvc.perform(post("/v1/queries?id=q1&q=" + OQL)
        .with(httpBasic(READ_USER, READ_USER))
        .with(JSON))
        .andExpect(status().isForbidden());

    assertThat(queryStore()).doesNotContainKey("q1");
  }

  @Test
  public void updateIsRefusedForAUserWithoutWritePermission() throws Exception {
    queryStore().put("q1", OQL);

    mockMvc.perform(put("/v1/queries/q1?q=" + OTHER_OQL)
        .with(httpBasic(READ_USER, READ_USER))
        .with(JSON))
        .andExpect(status().isForbidden());

    assertThat(queryStore().get("q1")).isEqualTo(OQL);
  }

  @Test
  public void deleteIsRefusedForAUserWithoutWritePermission() throws Exception {
    queryStore().put("q1", OQL);

    mockMvc.perform(delete("/v1/queries/q1")
        .with(httpBasic(READ_USER, READ_USER))
        .with(JSON))
        .andExpect(status().isForbidden());

    assertThat(queryStore()).containsKey("q1");
  }

  @Test
  public void createAndUpdateAreAllowedForAUserWithWritePermission() throws Exception {
    mockMvc.perform(post("/v1/queries?id=q1&q=" + OQL)
        .with(httpBasic(WRITE_USER, WRITE_USER))
        .with(JSON))
        .andExpect(status().isCreated());

    assertThat(queryStore().get("q1")).isEqualTo(OQL);

    mockMvc.perform(put("/v1/queries/q1?q=" + OTHER_OQL)
        .with(httpBasic(WRITE_USER, WRITE_USER))
        .with(JSON))
        .andExpect(status().isOk());

    assertThat(queryStore().get("q1")).isEqualTo(OTHER_OQL);
  }

  @Test
  public void listIsAllowedForAUserWithReadPermission() throws Exception {
    queryStore().put("q1", OQL);

    mockMvc.perform(get("/v1/queries")
        .with(httpBasic(READ_USER, READ_USER))
        .with(JSON))
        .andExpect(status().isOk());
  }

  private static class JsonRequestPostProcessor implements RequestPostProcessor {

    @SuppressWarnings("deprecation")
    private static final MediaType APPLICATION_JSON_UTF8 = MediaType.APPLICATION_JSON_UTF8;

    @Override
    public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
      request.addHeader(HttpHeaders.ACCEPT, APPLICATION_JSON_UTF8);
      request.addHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON_UTF8);
      return request;
    }
  }
}


class SecuredTestContextLoader extends GenericXmlWebContextLoader {
  @Override
  protected void loadBeanDefinitions(GenericWebApplicationContext context,
      WebMergedContextConfiguration webMergedConfig) {
    super.loadBeanDefinitions(context, webMergedConfig);
    context.getServletContext().setAttribute(
        HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
        QueryAccessControllerAuthorizationTest.rule.getCache().getSecurityService());
  }
}
