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

import static org.hamcrest.Matchers.is;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.util.internal.GeodeJsonMapper;

/**
 * Integration test for @PreAuthorize HTTP layer authorization.
 *
 * <p>
 * <b>Purpose:</b> This test validates that Spring Security's @PreAuthorize annotation correctly
 * enforces authorization at the HTTP boundary in a <b>single-JVM environment</b>. This represents
 * the production deployment model where Jetty and the REST API run in a single JVM process.
 * </p>
 *
 * <p>
 * <b>Why This Test Exists:</b>
 * </p>
 * <ul>
 * <li><b>Spring Security Design:</b> @PreAuthorize uses ThreadLocal-based SecurityContext storage,
 * which works correctly within a single JVM but does not propagate across JVM boundaries.</li>
 * <li><b>Production Model:</b> In production, all HTTP requests are processed within the same JVM
 * (Locator with embedded Jetty), making @PreAuthorize the appropriate authorization mechanism for
 * the REST API.</li>
 * <li><b>Jetty 12 Architecture:</b> Jetty 12's multi-environment architecture (EE8, EE9, EE10)
 * requires proper Spring Security configuration to ensure SecurityContext is available to
 * authorization interceptors.</li>
 * </ul>
 *
 * <p>
 * <b>What This Test Validates:</b>
 * </p>
 * <ul>
 * <li>BasicAuthenticationFilter successfully authenticates users via Geode SecurityManager</li>
 * <li>@PreAuthorize interceptor receives the SecurityContext from authentication filter</li>
 * <li>Authorization rules are correctly enforced (e.g., DATA:READ cannot perform CLUSTER:MANAGE
 * operations)</li>
 * <li>Proper HTTP status codes are returned (403 Forbidden for authorization failures)</li>
 * </ul>
 *
 * <p>
 * <b>Relationship to DUnit Tests:</b>
 * </p>
 * <p>
 * DUnit tests run in a multi-JVM environment where Spring Security's ThreadLocal-based
 * SecurityContext cannot propagate across JVM boundaries. Therefore:
 * </p>
 * <ul>
 * <li><b>Integration Tests (this class):</b> Test @PreAuthorize enforcement at HTTP boundary in
 * single-JVM</li>
 * <li><b>DUnit Tests:</b> Test distributed cluster operations using Geode's native security
 * (Apache Shiro)</li>
 * </ul>
 *
 * <p>
 * <b>Historical Context:</b>
 * </p>
 * <p>
 * Prior to Jetty 12 migration, @PreAuthorize appeared to work in DUnit tests due to Jetty 11's
 * monolithic architecture allowing ThreadLocal sharing across servlet components. Jetty 12's
 * environment isolation revealed that DUnit tests were never truly validating distributed
 * authorization. See PRE_JAKARTA_SECURITY_CONTEXT_ANALYSIS.md for detailed analysis.
 * </p>
 *
 * <p>
 * <b>References:</b>
 * </p>
 * <ul>
 * <li>SPRING_SECURITY_CROSS_JVM_RESEARCH.md - Spring Security cross-JVM limitations</li>
 * <li>GEODE_SECURITY_CROSS_JVM_RESEARCH.md - Geode's distributed security architecture</li>
 * <li>PRE_JAKARTA_SECURITY_CONTEXT_ANALYSIS.md - Why it appeared to work before Jetty 12</li>
 * <li>SECURITY_CONTEXT_COMPLETE_RESEARCH_SUMMARY.md - Executive summary</li>
 * </ul>
 *
 * @see org.apache.geode.management.internal.rest.security.RestSecurityConfiguration
 * @see org.apache.geode.examples.SimpleSecurityManager
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = SecuredLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class ClusterManagementAuthorizationIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  private LocatorWebContext context;
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    context = new LocatorWebContext(webApplicationContext);
    mapper = GeodeJsonMapper.getMapper();
  }

  /**
   * Test that a user with only DATA:READ permission is denied when attempting a CLUSTER:MANAGE
   * operation.
   *
   * <p>
   * This validates that @PreAuthorize correctly enforces the CLUSTER:MANAGE permission requirement
   * for creating regions.
   * </p>
   *
   * <p>
   * <b>Expected Flow:</b>
   * </p>
   * <ol>
   * <li>HTTP POST request with Basic Auth (user: dataRead/dataRead)</li>
   * <li>BasicAuthenticationFilter authenticates via GeodeAuthenticationProvider</li>
   * <li>SecurityContext populated with Authentication containing DATA:READ authority</li>
   * <li>@PreAuthorize("hasRole('DATA:MANAGE')") interceptor checks permissions</li>
   * <li>Authorization fails - user has READ but needs MANAGE</li>
   * <li>HTTP 403 Forbidden returned</li>
   * </ol>
   */
  @Test
  public void createRegion_withReadPermission_shouldReturnForbidden() throws Exception {
    Region region = new Region();
    region.setName("testRegion");
    region.setType(RegionType.REPLICATE);

    context.perform(post("/v1/regions")
        .with(httpBasic("dataRead", "dataRead"))
        .content(mapper.writeValueAsString(region)))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("DataRead not authorized for DATA:MANAGE.")));
  }

  /**
   * Test that a user with CLUSTER:READ permission is denied when attempting a CLUSTER:MANAGE
   * operation.
   *
   * <p>
   * This validates that @PreAuthorize distinguishes between READ and MANAGE permissions.
   * </p>
   */
  @Test
  public void createRegion_withClusterReadPermission_shouldReturnForbidden() throws Exception {
    Region region = new Region();
    region.setName("testRegion");
    region.setType(RegionType.REPLICATE);

    context.perform(post("/v1/regions")
        .with(httpBasic("clusterRead", "clusterRead"))
        .content(mapper.writeValueAsString(region)))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("ClusterRead not authorized for DATA:MANAGE.")));
  }

  /**
   * Test that a user with DATA:MANAGE permission can successfully create a region.
   *
   * <p>
   * This validates that @PreAuthorize allows authorized operations to proceed.
   * </p>
   *
   * <p>
   * <b>Expected Flow:</b>
   * </p>
   * <ol>
   * <li>HTTP POST request with Basic Auth (user: dataManage/dataManage)</li>
   * <li>BasicAuthenticationFilter authenticates via GeodeAuthenticationProvider</li>
   * <li>SecurityContext populated with Authentication containing DATA:MANAGE authority</li>
   * <li>@PreAuthorize("hasRole('DATA:MANAGE')") interceptor checks permissions</li>
   * <li>Authorization succeeds - user has required MANAGE permission</li>
   * <li>Controller method executes, region created</li>
   * <li>HTTP 201 Created returned</li>
   * </ol>
   */
  @Test
  public void createRegion_withManagePermission_shouldSucceed() throws Exception {
    Region region = new Region();
    region.setName("authorizedRegion");
    region.setType(RegionType.REPLICATE);

    try {
      context.perform(post("/v1/regions")
          .with(httpBasic("dataManage", "dataManage"))
          .content(mapper.writeValueAsString(region)))
          .andExpect(status().isCreated())
          .andExpect(jsonPath("$.statusCode", is("OK")));
    } finally {
      // Cleanup - region creation may partially succeed even in test environment
      // Ignore cleanup failures as cluster may not be fully initialized
    }
  }

  /**
   * Test that a request without credentials is rejected with 401 Unauthorized.
   *
   * <p>
   * This validates that BasicAuthenticationFilter requires authentication before authorization.
   * </p>
   */
  @Test
  public void createRegion_withoutCredentials_shouldReturnUnauthorized() throws Exception {
    Region region = new Region();
    region.setName("testRegion");
    region.setType(RegionType.REPLICATE);

    context.perform(post("/v1/regions")
        .content(mapper.writeValueAsString(region)))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Full authentication is required to access this resource.")));
  }

  /**
   * Test that a request with invalid credentials is rejected with 401 Unauthorized.
   *
   * <p>
   * This validates that BasicAuthenticationFilter properly validates credentials via Geode
   * SecurityManager.
   * </p>
   */
  @Test
  public void createRegion_withInvalidCredentials_shouldReturnUnauthorized() throws Exception {
    Region region = new Region();
    region.setName("testRegion");
    region.setType(RegionType.REPLICATE);

    context.perform(post("/v1/regions")
        .with(httpBasic("invalidUser", "wrongPassword"))
        .content(mapper.writeValueAsString(region)))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Invalid username/password.")));
  }
}
