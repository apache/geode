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

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class ManagementRestSecurityConfigurationDUnitTest {

  private MemberVM locator;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void testWithSecurityManager() {
    locator = cluster.startLocatorVM(0,
        x -> x.withHttpService().withSecurityManager(SimpleSecurityManager.class));
    GeodeDevRestClient client =
        new GeodeDevRestClient("/management", "localhost", locator.getHttpPort(), false);

    // Unsecured no credentials
    assertResponse(client.doGet("/swagger-ui.html", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", null, null)).hasStatusCode(200);

    // unsecured with credentials
    assertResponse(client.doGet("/swagger-ui.html", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", "cluster", "cluster")).hasStatusCode(200);

    // secured with credentials
    assertResponse(client.doGet("/v1/regions", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", "cluster", "cluster")).hasStatusCode(200);

    // secured no/incorrect credentials
    assertResponse(client.doGet("/v1/regions", null, null)).hasStatusCode(401);
    assertResponse(client.doGet("/v1/regions", "data", "invalid")).hasStatusCode(401);
    assertResponse(client.doGet("/v1/ping", null, null)).hasStatusCode(401);
    assertResponse(client.doGet("/v1/ping", "cluster", "invalid")).hasStatusCode(401);
  }

  @Test
  public void testWithoutSecurityManager() {
    locator = cluster.startLocatorVM(1, MemberStarterRule::withHttpService);
    GeodeDevRestClient client =
        new GeodeDevRestClient("/management", "localhost", locator.getHttpPort(), false);

    // Unsecured no credentials
    assertResponse(client.doGet("/swagger-ui.html", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", null, null)).hasStatusCode(200);

    // unsecured with credentials
    assertResponse(client.doGet("/swagger-ui.html", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/api-docs", "cluster", "cluster")).hasStatusCode(200);

    // secured with credentials
    assertResponse(client.doGet("/v1/regions", "cluster", "cluster")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", "cluster", "cluster")).hasStatusCode(200);

    // secured no/incorrect credentials
    assertResponse(client.doGet("/v1/regions", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/regions", "data", "invalid")).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", null, null)).hasStatusCode(200);
    assertResponse(client.doGet("/v1/ping", "cluster", "invalid")).hasStatusCode(200);
  }
}
