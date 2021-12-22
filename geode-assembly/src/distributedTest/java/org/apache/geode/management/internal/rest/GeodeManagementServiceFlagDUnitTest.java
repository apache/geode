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

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class GeodeManagementServiceFlagDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator;

  private GeodeDevRestClient restClient;

  @Test
  public void withOutService() throws Exception {
    locator = cluster.startLocatorVM(0,
        l -> l.withoutManagementRestService()
            .withHttpService());
    restClient =
        new GeodeDevRestClient("/management/v1", "localhost", locator.getHttpPort(),
            false);
    restClient.doGetAndAssert("/ping").hasStatusCode(404);
  }

  @Test
  public void withServiceByDefault() throws Exception {
    locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    restClient =
        new GeodeDevRestClient("/management/v1", "localhost", locator.getHttpPort(),
            false);
    restClient.doGetAndAssert("/ping").hasStatusCode(200).hasResponseBody().isEqualTo("pong");
  }
}
