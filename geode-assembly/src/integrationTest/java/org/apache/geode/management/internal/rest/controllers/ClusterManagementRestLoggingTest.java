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

package org.apache.geode.management.internal.rest.controllers;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.ClientClusterManagementService;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class ClusterManagementRestLoggingTest {
  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withHttpService()
      .withSystemProperty("geode.management.request.logging", "false").withAutoStart();

  private static ClusterManagementService cms;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cms = new ClientClusterManagementService("localhost", locator.getHttpPort());
  }

  @Test
  public void ping() throws Exception {
    assertThat(cms.isConnected()).isTrue();
  }

}
