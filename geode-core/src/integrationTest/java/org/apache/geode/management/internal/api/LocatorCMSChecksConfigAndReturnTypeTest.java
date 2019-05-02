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

package org.apache.geode.management.internal.api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class LocatorCMSChecksConfigAndReturnTypeTest {

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  private static ClusterManagementService service;

  @BeforeClass
  public static void before() {
    service = locator.getLocator().getClusterManagementService();
  }

  @Test
  public void checkCorrectConfigAndReturnType() {
    service.list(new RegionConfig(), RuntimeRegionConfig.class);
    service.list(new MemberConfig(), MemberConfig.class);
  }

  @Test
  public void checkMismatchedConfigAndReturnType() {
    assertThatThrownBy(() -> service.list(new MemberConfig(), RuntimeRegionConfig.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Mismatched request type and return type:");
  }
}
