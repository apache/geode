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

package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


@Category(IntegrationTest.class)
public class ClusterConfigurationLoaderIntegrationTest {

  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  private ClusterConfigurationLoader loader;

  @Before
  public void before() {
    loader = new ClusterConfigurationLoader();
  }

  @Test
  public void requestForClusterConfiguration() throws Exception {
    Set<InternalDistributedMember> locators = new HashSet<>();
    locators.add((InternalDistributedMember) locator.getLocator().getDistributedSystem()
        .getDistributedMember());
    ConfigurationResponse response = loader.requestConfigurationFromLocators("", locators);
    Map<String, Configuration> configurationMap = response.getRequestedConfiguration();
    assertThat(configurationMap.size()).isEqualTo(1);
    assertThat(configurationMap.get("cluster")).isNotNull();
  }
}
