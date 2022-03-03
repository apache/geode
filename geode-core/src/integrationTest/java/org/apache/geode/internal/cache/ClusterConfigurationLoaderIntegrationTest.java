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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.config.ClusterConfigurationNotAvailableException;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


public class ClusterConfigurationLoaderIntegrationTest {
  private ClusterConfigurationLoader loader;

  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule();

  @Before
  public void before() {
    loader = Mockito.spy(new ClusterConfigurationLoader());
  }

  @Test
  public void requestConfigurationFromLocatorsShouldThrowExceptionWhenClusterConfigurationServiceIsNotEnabled() {
    locator.withProperty("enable-cluster-configuration", "false").startLocator();

    Set<InternalDistributedMember> locators = new HashSet<>();
    locators.add((InternalDistributedMember) locator.getLocator().getDistributedSystem()
        .getDistributedMember());

    assertThatThrownBy(() -> loader.requestConfigurationFromLocators("", locators))
        .isInstanceOf(FunctionException.class).hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("The cluster configuration service is not enabled on this member.");
  }

  @Test
  public void requestConfigurationFromLocatorsShouldThrowExceptionAfterTheSixthNullConfigurationResponse() {
    CustomAnswer customAnswer = new CustomAnswer(10);
    Mockito.doAnswer(customAnswer).when(loader).requestConfigurationFromOneLocator(Mockito.any(),
        Mockito.anySet());

    locator.withProperty("enable-cluster-configuration", "true").startLocator();
    Set<InternalDistributedMember> locators = new HashSet<>();
    locators.add((InternalDistributedMember) locator.getLocator().getDistributedSystem()
        .getDistributedMember());

    assertThatThrownBy(() -> loader.requestConfigurationFromLocators("", locators))
        .isInstanceOf(ClusterConfigurationNotAvailableException.class)
        .hasMessageContaining("Unable to retrieve cluster configuration from the locator.");

    assertThat(customAnswer.calls).isEqualTo(6);
  }


  @Test
  public void requestConfigurationFromLocatorsShouldCorrectlyLoadTheClusterConfiguration()
      throws Exception {
    locator.withProperty("enable-cluster-configuration", "true").startLocator();

    Set<InternalDistributedMember> locators = new HashSet<>();
    locators.add((InternalDistributedMember) locator.getLocator().getDistributedSystem()
        .getDistributedMember());

    ConfigurationResponse response = loader.requestConfigurationFromLocators("", locators);
    Map<String, Configuration> configurationMap = response.getRequestedConfiguration();
    assertThat(configurationMap.size()).isEqualTo(1);
    assertThat(configurationMap.get("cluster")).isNotNull();
  }

  @Test
  public void requestConfigurationFromLocatorsShouldCorrectlyLoadTheClusterConfigurationEvenAfterSeveralRetries()
      throws Exception {
    int mockLimit = 6;
    CustomAnswer customAnswer = new CustomAnswer(mockLimit);
    Mockito.doAnswer(customAnswer).when(loader).requestConfigurationFromOneLocator(Mockito.any(),
        Mockito.anySet());

    locator.withProperty("enable-cluster-configuration", "true").startLocator();
    Set<InternalDistributedMember> locators = new HashSet<>();
    locators.add((InternalDistributedMember) locator.getLocator().getDistributedSystem()
        .getDistributedMember());

    ConfigurationResponse response = loader.requestConfigurationFromLocators("", locators);
    Map<String, Configuration> configurationMap = response.getRequestedConfiguration();
    assertThat(configurationMap.size()).isEqualTo(1);
    assertThat(configurationMap.get("cluster")).isNotNull();
    assertThat(customAnswer.calls).isEqualTo(mockLimit);
  }

  class CustomAnswer implements Answer {
    public int calls;
    public int mockLimit;

    public CustomAnswer(int mockLimit) {
      calls = 0;
      this.mockLimit = mockLimit;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      calls++;
      if (calls < mockLimit) {
        return null;
      } else {
        return invocation.callRealMethod();
      }
    }
  }
}
