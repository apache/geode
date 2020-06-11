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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.services.module.ModuleService;

public class InternalDistributedSystemBuilderIntegrationTest {

  private InternalDistributedSystem system;
  private MetricsService.Builder metricsSessionBuilder;

  @Before
  public void setup() {
    metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any())).thenReturn(mock(MetricsService.class));
  }

  @After
  public void tearDown() {
    system.disconnect();
  }

  @Test
  public void buildBuildsAndInitializesSystem() {
    String theName = "theName";
    Properties configProperties = new Properties();
    configProperties.setProperty(NAME, theName);

    system =
        new InternalDistributedSystem.Builder(configProperties, metricsSessionBuilder, ModuleService
            .getDefaultModuleService())
                .build();

    assertThat(system.isConnected()).isTrue();
    assertThat(system.getName()).isEqualTo(theName);
  }

  @Test
  public void buildUsesSecurityConfig() {
    SecurityManager theSecurityManager = mock(SecurityManager.class);
    PostProcessor thePostProcessor = mock(PostProcessor.class);

    SecurityConfig securityConfig = new SecurityConfig(theSecurityManager, thePostProcessor);
    Properties configProperties = new Properties();

    system = new InternalDistributedSystem.Builder(configProperties, metricsSessionBuilder,
        ModuleService.getDefaultModuleService())
            .setSecurityConfig(securityConfig)
            .build();

    assertThat(system.getSecurityService().getSecurityManager())
        .isSameAs(theSecurityManager);
    assertThat(system.getSecurityService().getPostProcessor())
        .isSameAs(thePostProcessor);
  }
}
