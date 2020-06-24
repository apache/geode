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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class InternalDistributedSystemSecurityIntegrationTest {

  private InternalDistributedSystem system;
  private MetricsService.Builder metricsSessionBuilder;

  @Before
  public void setup() {
    metricsSessionBuilder = mock(MetricsService.Builder.class);
    when(metricsSessionBuilder.build(any(), any())).thenReturn(mock(MetricsService.class));
  }

  @After
  public void tearDown() {
    system.disconnect();
  }

  @Test
  public void disconnectClosesSecurityManager() {
    SecurityManager theSecurityManager = mock(SecurityManager.class);

    SecurityConfig securityConfig =
        new SecurityConfig(theSecurityManager, mock(PostProcessor.class));
    Properties configProperties = new Properties();

    system =
        InternalDistributedSystem.connectInternal(configProperties, securityConfig,
            metricsSessionBuilder, new ServiceLoaderModuleService(LogService.getLogger()));

    system.disconnect();

    verify(theSecurityManager).close();
  }

  @Test
  public void disconnectClosesPostProcessor() {
    PostProcessor thePostProcessor = mock(PostProcessor.class);

    SecurityConfig securityConfig =
        new SecurityConfig(mock(SecurityManager.class), thePostProcessor);
    Properties configProperties = new Properties();

    system = InternalDistributedSystem.connectInternal(
        configProperties, securityConfig, metricsSessionBuilder,
        new ServiceLoaderModuleService(LogService.getLogger()));

    system.disconnect();

    verify(thePostProcessor).close();
  }
}
