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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.util.internal.GeodeGlossary;

public class InternalDistributedSystemLockMemoryIntegrationTest {

  private MetricsService.Builder builder;
  private InternalDistributedSystem system;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    builder = mock(MetricsService.Builder.class);
    when(builder.build(any(), any())).thenReturn(mock(MetricsService.class));
  }

  @After
  public void tearDown() {
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void lockMemoryAllowedIfAllowMemoryOverCommitIsSet() {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Cache.ALLOW_MEMORY_OVERCOMMIT", "true");
    system = spy(new InternalDistributedSystem.Builder(new Properties(), builder,
        new ServiceLoaderModuleService(LogService.getLogger())).build());
    doNothing().when(system).lockMemory();

    system.lockMemory(100, 200);

    verify(system).lockMemory();
  }

  @Test
  public void lockMemoryAvoidedIfAvoidMemoryLockWhenOverCommitIsSet() {
    System.setProperty(
        GeodeGlossary.GEMFIRE_PREFIX + "Cache.AVOID_MEMORY_LOCK_WHEN_OVERCOMMIT", "true");
    system = spy(new InternalDistributedSystem.Builder(new Properties(), builder,
        new ServiceLoaderModuleService(LogService.getLogger())).build());

    system.lockMemory(100, 200);

    verify(system, never()).lockMemory();
  }

  @Test
  public void lockMemoryAvoidedIfAvoidAndAllowMemoryLockWhenOverCommitBothSet() {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Cache.ALLOW_MEMORY_OVERCOMMIT", "true");
    System.setProperty(
        GeodeGlossary.GEMFIRE_PREFIX + "Cache.AVOID_MEMORY_LOCK_WHEN_OVERCOMMIT", "true");
    system = spy(new InternalDistributedSystem.Builder(new Properties(), builder,
        new ServiceLoaderModuleService(LogService.getLogger())).build());

    system.lockMemory(100, 200);

    verify(system, never()).lockMemory();
  }


  @Test
  public void lockMemoryThrowsIfMemoryOverCommit() {
    system = spy(new InternalDistributedSystem.Builder(new Properties(), builder,
        new ServiceLoaderModuleService(LogService.getLogger())).build());

    Throwable caughtException = catchThrowable(() -> system.lockMemory(100, 200));

    assertThat(caughtException).isInstanceOf(IllegalStateException.class);
    verify(system, never()).lockMemory();
  }

  @Test
  public void locksMemoryIfMemoryNotOverCommit() {
    system = spy(new InternalDistributedSystem.Builder(new Properties(), builder,
        new ServiceLoaderModuleService(LogService.getLogger())).build());
    doNothing().when(system).lockMemory();

    system.lockMemory(200, 100);

    verify(system).lockMemory();
  }
}
