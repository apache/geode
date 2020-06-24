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

package org.apache.geode.management.internal.cli.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.internal.CommandProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;

public class MemberCommandServiceTest {
  private InternalCache cache;
  private CommandProcessor commandProcessor;

  @Before
  public void init() {
    cache = mock(InternalCache.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    SecurityService securityService = mock(SecurityService.class);
    GfshParser gfshParser = mock(GfshParser.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    Statistics statistics = mock(Statistics.class);

    ServiceLoaderModuleService moduleService =
        new ServiceLoaderModuleService(LogService.getLogger());

    CommandExecutor executor = mock(CommandExecutor.class);
    commandProcessor = new OnlineCommandProcessor(gfshParser,
        securityService, executor);

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(cache.getInternalDistributedSystem()).thenReturn(distributedSystem);
    when(cache.isClosed()).thenReturn(true);
    when(cache.getSecurityService()).thenReturn(securityService);
    when(((StatisticsFactory) distributedSystem).createAtomicStatistics(any(), anyString(),
        anyLong())).thenReturn(statistics);
    when(distributedSystem.getCancelCriterion()).thenReturn(new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return null;
      }

      @Override
      public RuntimeException generateCancelledException(Throwable throwable) {
        return null;
      }
    });

    OnlineCommandProcessor onlineCommandProcessor = new OnlineCommandProcessor();
    onlineCommandProcessor.init(cache, moduleService);

    when(cache.getService(CommandProcessor.class)).thenReturn(onlineCommandProcessor);
    when(distributedSystem.getProperties()).thenReturn(new Properties());
  }

  @Test
  public void processCommandError() throws Exception {
    @SuppressWarnings("deprecation")
    MemberCommandService memberCommandService =
        new MemberCommandService(cache, new ServiceLoaderModuleService(LogService.getLogger()));

    Result result = memberCommandService.processCommand("fake command");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }
}
