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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.internal.CommandProcessor;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.Result;

public class MemberCommandServiceTest {
  private InternalCache cache;

  @Before
  public void init() {
    cache = mock(InternalCache.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    SecurityService securityService = mock(SecurityService.class);
    Properties cacheProperties = new Properties();

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    when(cache.isClosed()).thenReturn(true);
    when(cache.getSecurityService()).thenReturn(securityService);
    when(cache.getService(CommandProcessor.class)).thenReturn(new OnlineCommandProcessor());
    when(distributedSystem.getProperties()).thenReturn(cacheProperties);
  }

  @Test
  public void processCommandError() throws Exception {
    @SuppressWarnings("deprecation")
    MemberCommandService memberCommandService = new MemberCommandService(cache);

    Result result = memberCommandService.processCommand("fake command");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }
}
