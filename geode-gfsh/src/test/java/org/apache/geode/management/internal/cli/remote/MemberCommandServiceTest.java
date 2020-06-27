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

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.internal.CommandProcessor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;

public class MemberCommandServiceTest {
  private InternalCache cache;
  private CommandProcessor commandProcessor;

  @Before
  public void init() {
    cache = mock(InternalCache.class);
    SecurityService securityService = mock(SecurityService.class);
    GfshParser gfshParser = mock(GfshParser.class);
    CommandExecutor executor = mock(CommandExecutor.class);
    commandProcessor = new OnlineCommandProcessor(gfshParser,
        securityService, executor);
  }

  @Test
  public void processCommandError() throws Exception {
    @SuppressWarnings("deprecation")
    MemberCommandService memberCommandService = new MemberCommandService(cache, commandProcessor);

    Result result = memberCommandService.processCommand("fake command");

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }
}
