/*
 *
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
 *
 */

package org.apache.geode.redis.internal.commands.executor.connection;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_USERNAME_OR_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.security.AuthenticationExpiredException;

public class AuthExecutorTest {
  private AuthExecutor executor;
  private Command command;
  private ExecutionHandlerContext context;

  @Before
  public void before() {
    executor = spy(AuthExecutor.class);
    command = mock(Command.class);
    context = mock(ExecutionHandlerContext.class);
  }

  @Test
  public void notIntegratedService() {
    when(context.isSecurityEnabled()).thenReturn(false);
    RedisResponse redisResponse = executor.executeCommand(command, context);
    assertThat(redisResponse.toString()).contains(ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED);
  }

  @Test
  public void handleAuthExpirationException() {
    when(context.isSecurityEnabled()).thenReturn(true);
    doReturn(new Properties()).when(executor).getSecurityProperties(command, context);
    when(context.login(any())).thenThrow(new AuthenticationExpiredException("expired"));
    RedisResponse redisResponse = executor.executeCommand(command, context);
    assertThat(redisResponse.toString()).contains(ERROR_INVALID_USERNAME_OR_PASSWORD);
  }
}
