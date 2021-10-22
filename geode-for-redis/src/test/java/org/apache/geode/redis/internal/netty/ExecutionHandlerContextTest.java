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
package org.apache.geode.redis.internal.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.shiro.subject.Subject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.services.RedisSecurityService;
import org.apache.geode.redis.internal.statistics.RedisStats;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class ExecutionHandlerContextTest {
  private ExecutionHandlerContext createContext(RedisSecurityService securityService) {
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    RedisStats redisStats = mock(RedisStats.class);
    return new ExecutionHandlerContext(channel, null, null, null, redisStats, null, 0, null,
        securityService);
  }

  @Test
  public void isAuthorized_withReadOp_checksForWritePermission() {
    RedisSecurityService securityService = mock(RedisSecurityService.class);
    ExecutionHandlerContext context = createContext(securityService);
    context.setSubject(mock(Subject.class));

    boolean result = context.isAuthorized(RedisCommandType.GET);

    assertThat(result).isTrue();
    ArgumentCaptor<ResourcePermission> argumentCaptor =
        ArgumentCaptor.forClass(ResourcePermission.class);
    verify(securityService, times(1)).authorize(argumentCaptor.capture(), any());
    assertThat(argumentCaptor.getValue().getOperationString()).isEqualTo("READ");
    assertThat(argumentCaptor.getValue().getResourceString()).isEqualTo("DATA");
    assertThat(argumentCaptor.getValue().getTarget()).isEqualTo(RegionProvider.REDIS_DATA_REGION);
  }

  @Test
  public void isAuthorized_withWriteOp_checksForWritePermission() {
    RedisSecurityService securityService = mock(RedisSecurityService.class);
    ExecutionHandlerContext context = createContext(securityService);
    context.setSubject(mock(Subject.class));

    boolean result = context.isAuthorized(RedisCommandType.SET);

    assertThat(result).isTrue();
    ArgumentCaptor<ResourcePermission> argumentCaptor =
        ArgumentCaptor.forClass(ResourcePermission.class);
    verify(securityService, times(1)).authorize(argumentCaptor.capture(), any());
    assertThat(argumentCaptor.getValue().getOperationString()).isEqualTo("WRITE");
    assertThat(argumentCaptor.getValue().getResourceString()).isEqualTo("DATA");
    assertThat(argumentCaptor.getValue().getTarget()).isEqualTo(RegionProvider.REDIS_DATA_REGION);
  }

  @Test
  public void isAuthorized_withNoSubject_doesNoCheckAndReturnsTrue() {
    RedisSecurityService securityService = mock(RedisSecurityService.class);
    ExecutionHandlerContext context = createContext(securityService);
    context.setSubject(null);

    boolean result = context.isAuthorized(RedisCommandType.SET);

    assertThat(result).isTrue();
    verify(securityService, never()).authorize(any(), any());
  }

  @Test
  public void isAuthorized_withFailedCheckReturnsFalse() {
    RedisSecurityService securityService = mock(RedisSecurityService.class);
    doThrow(NotAuthorizedException.class).when(securityService).authorize(any(), any());
    ExecutionHandlerContext context = createContext(securityService);
    context.setSubject(mock(Subject.class));

    boolean result = context.isAuthorized(RedisCommandType.SET);

    assertThat(result).isFalse();
  }
}
