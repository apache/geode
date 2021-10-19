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

package org.apache.geode.redis.internal.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import io.netty.channel.ChannelId;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission;

public class RedisSecurityServiceTest {

  private RedisSecurityService redisSecurityService;
  private SecurityService geodeSecurityService;

  @Before
  public void setup() {
    geodeSecurityService = mock(SecurityService.class);
    redisSecurityService = new RedisSecurityService(geodeSecurityService);
  }

  @Test
  public void serviceDelegatesToLogin_andCanAuthenticateCorrectly() {
    ChannelId channelId = mock(ChannelId.class);
    when(channelId.asShortText()).thenReturn("channelId");
    Properties properties = mock(Properties.class);
    Subject subject = mock(Subject.class);
    when(geodeSecurityService.login(any())).thenReturn(subject);

    assertThat(redisSecurityService.login(channelId, properties)).isEqualTo(subject);
    assertThat(redisSecurityService.isAuthenticated(channelId)).isTrue();
  }

  @Test
  public void serviceDelegatesToLogout() {
    ChannelId channelId = mock(ChannelId.class);
    when(channelId.asShortText()).thenReturn("channelId");
    Properties properties = mock(Properties.class);
    Subject subject = mock(Subject.class);
    when(geodeSecurityService.login(any())).thenReturn(subject);

    assertThat(redisSecurityService.login(channelId, properties)).isEqualTo(subject);

    redisSecurityService.logout(channelId);

    assertThat(redisSecurityService.isAuthenticated(channelId)).isFalse();
  }

  @Test
  public void serviceDelegatesToAuthorize() {
    ChannelId channelId = mock(ChannelId.class);
    when(channelId.asShortText()).thenReturn("channelId");
    Properties properties = mock(Properties.class);
    Subject subject = mock(Subject.class);
    when(geodeSecurityService.login(any())).thenReturn(subject);

    assertThat(redisSecurityService.login(channelId, properties)).isEqualTo(subject);

    redisSecurityService.authorize(mock(ResourcePermission.class), subject);

    ArgumentCaptor<Subject> argumentCaptor = ArgumentCaptor.forClass(Subject.class);
    verify(geodeSecurityService)
        .authorize(any(ResourcePermission.class), argumentCaptor.capture());
    assertThat(argumentCaptor.getValue()).isEqualTo(subject);
  }

}
