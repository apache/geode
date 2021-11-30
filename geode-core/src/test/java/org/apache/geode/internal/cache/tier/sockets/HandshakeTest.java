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
 *
 */

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class HandshakeTest {

  private SecurityService securityService;

  @Before
  public void before() throws Exception {
    securityService = mock(SecurityService.class);
  }

  @Test
  public void authRequiredHasCredentials() throws Exception {
    Handshake.throwIfMissingRequiredCredentials(true, true);
  }

  @Test
  public void authRequiredHasNoCredentials() throws Exception {
    assertThatThrownBy(() -> Handshake.throwIfMissingRequiredCredentials(true, false))
        .isExactlyInstanceOf(AuthenticationRequiredException.class)
        .hasMessage("No security credentials are provided");
  }

  @Test
  public void authNotRequiredHasCredentials() throws Exception {
    Handshake.throwIfMissingRequiredCredentials(false, true);
  }

  @Test
  public void authNotRequiredHasNoCredentials() throws Exception {
    Handshake.throwIfMissingRequiredCredentials(false, false);
  }

  @Test
  public void verifyCredentialsShouldThrowCacheClosedException() {
    Properties properties = new Properties();
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new CacheClosedException()).when(securityService).login(properties);
    assertThatThrownBy(
        () -> Handshake.authenticate(null, properties, null, null, null, null, securityService))
            .isInstanceOf(CacheClosedException.class);
  }
}
