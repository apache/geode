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

package org.apache.geode.internal.cache.tier.sockets.command;

import static org.apache.geode.internal.cache.tier.Command.REQUIRES_RESPONSE;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class PutUserCredentialsTest {
  private PutUserCredentials command;
  private Message message;
  private ServerConnection connection;
  private SecurityService securityService;


  @Before
  public void before() throws Exception {
    command = spy(new PutUserCredentials());
    message = mock(Message.class);
    connection = mock(ServerConnection.class);
    securityService = mock(SecurityService.class);
  }

  @Test
  public void doNothingIfNotSecureMode() throws Exception {
    when(message.isSecureMode()).thenReturn(false);
    command.cmdExecute(message, connection, securityService, 0);
    verify(connection, never()).getUniqueId();
  }

  @Test
  public void doNothingIfInvalidParts() throws Exception {
    when(message.isSecureMode()).thenReturn(true);
    when(message.getNumberOfParts()).thenReturn(2);
    command.cmdExecute(message, connection, securityService, 0);
    verify(connection, never()).getUniqueId();
  }

  @Test
  public void setUniqueIdIfMessageIsValid() throws Exception {
    when(message.isSecureMode()).thenReturn(true);
    when(message.getNumberOfParts()).thenReturn(1);
    when(connection.getUniqueId()).thenReturn(-1L);
    doNothing().when(command).writeResponse(
        eq(null), eq(null), eq(message), eq(false), eq(connection));
    command.cmdExecute(message, connection, securityService, 0);
    verify(connection).getUniqueId();
    verify(connection).setAsTrue(REQUIRES_RESPONSE);
    verify(connection).setCredentials(message, -1l);
    verify(command).writeResponse(eq(null), eq(null), eq(message), eq(false), eq(connection));
  }
}
