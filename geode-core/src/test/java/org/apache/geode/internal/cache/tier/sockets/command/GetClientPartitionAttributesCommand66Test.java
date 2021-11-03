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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class GetClientPartitionAttributesCommand66Test {

  private static final String REGION_NAME = "region1";

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private InternalCache cache;
  @Mock
  private Part regionNamePart;
  @Mock
  private Message responseMessage;

  @InjectMocks
  private GetClientPartitionAttributesCommand66 getClientPartitionAttributesCommand66;

  @Before
  public void setUp() throws Exception {
    this.getClientPartitionAttributesCommand66 = new GetClientPartitionAttributesCommand66();
    MockitoAnnotations.initMocks(this);

    when(this.cache.getRegion(isA(String.class))).thenReturn(mock(LocalRegion.class));

    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);

    when(this.regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getResponseMessage()).thenReturn(this.responseMessage);
  }

  @Test
  public void noSecuirtyShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.getClientPartitionAttributesCommand66.cmdExecute(this.message, this.serverConnection,
        this.securityService, 0);

    verify(this.responseMessage).send();
  }

}
