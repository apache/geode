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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class GatewayReceiverCommandTest {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final Object VALUE = "value1";

  private static final byte[] REMOVE_ON_EXCEPTION_BYTES = new byte[] {0};
  private static final byte[] POSSIBLE_DUPLICATE_BYTES = new byte[] {1};
  private static final byte[] CALLBACK_ARG_EXIST_BYTES = new byte[] {0};

  @Mock
  private EventID eventId;

  @Mock
  private Message message;
  @Mock
  private SecurityService securityService;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private CachedRegionHelper cachedRegionHelper;
  @Mock
  private GatewayReceiverStats gatewayReceiverStats;
  @Mock
  private InternalCache cache;


  @Mock
  private Part numberOfEventsPart;
  @Mock
  private Part batchIdPart;
  @Mock
  private Part dsidPart;
  @Mock
  private Part removeOnExceptionPart;
  @Mock
  private Part actionTypePart;
  @Mock
  private Part possibleDuplicatePart;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part eventIdPart;
  @Mock
  private Part keyPart;
  @Mock
  private Part valuePart;
  @Mock
  private Part callbackArgExistsPart;
  @Mock
  private Part versionTimeStampPart;

  private GatewayReceiverCommand gatewayReceiverCommand;

  @Before
  public void setUp() throws Exception {
    gatewayReceiverCommand = (GatewayReceiverCommand) GatewayReceiverCommand.getCommand();
    MockitoAnnotations.openMocks(this);

    when(serverConnection.getCachedRegionHelper()).thenReturn(cachedRegionHelper);
    when(serverConnection.getCacheServerStats()).thenReturn(gatewayReceiverStats);
    when(serverConnection.getLatestBatchIdReplied()).thenReturn(0);

    when(cachedRegionHelper.getCacheForGatewayCommand()).thenReturn(cache);

    when(numberOfEventsPart.getInt()).thenReturn(1);
    when(batchIdPart.getInt()).thenReturn(1);
    when(dsidPart.getInt()).thenReturn(1);
    when(removeOnExceptionPart.getSerializedForm()).thenReturn(REMOVE_ON_EXCEPTION_BYTES);

    when(possibleDuplicatePart.getObject()).thenReturn(POSSIBLE_DUPLICATE_BYTES);
    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);
    when(eventIdPart.getObject()).thenReturn(eventId);
    when(keyPart.getStringOrObject()).thenReturn(KEY);
    when(valuePart.getStringOrObject()).thenReturn(VALUE);
    when(callbackArgExistsPart.getObject()).thenReturn(CALLBACK_ARG_EXIST_BYTES);
    when(versionTimeStampPart.getLong()).thenReturn(1l);

    when(message.getNumberOfParts()).thenReturn(12);
    when(message.getPart(eq(0))).thenReturn(numberOfEventsPart);
    when(message.getPart(eq(1))).thenReturn(batchIdPart);
    when(message.getPart(eq(2))).thenReturn(dsidPart);
    when(message.getPart(eq(3))).thenReturn(removeOnExceptionPart);
    when(message.getPart(eq(4))).thenReturn(actionTypePart);

    when(message.getPart(eq(5))).thenReturn(possibleDuplicatePart);
    when(message.getPart(eq(6))).thenReturn(regionNamePart);
    when(message.getPart(eq(7))).thenReturn(eventIdPart);
    when(message.getPart(eq(8))).thenReturn(keyPart);
    when(message.getPart(eq(9))).thenReturn(valuePart);
    when(message.getPart(eq(10))).thenReturn(callbackArgExistsPart);
    when(message.getPart(eq(11))).thenReturn(versionTimeStampPart);

  }

  @Test
  public void cacheClosedAtCreateEvent() throws Exception {
    when(cache.getRegion(any())).thenThrow(CacheClosedException.class);
    when(actionTypePart.getInt()).thenReturn(0);

    gatewayReceiverCommand.cmdExecute(message, serverConnection, securityService, 0);
    verify(serverConnection).setFlagProcessMessagesAsFalse();
  }

  @Test
  public void cacheClosedAtUpdateEvent() throws Exception {
    when(cache.getRegion(any())).thenThrow(CacheClosedException.class);
    when(actionTypePart.getInt()).thenReturn(1);

    gatewayReceiverCommand.cmdExecute(message, serverConnection, securityService, 0);
    verify(serverConnection).setFlagProcessMessagesAsFalse();
  }

  @Test
  public void cacheClosedAtDestroyEvent() throws Exception {

    when(message.getPart(eq(9))).thenReturn(callbackArgExistsPart);
    when(message.getPart(eq(10))).thenReturn(versionTimeStampPart);

    when(cache.getRegion(any())).thenThrow(CacheClosedException.class);
    when(actionTypePart.getInt()).thenReturn(2);

    gatewayReceiverCommand.cmdExecute(message, serverConnection, securityService, 0);
    verify(serverConnection).setFlagProcessMessagesAsFalse();
  }


}
