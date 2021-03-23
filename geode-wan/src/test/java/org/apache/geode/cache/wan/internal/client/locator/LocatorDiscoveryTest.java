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
package org.apache.geode.cache.wan.internal.client.locator;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinResponse;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingResponse;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;

public class LocatorDiscoveryTest {
  private DistributionLocatorId locatorId;
  private RemoteLocatorJoinRequest request;
  private LocatorDiscovery locatorDiscovery;
  private TcpClient locatorClient;
  private WanLocatorDiscoverer discoverer;
  private LocatorDiscovery.LocalLocatorDiscovery localLocatorDiscovery;
  private LocatorDiscovery.RemoteLocatorDiscovery remoteLocatorDiscovery;

  @Before
  public void setUp() {
    discoverer = mock(WanLocatorDiscoverer.class);
    locatorId = mock(DistributionLocatorId.class);
    request = mock(RemoteLocatorJoinRequest.class);
    LocatorMembershipListener listener = mock(LocatorMembershipListener.class);
    locatorClient = mock(TcpClient.class);
    locatorDiscovery =
        Mockito.spy(new LocatorDiscovery(discoverer, locatorId, request, listener, locatorClient));
    localLocatorDiscovery = locatorDiscovery.new LocalLocatorDiscovery();
    remoteLocatorDiscovery = locatorDiscovery.new RemoteLocatorDiscovery();
  }

  @Test
  public void skipFailureLoggingReturnsCorrectly() {
    // First call should always be false
    assertFalse(locatorDiscovery.skipFailureLogging(locatorId));
    long firstReturnedFalse = System.currentTimeMillis();

    // Next calls should only be false if more than 1000ms has passed since the last call that
    // returned false
    assertTrue(locatorDiscovery.skipFailureLogging(locatorId));
    assertTrue(locatorDiscovery.skipFailureLogging(locatorId));
    await().until(() -> System.currentTimeMillis() - firstReturnedFalse > 1000);
    assertFalse(locatorDiscovery.skipFailureLogging(locatorId));
    long secondReturnedFalse = System.currentTimeMillis();

    // Next calls should only be false if more than 2000ms has passed since the last call that
    // returned false
    assertTrue(locatorDiscovery.skipFailureLogging(locatorId));
    await().until(() -> System.currentTimeMillis() - secondReturnedFalse > 2000);
    assertFalse(locatorDiscovery.skipFailureLogging(locatorId));
  }

  @Test
  public void localLocatorDiscoveryDoesNothingWhenDiscovererIsStopped() {
    when(discoverer.isStopped()).thenReturn(true);
    localLocatorDiscovery.run();
    verifyNoInteractions(locatorClient);
  }

  @Test
  public void localLocatorDiscoveryStopsWithNonNullResponse()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    RemoteLocatorJoinResponse response = mock(RemoteLocatorJoinResponse.class);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(response);
    doNothing().when(locatorDiscovery).addExchangedLocators(response);

    localLocatorDiscovery.run();

    // Confirm that we stopped after the first response was received
    verify(locatorDiscovery, times(1)).addExchangedLocators(response);
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
  }

  @Test
  public void localLocatorDiscoveryRetriesWithNullResponse()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(null);

    localLocatorDiscovery.run();

    // Confirm that we retried each time the response was null
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(2)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
  }

  @Test
  public void localLocatorRetriesWhenIOException() throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenThrow(new IOException());

    localLocatorDiscovery.run();

    // Confirm that we retried after the first exception was thrown
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(2)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
  }

  @Test
  public void localLocatorDoesNotRetryWhenClassNotFoundException()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenThrow(new ClassNotFoundException());

    localLocatorDiscovery.run();

    // Confirm that we did not retry after the first exception
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
  }

  @Test
  public void localLocatorDoesNotRetryWhenClassCastException()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenThrow(new ClassCastException());

    localLocatorDiscovery.run();

    // Confirm that we did not retry after the first exception
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDiscoveryDoesNothingWhenDiscovererIsStopped() {
    when(discoverer.isStopped()).thenReturn(true);
    remoteLocatorDiscovery.run();
    verifyNoInteractions(locatorClient);
  }

  @Test
  public void remoteLocatorDiscoveryPingsRemoteWhenJoinResponseIsNotNull()
      throws IOException, ClassNotFoundException {
    // Only allow one attempt before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(true);
    RemoteLocatorJoinResponse joinResponse = mock(RemoteLocatorJoinResponse.class);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(joinResponse);
    doNothing().when(locatorDiscovery).addExchangedLocators(joinResponse);

    // Return null to prevent the ping loop continuing forever
    when(locatorClient.requestToServer(any(), any(RemoteLocatorPingRequest.class), anyInt(),
        anyBoolean())).thenReturn(null);

    remoteLocatorDiscovery.run();

    // Confirm that we sent a ping request after the first joinResponse was received
    verify(locatorDiscovery, times(1)).addExchangedLocators(joinResponse);
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(1)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDiscoveryRetriesPingRemoteWhenJoinResponseIsNotNullAndPingResponseIsNotNull()
      throws IOException, ClassNotFoundException {
    // Only allow one attempt before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(true);
    RemoteLocatorJoinResponse joinResponse = mock(RemoteLocatorJoinResponse.class);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(joinResponse);
    doNothing().when(locatorDiscovery).addExchangedLocators(joinResponse);

    RemoteLocatorPingResponse pingResponse = mock(RemoteLocatorPingResponse.class);
    // Return a non-null RemoteLocatorPingResponse, then return null to prevent the ping loop
    // continuing forever
    when(locatorClient.requestToServer(any(), any(RemoteLocatorPingRequest.class), anyInt(),
        anyBoolean())).thenReturn(pingResponse).thenReturn(null);

    remoteLocatorDiscovery.run();

    // Confirm that we retried pinging the remote locator
    verify(locatorDiscovery, times(1)).addExchangedLocators(joinResponse);
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(2)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDiscoveryRetriesWithNullJoinResponse()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(null);

    remoteLocatorDiscovery.run();

    // Confirm that we retried each time the response was null
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(2)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(0)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorRetriesWhenIOExceptionWhenSendingRemoteLocatorJoinRequest()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenThrow(new IOException());

    remoteLocatorDiscovery.run();

    // Confirm that we retried after the first exception was thrown
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(2)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorRetriesWhenIOExceptionWhenSendingRemoteLocatorPingRequest()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    RemoteLocatorJoinResponse joinResponse = mock(RemoteLocatorJoinResponse.class);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(joinResponse);
    doNothing().when(locatorDiscovery).addExchangedLocators(joinResponse);

    when(locatorClient.requestToServer(any(), any(RemoteLocatorPingRequest.class), anyInt(),
        anyBoolean())).thenThrow(new IOException());

    remoteLocatorDiscovery.run();

    // Confirm that we retried after the first exception was thrown
    verify(locatorDiscovery, times(2)).addExchangedLocators(any());
    verify(locatorClient, times(2)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(2)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDoesNotRetryWhenClassNotFoundExceptionFromJoinRequest()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenThrow(new ClassNotFoundException());

    remoteLocatorDiscovery.run();

    // Confirm that we did not retry after the first exception
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(0)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDoesNotRetryWhenClassNotFoundExceptionFromPingRequest()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    RemoteLocatorJoinResponse joinResponse = mock(RemoteLocatorJoinResponse.class);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(joinResponse);
    doNothing().when(locatorDiscovery).addExchangedLocators(joinResponse);

    when(locatorClient.requestToServer(any(), any(RemoteLocatorPingRequest.class), anyInt(),
        anyBoolean())).thenThrow(new ClassNotFoundException());

    remoteLocatorDiscovery.run();

    // Confirm that we did not retry after the first exception
    verify(locatorDiscovery, times(1)).addExchangedLocators(any());
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(1)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDoesNotRetryWhenClassCastExceptionFromJoinRequest()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenThrow(new ClassCastException());

    remoteLocatorDiscovery.run();

    // Confirm that we did not retry after the first exception
    verify(locatorDiscovery, times(0)).addExchangedLocators(any());
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(0)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }

  @Test
  public void remoteLocatorDoesNotRetryWhenClassCastExceptionFromPingRequest()
      throws IOException, ClassNotFoundException {
    // Only allow two retries before stopping the locator discoverer
    when(discoverer.isStopped()).thenReturn(false).thenReturn(false).thenReturn(true);
    RemoteLocatorJoinResponse joinResponse = mock(RemoteLocatorJoinResponse.class);
    when(locatorClient.requestToServer(any(), eq(request), anyInt(), anyBoolean()))
        .thenReturn(joinResponse);
    doNothing().when(locatorDiscovery).addExchangedLocators(joinResponse);

    when(locatorClient.requestToServer(any(), any(RemoteLocatorPingRequest.class), anyInt(),
        anyBoolean())).thenThrow(new ClassCastException());

    remoteLocatorDiscovery.run();

    // Confirm that we did not retry after the first exception
    verify(locatorDiscovery, times(1)).addExchangedLocators(any());
    verify(locatorClient, times(1)).requestToServer(any(), eq(request), anyInt(), anyBoolean());
    verify(locatorClient, times(1)).requestToServer(any(), any(RemoteLocatorPingRequest.class),
        anyInt(), anyBoolean());
  }
}
