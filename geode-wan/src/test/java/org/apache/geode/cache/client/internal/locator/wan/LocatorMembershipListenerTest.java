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
package org.apache.geode.cache.client.internal.locator.wan;

import static org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListenerImpl.LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.test.junit.ResultCaptor;

public class LocatorMembershipListenerTest {
  private TcpClient tcpClient;
  private LocatorMembershipListenerImpl locatorMembershipListener;

  private DistributionLocatorId buildDistributionLocatorId(int port) {
    return new DistributionLocatorId("localhost[" + port + "]");
  }

  private List<LocatorJoinMessage> buildPermutationsForClusterId(int dsId, int locatorsAmount) {
    int basePort = dsId * 10000;
    List<LocatorJoinMessage> joinMessages = new ArrayList<>();

    for (int i = 1; i <= locatorsAmount; i++) {
      DistributionLocatorId sourceLocatorId = buildDistributionLocatorId(basePort + i);

      for (int j = 1; j <= locatorsAmount; j++) {
        DistributionLocatorId distributionLocatorId = buildDistributionLocatorId(basePort + j);
        LocatorJoinMessage locatorJoinMessage =
            new LocatorJoinMessage(dsId, distributionLocatorId, sourceLocatorId, "");
        joinMessages.add(locatorJoinMessage);
      }
    }

    return joinMessages;
  }

  private void verifyMessagesSentBothWays(DistributionLocatorId sourceLocator,
      int advertisedLocatorDsId, DistributionLocatorId advertisedLocator,
      int initialTargetLocatorDsId, DistributionLocatorId initialTargetLocator)
      throws IOException, ClassNotFoundException {
    verify(tcpClient).requestToServer(initialTargetLocator.getHost(),
        new LocatorJoinMessage(advertisedLocatorDsId, advertisedLocator, sourceLocator, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(advertisedLocator.getHost(),
        new LocatorJoinMessage(initialTargetLocatorDsId, initialTargetLocator, sourceLocator, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
  }

  private void joinLocatorsDistributorThread(ResultCaptor<Thread> resultCaptor) {
    Thread distributorThread = resultCaptor.getResult();
    await().untilAsserted(
        () -> assertThat(distributorThread.getState()).isEqualTo(Thread.State.TERMINATED));
  }

  @Before
  public void setUp() {
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getStartLocator()).thenReturn(DistributionConfig.DEFAULT_START_LOCATOR);
    when(distributionConfig.getMemberTimeout())
        .thenReturn(DistributionConfig.DEFAULT_MEMBER_TIMEOUT);

    tcpClient = mock(TcpClient.class);
    locatorMembershipListener = spy(new LocatorMembershipListenerImpl(tcpClient));
    locatorMembershipListener.setConfig(distributionConfig);
  }

  @Test
  public void handleRemoteLocatorPingRequestShouldReturnCorrectResponseWithoutUpdatingInternalStructures() {
    RemoteLocatorPingRequest remoteLocatorPingRequest = new RemoteLocatorPingRequest();
    Object response = locatorMembershipListener.handleRequest(remoteLocatorPingRequest);

    assertThat(response).isNotNull().isInstanceOf(RemoteLocatorPingResponse.class);
    assertThat(locatorMembershipListener.getAllLocatorsInfo()).isEmpty();
    assertThat(locatorMembershipListener.getAllServerLocatorsInfo()).isEmpty();
  }

  @Test
  public void handleRemoteLocatorRequestShouldReturnListOfKnownRemoteLocatorsForTheRequestedDsIdWithoutUpdatingInternalStructures() {
    RemoteLocatorRequest remoteLocatorRequest = new RemoteLocatorRequest(1, "");
    Set<String> cluster1Locators =
        new HashSet<>(Arrays.asList("localhost[10101]", "localhost[10102]"));
    when(locatorMembershipListener.getRemoteLocatorInfo(1)).thenReturn(cluster1Locators);

    Object response = locatorMembershipListener.handleRequest(remoteLocatorRequest);
    assertThat(response).isNotNull().isInstanceOf(RemoteLocatorResponse.class);
    assertThat(locatorMembershipListener.getAllLocatorsInfo()).isEmpty();
    assertThat(locatorMembershipListener.getAllServerLocatorsInfo()).isEmpty();
    assertThat(((RemoteLocatorResponse) response).getLocators()).isEqualTo(cluster1Locators);
  }

  @Test
  public void handleRemoteLocatorJoinRequestShouldReturnAllKnownLocatorsAndUpdateInternalStructures() {
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    RemoteLocatorJoinRequest locator1Site1JoinRequest =
        new RemoteLocatorJoinRequest(1, locator1Site1, "");
    DistributionLocatorId locator1Site2 = buildDistributionLocatorId(20201);
    RemoteLocatorJoinRequest locator1Site2JoinRequest =
        new RemoteLocatorJoinRequest(2, locator1Site2, "");
    DistributionLocatorId locator2Site2 = buildDistributionLocatorId(20202);
    RemoteLocatorJoinRequest locator2Site2JoinRequest =
        new RemoteLocatorJoinRequest(2, locator2Site2, "");

    // First locator from site 1.
    Object response = locatorMembershipListener.handleRequest(locator1Site1JoinRequest);
    assertThat(response).isNotNull().isInstanceOf(RemoteLocatorJoinResponse.class);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators()).isNotNull().hasSize(1);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(1).contains(locator1Site1))
        .isTrue();

    // Two locators from site 2.
    locatorMembershipListener.handleRequest(locator1Site2JoinRequest);
    response = locatorMembershipListener.handleRequest(locator2Site2JoinRequest);
    assertThat(response).isNotNull().isInstanceOf(RemoteLocatorJoinResponse.class);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators()).isNotNull().hasSize(2);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(1).size()).isEqualTo(1);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(1).contains(locator1Site1))
        .isTrue();
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(2).size()).isEqualTo(2);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(2).contains(locator1Site2))
        .isTrue();
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(2).contains(locator2Site2))
        .isTrue();
  }

  @Test
  public void handleLocatorJoinMessageShouldUpdateInternalStructures()
      throws InterruptedException, ExecutionException {
    int clusters = 4;
    int locatorsPerCluster = 6;
    List<LocatorJoinMessage> allJoinMessages = new ArrayList<>();

    for (int i = 1; i <= clusters; i++) {
      allJoinMessages.addAll(buildPermutationsForClusterId(i, locatorsPerCluster));
    }

    Collection<Callable<Object>> requests = new ArrayList<>();
    allJoinMessages.forEach(
        (request) -> requests.add(new HandlerCallable(locatorMembershipListener, request)));

    ExecutorService executorService = Executors.newFixedThreadPool(allJoinMessages.size());
    List<Future<Object>> futures = executorService.invokeAll(requests);
    for (Future future : futures) {
      Object response = future.get();
      assertThat(response).isNull();
    }
    executorService.shutdownNow();

    assertThat(locatorMembershipListener.getAllLocatorsInfo().size()).isEqualTo(clusters);
    ConcurrentMap<Integer, Set<DistributionLocatorId>> locatorsPerClusterMap =
        locatorMembershipListener.getAllLocatorsInfo();
    locatorsPerClusterMap
        .forEach((key, value) -> assertThat(value.size()).isEqualTo(locatorsPerCluster));
  }

  @Test
  public void locatorJoinedShouldNotifyNobodyIfThereAreNoKnownLocators()
      throws IOException, ClassNotFoundException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(20202);
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    ResultCaptor<Thread> resultCaptor = new ResultCaptor<>();
    doAnswer(resultCaptor).when(locatorMembershipListener).buildLocatorsDistributorThread(
        any(DistributionLocatorId.class), anyMap(), any(DistributionLocatorId.class), anyInt());
    locatorMembershipListener.locatorJoined(2, joiningLocator, locator1Site1);
    joinLocatorsDistributorThread(resultCaptor);
    verify(tcpClient, times(0)).requestToServer(any(InetSocketAddress.class),
        any(LocatorJoinMessage.class), anyInt(), anyBoolean());
  }

  @Test
  public void locatorJoinedShouldNotifyKnownLocatorAboutTheJoiningLocatorAndJoiningLocatorAboutTheKnownOne()
      throws IOException, ClassNotFoundException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(20202);
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId locator3Site3 = buildDistributionLocatorId(30303);
    allLocatorsInfo.put(3, new HashSet<>(Collections.singletonList(locator3Site3)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    ResultCaptor<Thread> resultCaptor = new ResultCaptor<>();
    doAnswer(resultCaptor).when(locatorMembershipListener).buildLocatorsDistributorThread(
        any(DistributionLocatorId.class), anyMap(), any(DistributionLocatorId.class), anyInt());
    locatorMembershipListener.locatorJoined(2, joiningLocator, locator1Site1);
    joinLocatorsDistributorThread(resultCaptor);
    verifyMessagesSentBothWays(locator1Site1, 2, joiningLocator, 3, locator3Site3);
  }

  @Test
  public void locatorJoinedShouldNotifyEveryKnownLocatorAboutTheJoiningLocatorAndJoiningLocatorAboutAllTheKnownLocators()
      throws IOException, ClassNotFoundException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(10102);
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId locator3Site1 = buildDistributionLocatorId(10103);
    DistributionLocatorId locator1Site2 = buildDistributionLocatorId(20201);
    DistributionLocatorId locator2Site2 = buildDistributionLocatorId(20202);
    DistributionLocatorId locator1Site3 = buildDistributionLocatorId(30301);
    DistributionLocatorId locator2Site3 = buildDistributionLocatorId(30302);
    DistributionLocatorId locator3Site3 = buildDistributionLocatorId(30303);
    allLocatorsInfo.put(1, new HashSet<>(Arrays.asList(locator1Site1, locator3Site1)));
    allLocatorsInfo.put(2, new HashSet<>(Arrays.asList(locator1Site2, locator2Site2)));
    allLocatorsInfo.put(3,
        new HashSet<>(Arrays.asList(locator1Site3, locator2Site3, locator3Site3)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    ResultCaptor<Thread> resultCaptor = new ResultCaptor<>();
    doAnswer(resultCaptor).when(locatorMembershipListener).buildLocatorsDistributorThread(
        any(DistributionLocatorId.class), anyMap(), any(DistributionLocatorId.class), anyInt());
    locatorMembershipListener.locatorJoined(1, joiningLocator, locator1Site1);
    joinLocatorsDistributorThread(resultCaptor);
    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 1, locator3Site1);
    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 2, locator1Site2);
    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 2, locator2Site2);
    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 3, locator1Site3);
    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 3, locator2Site3);
    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 3, locator3Site3);
  }

  @Test
  public void locatorJoinedShouldRetryUpToTheConfiguredUpperBoundOnConnectionFailures()
      throws IOException, ClassNotFoundException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(10102);
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId locator3Site1 = buildDistributionLocatorId(10103);
    allLocatorsInfo.put(1, new HashSet<>(Collections.singletonList(locator3Site1)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);
    when(tcpClient.requestToServer(locator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false))
            .thenThrow(new EOFException("Mock Exception"));

    ResultCaptor<Thread> resultCaptor = new ResultCaptor<>();
    doAnswer(resultCaptor).when(locatorMembershipListener).buildLocatorsDistributorThread(
        any(DistributionLocatorId.class), anyMap(), any(DistributionLocatorId.class), anyInt());
    locatorMembershipListener.locatorJoined(1, joiningLocator, locator1Site1);
    joinLocatorsDistributorThread(resultCaptor);

    verify(tcpClient, times(LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS + 1)).requestToServer(
        locator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(1, locator3Site1, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
  }

  @Test
  public void locatorJoinedShouldNotRetryAgainAfterSuccessfulRetryOnConnectionFailures()
      throws IOException, ClassNotFoundException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(10102);
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId locator3Site1 = buildDistributionLocatorId(10103);
    allLocatorsInfo.put(1, new HashSet<>(Collections.singletonList(locator3Site1)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);
    when(tcpClient.requestToServer(locator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false))
            .thenThrow(new EOFException("Mock Exception"))
            .thenReturn(null);

    ResultCaptor<Thread> resultCaptor = new ResultCaptor<>();
    doAnswer(resultCaptor).when(locatorMembershipListener).buildLocatorsDistributorThread(
        any(DistributionLocatorId.class), anyMap(), any(DistributionLocatorId.class), anyInt());
    locatorMembershipListener.locatorJoined(1, joiningLocator, locator1Site1);
    joinLocatorsDistributorThread(resultCaptor);

    verify(tcpClient, times(2)).requestToServer(locator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(1, locator3Site1, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
  }

  @Test
  public void locatorJoinedShouldRetryOnlyFailedMessagesOnConnectionFailures()
      throws IOException, ClassNotFoundException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(10102);
    DistributionLocatorId locator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId locator3Site1 = buildDistributionLocatorId(10103);
    DistributionLocatorId locator1Site2 = buildDistributionLocatorId(20201);
    DistributionLocatorId locator1Site3 = buildDistributionLocatorId(30301);
    allLocatorsInfo.put(1, new HashSet<>(Arrays.asList(locator1Site1, locator3Site1)));
    allLocatorsInfo.put(2, new HashSet<>(Collections.singletonList(locator1Site2)));
    allLocatorsInfo.put(3, new HashSet<>(Collections.singletonList(locator1Site3)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    // Fail on first 2 attempts and succeed on third attempt.
    when(tcpClient.requestToServer(locator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false))
            .thenThrow(new EOFException("Mock Exception"))
            .thenThrow(new EOFException("Mock Exception")).thenReturn(null);

    // Fail always.
    when(tcpClient.requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(3, locator1Site3, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false))
            .thenThrow(new EOFException("Mock Exception"));

    ResultCaptor<Thread> resultCaptor = new ResultCaptor<>();
    doAnswer(resultCaptor).when(locatorMembershipListener).buildLocatorsDistributorThread(
        any(DistributionLocatorId.class), anyMap(), any(DistributionLocatorId.class), anyInt());
    locatorMembershipListener.locatorJoined(1, joiningLocator, locator1Site1);
    joinLocatorsDistributorThread(resultCaptor);

    verifyMessagesSentBothWays(locator1Site1, 1, joiningLocator, 2, locator1Site2);
    verify(tcpClient, times(3)).requestToServer(locator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(1, locator3Site1, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(locator1Site3.getHost(),
        new LocatorJoinMessage(1, joiningLocator, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient, times(LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS + 1)).requestToServer(
        joiningLocator.getHost(),
        new LocatorJoinMessage(3, locator1Site3, locator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
  }

  private static class HandlerCallable implements Callable<Object> {
    private final Object request;
    private final LocatorMembershipListenerImpl locatorMembershipListener;

    HandlerCallable(LocatorMembershipListenerImpl locatorMembershipListener, Object request) {
      this.request = request;
      this.locatorMembershipListener = locatorMembershipListener;
    }

    @Override
    public Object call() {
      return locatorMembershipListener.handleRequest(request);
    }
  }
}
