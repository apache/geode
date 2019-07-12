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

import static org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListenerImpl.WAN_LOCATORS_DISTRIBUTOR_THREAD_NAME;
import static org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListenerImpl.WAN_LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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

  private void joinLocatorsDistributorThread() throws InterruptedException {
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    Optional<Thread> distributorThread = threads.stream()
        .filter(t -> WAN_LOCATORS_DISTRIBUTOR_THREAD_NAME.equals(t.getName()))
        .findFirst();

    if (distributorThread.isPresent()) {
      distributorThread.get().join();
    }
  }

  @Before
  public void setUp() {
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getStartLocator()).thenReturn(DistributionConfig.DEFAULT_START_LOCATOR);
    when(distributionConfig.getMemberTimeout())
        .thenReturn(DistributionConfig.DEFAULT_MEMBER_TIMEOUT);

    tcpClient = spy(mock(TcpClient.class));
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
    DistributionLocatorId distributionLocator1Site1 = buildDistributionLocatorId(10101);
    RemoteLocatorJoinRequest remoteLocator1JoinRequestSite1 =
        new RemoteLocatorJoinRequest(1, distributionLocator1Site1, "");
    DistributionLocatorId distributionLocator1Site2 = buildDistributionLocatorId(20201);
    RemoteLocatorJoinRequest remoteLocator1JoinRequestSite2 =
        new RemoteLocatorJoinRequest(2, distributionLocator1Site2, "");
    DistributionLocatorId distributionLocator2Site2 = buildDistributionLocatorId(20202);
    RemoteLocatorJoinRequest remoteLocator2JoinRequestSite2 =
        new RemoteLocatorJoinRequest(2, distributionLocator2Site2, "");

    // First locator from site 1.
    Object response = locatorMembershipListener.handleRequest(remoteLocator1JoinRequestSite1);
    assertThat(response).isNotNull().isInstanceOf(RemoteLocatorJoinResponse.class);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators()).isNotNull().hasSize(1);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(1)
        .contains(distributionLocator1Site1)).isTrue();

    // Two locators from site 2.
    locatorMembershipListener.handleRequest(remoteLocator1JoinRequestSite2);
    response = locatorMembershipListener.handleRequest(remoteLocator2JoinRequestSite2);
    assertThat(response).isNotNull().isInstanceOf(RemoteLocatorJoinResponse.class);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators()).isNotNull().hasSize(2);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(1).size()).isEqualTo(1);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(1)
        .contains(distributionLocator1Site1)).isTrue();
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(2).size()).isEqualTo(2);
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(2)
        .contains(distributionLocator1Site2)).isTrue();
    assertThat(((RemoteLocatorJoinResponse) response).getLocators().get(2)
        .contains(distributionLocator2Site2)).isTrue();
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
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfoBeforeMessage =
        locatorMembershipListener.getAllLocatorsInfo();
    allLocatorsInfoBeforeMessage
        .forEach((key, value) -> assertThat(value.size()).isEqualTo(locatorsPerCluster));
  }

  @Test
  public void locatorJoinedShouldNotifyEveryKnownLocatorAboutTheJoiningLocator()
      throws IOException, ClassNotFoundException, InterruptedException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(10102);
    DistributionLocatorId distributionLocator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId distributionLocator3Site1 = buildDistributionLocatorId(10103);
    DistributionLocatorId distributionLocator1Site2 = buildDistributionLocatorId(20201);
    DistributionLocatorId distributionLocator2Site2 = buildDistributionLocatorId(20202);
    DistributionLocatorId distributionLocator1Site3 = buildDistributionLocatorId(30301);
    DistributionLocatorId distributionLocator2Site3 = buildDistributionLocatorId(30302);
    DistributionLocatorId distributionLocator3Site3 = buildDistributionLocatorId(30303);
    allLocatorsInfo.put(1,
        new HashSet<>(Arrays.asList(distributionLocator1Site1, distributionLocator3Site1)));
    allLocatorsInfo.put(2,
        new HashSet<>(Arrays.asList(distributionLocator1Site2, distributionLocator2Site2)));
    allLocatorsInfo.put(3, new HashSet<>(Arrays.asList(distributionLocator1Site3,
        distributionLocator2Site3, distributionLocator3Site3)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    locatorMembershipListener.locatorJoined(1, joiningLocator, distributionLocator1Site1);
    joinLocatorsDistributorThread();

    verify(tcpClient).requestToServer(distributionLocator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(1, distributionLocator3Site1, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator1Site2.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(2, distributionLocator1Site2, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator2Site2.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(2, distributionLocator2Site2, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator1Site3.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(3, distributionLocator1Site3, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator2Site3.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(3, distributionLocator2Site3, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator3Site3.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(3, distributionLocator3Site3, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
  }

  @Test
  public void locatorJoinedShouldRetryOnConnectionFailures()
      throws IOException, ClassNotFoundException, InterruptedException {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<>();
    DistributionLocatorId joiningLocator = buildDistributionLocatorId(10102);
    DistributionLocatorId distributionLocator1Site1 = buildDistributionLocatorId(10101);
    DistributionLocatorId distributionLocator3Site1 = buildDistributionLocatorId(10103);
    DistributionLocatorId distributionLocator1Site2 = buildDistributionLocatorId(20201);
    DistributionLocatorId distributionLocator1Site3 = buildDistributionLocatorId(30301);
    allLocatorsInfo.put(1,
        new HashSet<>(Arrays.asList(distributionLocator1Site1, distributionLocator3Site1)));
    allLocatorsInfo.put(2, new HashSet<>(Collections.singletonList(distributionLocator1Site2)));
    allLocatorsInfo.put(3, new HashSet<>(Collections.singletonList(distributionLocator1Site3)));
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    // Fail on first 2 attempts and succeed on third attempt.
    when(tcpClient.requestToServer(distributionLocator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false))
            .thenThrow(new EOFException("Mock Exception"))
            .thenThrow(new EOFException("Mock Exception")).thenReturn(null);
    when(tcpClient.requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(3, distributionLocator1Site3, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false))
            .thenThrow(new EOFException("Mock Exception"));

    locatorMembershipListener.locatorJoined(1, joiningLocator, distributionLocator1Site1);
    joinLocatorsDistributorThread();

    verify(tcpClient, times(3)).requestToServer(distributionLocator3Site1.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(1, distributionLocator3Site1, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator1Site2.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(joiningLocator.getHost(),
        new LocatorJoinMessage(2, distributionLocator1Site2, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient).requestToServer(distributionLocator1Site3.getHost(),
        new LocatorJoinMessage(1, joiningLocator, distributionLocator1Site1, ""),
        DistributionConfig.DEFAULT_MEMBER_TIMEOUT, false);
    verify(tcpClient, times(WAN_LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS + 1)).requestToServer(
        joiningLocator.getHost(),
        new LocatorJoinMessage(3, distributionLocator1Site3, distributionLocator1Site1, ""),
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
