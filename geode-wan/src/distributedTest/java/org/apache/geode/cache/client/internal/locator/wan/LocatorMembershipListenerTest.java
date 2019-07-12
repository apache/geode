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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
  private LocatorMembershipListenerImpl locatorMembershipListener;

  @Before
  public void setUp() {
    TcpClient mockTcpClient = mock(TcpClient.class);
    locatorMembershipListener = new LocatorMembershipListenerImpl(mockTcpClient);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getStartLocator()).thenReturn(DistributionConfig.DEFAULT_START_LOCATOR);

    locatorMembershipListener.setConfig(distributionConfig);
  }

  @Test
  public void remoteLocatorJoinRequestTest() throws InterruptedException, ExecutionException {
    DistributionLocatorId distributionLocator1Site1 = new DistributionLocatorId(10101, "localhost");
    RemoteLocatorJoinRequest remoteLocator1JoinRequestSite1 =
        new RemoteLocatorJoinRequest(1, distributionLocator1Site1, "");
    DistributionLocatorId distributionLocator2Site1 = new DistributionLocatorId(10102, "localhost");
    RemoteLocatorJoinRequest remoteLocator2JoinRequestSite1 =
        new RemoteLocatorJoinRequest(1, distributionLocator2Site1, "");
    DistributionLocatorId distributionLocator3Site1 = new DistributionLocatorId(10103, "localhost");
    RemoteLocatorJoinRequest remoteLocator3JoinRequestSite1 =
        new RemoteLocatorJoinRequest(1, distributionLocator3Site1, "");
    DistributionLocatorId distributionLocator4Site1 = new DistributionLocatorId(10104, "localhost");
    RemoteLocatorJoinRequest remoteLocator4JoinRequestSite1 =
        new RemoteLocatorJoinRequest(1, distributionLocator4Site1, "");

    DistributionLocatorId distributionLocator1Site2 = new DistributionLocatorId(20201, "localhost");
    RemoteLocatorJoinRequest remoteLocator1JoinRequestSite2 =
        new RemoteLocatorJoinRequest(2, distributionLocator1Site2, "");
    DistributionLocatorId distributionLocator2Site2 = new DistributionLocatorId(20202, "localhost");
    RemoteLocatorJoinRequest remoteLocator2JoinRequestSite2 =
        new RemoteLocatorJoinRequest(2, distributionLocator2Site2, "");
    DistributionLocatorId distributionLocator3Site2 = new DistributionLocatorId(20203, "localhost");
    RemoteLocatorJoinRequest remoteLocator3JoinRequestSite2 =
        new RemoteLocatorJoinRequest(2, distributionLocator3Site2, "");
    DistributionLocatorId distributionLocator4Site2 = new DistributionLocatorId(20204, "localhost");
    RemoteLocatorJoinRequest remoteLocator4JoinRequestSite2 =
        new RemoteLocatorJoinRequest(2, distributionLocator4Site2, "");

    DistributionLocatorId distributionLocator1Site3 = new DistributionLocatorId(30301, "localhost");
    RemoteLocatorJoinRequest remoteLocator1JoinRequestSite3 =
        new RemoteLocatorJoinRequest(3, distributionLocator1Site3, "");
    DistributionLocatorId distributionLocator2Site3 = new DistributionLocatorId(30302, "localhost");
    RemoteLocatorJoinRequest remoteLocator2JoinRequestSite3 =
        new RemoteLocatorJoinRequest(3, distributionLocator2Site3, "");
    DistributionLocatorId distributionLocator3Site3 = new DistributionLocatorId(30303, "localhost");
    RemoteLocatorJoinRequest remoteLocator3JoinRequestSite3 =
        new RemoteLocatorJoinRequest(3, distributionLocator3Site3, "");
    DistributionLocatorId distributionLocator4Site3 = new DistributionLocatorId(30304, "localhost");
    RemoteLocatorJoinRequest remoteLocator4JoinRequestSite3 =
        new RemoteLocatorJoinRequest(3, distributionLocator4Site3, "");

    ExecutorService executorService = Executors.newFixedThreadPool(12);
    Collection<Callable<Object>> requests = Arrays.asList(
        new HandlerCallable(locatorMembershipListener, remoteLocator1JoinRequestSite1),
        new HandlerCallable(locatorMembershipListener, remoteLocator2JoinRequestSite1),
        new HandlerCallable(locatorMembershipListener, remoteLocator3JoinRequestSite1),
        new HandlerCallable(locatorMembershipListener, remoteLocator4JoinRequestSite1),
        new HandlerCallable(locatorMembershipListener, remoteLocator1JoinRequestSite2),
        new HandlerCallable(locatorMembershipListener, remoteLocator2JoinRequestSite2),
        new HandlerCallable(locatorMembershipListener, remoteLocator3JoinRequestSite2),
        new HandlerCallable(locatorMembershipListener, remoteLocator4JoinRequestSite2),
        new HandlerCallable(locatorMembershipListener, remoteLocator1JoinRequestSite3),
        new HandlerCallable(locatorMembershipListener, remoteLocator2JoinRequestSite3),
        new HandlerCallable(locatorMembershipListener, remoteLocator3JoinRequestSite3),
        new HandlerCallable(locatorMembershipListener, remoteLocator4JoinRequestSite3));

    List<Future<Object>> futures = executorService.invokeAll(requests);
    for (Future<Object> future : futures)
      future.get();
    executorService.shutdownNow();

    assertThat(locatorMembershipListener.getAllLocatorsInfo().size()).isEqualTo(3);
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfoBeforeMessage =
        locatorMembershipListener.getAllLocatorsInfo();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : allLocatorsInfoBeforeMessage
        .entrySet()) {
      assertThat(entry.getValue().size()).isEqualTo(4);
    }
  }

  private List<LocatorJoinMessage> buildPermutationsForDsId(int dsId, int locatorsAmount) {
    int basePort = dsId * 10000;
    List<LocatorJoinMessage> joinMessages = new ArrayList<>();

    for (int i = 1; i <= locatorsAmount; i++) {
      DistributionLocatorId sourceLocatorId = new DistributionLocatorId(basePort + i, "localhost");

      for (int j = 1; j <= locatorsAmount; j++) {
        DistributionLocatorId distributionLocatorId =
            new DistributionLocatorId(basePort + j, "localhost");
        LocatorJoinMessage locatorJoinMessage =
            new LocatorJoinMessage(dsId, distributionLocatorId, sourceLocatorId, "");
        joinMessages.add(locatorJoinMessage);
      }
    }

    return joinMessages;
  }

  @Test
  public void locatorJoinRequestTest() throws InterruptedException, ExecutionException {
    int locatorsPerCluster = 6;
    List<LocatorJoinMessage> allJoinMessages = new ArrayList<>();
    allJoinMessages.addAll(buildPermutationsForDsId(1, locatorsPerCluster));
    allJoinMessages.addAll(buildPermutationsForDsId(2, locatorsPerCluster));
    allJoinMessages.addAll(buildPermutationsForDsId(3, locatorsPerCluster));
    allJoinMessages.addAll(buildPermutationsForDsId(4, locatorsPerCluster));
    Collection<Callable<Object>> requests = new ArrayList<>();
    allJoinMessages.forEach(
        (request) -> requests.add(new HandlerCallable(locatorMembershipListener, request)));

    ExecutorService executorService = Executors.newFixedThreadPool(allJoinMessages.size());
    List<Future<Object>> futures = executorService.invokeAll(requests);
    for (Future<Object> future : futures)
      future.get();
    executorService.shutdownNow();

    assertThat(locatorMembershipListener.getAllLocatorsInfo().size()).isEqualTo(4);
    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfoBeforeMessage =
        locatorMembershipListener.getAllLocatorsInfo();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : allLocatorsInfoBeforeMessage
        .entrySet()) {
      assertThat(entry.getValue().size()).isEqualTo(locatorsPerCluster);
    }
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
