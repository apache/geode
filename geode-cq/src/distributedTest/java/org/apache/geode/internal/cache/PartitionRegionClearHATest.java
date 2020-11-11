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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.test.concurrent.FileBasedCountDownLatch;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PartitionRegionClearHATest implements Serializable {
  public static final String NAME = "testRegion";
  protected static MemberVM locator;
  protected static MemberVM server1, server2;
  protected static ClientVM client1, client2;
  protected static List<MemberVM> servers;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(5);

  private static AtomicBoolean clearEventReceived = new AtomicBoolean(false);
  private FileBasedCountDownLatch latch;

  @BeforeClass
  // setup once for all the tests: start up one locator, two servers and two clients.
  // create the regions on all servers and clients. client1 has registered interests. client2
  // has CQ
  public static void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));
    server2 = cluster.startServerVM(2,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));
    servers = Arrays.asList(server1, server2);

    client1 = cluster.startClientVM(3, cc -> {
      cc.withLocatorConnection(locatorPort).withPoolSubscription(true);
    });

    client2 = cluster.startClientVM(4, cc -> {
      cc.withLocatorConnection(locatorPort).withPoolSubscription(true);
    });

    // set client1 with a registered interest
    client1.invoke(() -> {
      Region<Object, Object> testRegion = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(NAME);
      testRegion.registerInterestForAllKeys();
    });

    // set client2 with a CQ
    client2.invoke(() -> {
      ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(NAME);

      QueryService queryService =
          ClusterStartupRule.getClientCache().getDefaultPool().getQueryService();
      CqAttributesFactory cqaFactory = new CqAttributesFactory();
      cqaFactory.addCqListener(new CqListener() {
        @Override
        public void onEvent(CqEvent aCqEvent) {
          Operation baseOperation = aCqEvent.getBaseOperation();
          if (baseOperation.isClear()) {
            clearEventReceived.set(true);
          }
        }

        @Override
        public void onError(CqEvent aCqEvent) {}
      });

      CqQuery cqQuery =
          queryService.newCq("select * from /" + NAME, cqaFactory.create());
      cqQuery.execute();
    });
  }

  @Before
  // before each test, initialize and set the DistributionMessageObserver and input some data into
  // the region
  public void before() throws Exception {
    latch = new FileBasedCountDownLatch(2);
    MemberVM.invokeInEveryMember(() -> {
      PauseDuringClearDistributionMessageObserver observer =
          new PauseDuringClearDistributionMessageObserver();
      observer.setLatch(latch);
      DistributionMessageObserver.setInstance(observer);
    }, server1, server2);

    client1.invoke(() -> {
      Region<Object, Object> testRegion = getClientRegion();
      for (int i = 0; i < 10; i++) {
        testRegion.put(i, "value" + i);
      }
    });
  }

  @After
  // after each test, reset the boolean variable and clear the DistributionMessageObserver
  public void after() throws Exception {
    clearEventReceived.set(false);
    MemberVM.invokeInEveryMember(() -> {
      DistributionMessageObserver.setInstance(null);
    }, server1, server2);
  }

  @Test
  public void restartServerThatIssuesClear() throws Exception {
    // sets the latch and issue the clear
    server1.invokeAsync(() -> {
      getRegion().clear();
    });

    // only restart the server1 until we have reached the observer code
    await().until(() -> latch.currentValue() == 1);

    // restart server1
    server1.stop(false);
    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));

    latch.countDown();
    verifyCleared();
  }

  @Test
  public void restartServerThatDoesNotIssueClear() throws Exception {
    server1.invokeAsync(() -> {
      getRegion().clear();
    });

    // only restart the server2 until we have reached the observer code
    await().until(() -> latch.currentValue() == 1);

    // restart server2
    server2.stop(false);
    int locatorPort = locator.getPort();
    server2 = cluster.startServerVM(2,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));

    latch.countDown();
    verifyCleared();
  }

  private void verifyCleared() {
    client1.invoke(() -> {
      Region<Object, Object> testRegion = getClientRegion();
      await().untilAsserted(() -> assertThat(testRegion.size()).isEqualTo(0));
    });

    client2.invoke(() -> {
      assertThat(clearEventReceived.get()).isTrue();
      assertThat(getClientRegion().size()).isEqualTo(0);
    });

    MemberVM.invokeInEveryMember(() -> {
      assertThat(getRegion().size()).isEqualTo(0);
    }, server1, server2);
  }

  private static Region<Object, Object> getClientRegion() {
    return ClusterStartupRule.getClientCache().getRegion("/" + NAME);
  }

  private static Region<Object, Object> getRegion() {
    return ClusterStartupRule.getCache().getRegion("/" + NAME);
  }

  private static class PauseDuringClearDistributionMessageObserver
      extends DistributionMessageObserver {
    private FileBasedCountDownLatch latch;

    public void setLatch(FileBasedCountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof PartitionedRegionClearMessage) {
        PartitionedRegionClearMessage clearMessage = (PartitionedRegionClearMessage) message;
        if (clearMessage
            .getOp() == PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR) {
          try {
            // count down to 1 so that we can go ahead and restart the server
            latch.countDown();
            // go ahead until the count is 0 (someone else must call countdown one more time
            latch.await();
          } catch (Exception ex) {
          }
        }
      }
    }
  }

}
