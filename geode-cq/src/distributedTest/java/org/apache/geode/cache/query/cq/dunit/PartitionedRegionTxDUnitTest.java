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
package org.apache.geode.cache.query.cq.dunit;


import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXState;
import org.apache.geode.internal.cache.TXStateInterface;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.security.query.TestCqListener;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category(ClientSubscriptionTest.class)
@RunWith(Parameterized.class)
public class PartitionedRegionTxDUnitTest implements Serializable {

  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Parameter
  public String conserveSockets;

  @Parameters
  public static Collection<String> data() {
    return Arrays.asList("true", "false");
  }

  @After
  public void clearObserver() {
    for (VM vm : VM.toArray(VM.getAllVMs(), VM.getController())) {
      vm.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
      });
    }
  }

  @Test
  public void verifyNoLockContentionBetweenCqRegistrationAndTxCommit() throws Exception {
    blackboard.setMailbox("CqQueryResultCount", 0);
    blackboard.setMailbox("CqEvents", 0);

    String REGION_NAME = "region";
    Properties properties = new Properties();
    properties.put("conserve-sockets", conserveSockets);
    MemberVM locator = clusterStartupRule.startLocatorVM(0, new Properties());
    MemberVM server1 = clusterStartupRule.startServerVM(1, properties, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, properties, locator.getPort());
    ClientVM client = clusterStartupRule.startClientVM(3,
        cacheRule -> cacheRule.withServerConnection(server2.getPort()).withPoolSubscription(true));

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region region = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(1).setTotalNumBuckets(1).create())
          .create(REGION_NAME);

      // Force primary bucket to get created.
      region.put("Key-1", "value-1");
      region.destroy("Key-1");
    });

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(1).setTotalNumBuckets(1).create())
          .create(REGION_NAME);
    });

    AsyncInvocation<Void> serverAsync = server1.invokeAsync(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof FilterProfile.OperationMessage) {
            blackboard.signalGate("RegistrationReqReceived");
          }
        }
      });

      TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
      txManager.begin();

      TXStateInterface txState =
          ((TXStateProxyImpl) txManager.getTXState()).getRealDeal(null, null);

      ((TXState) txState).setDuringApplyChanges(() -> {
        try {
          blackboard.signalGate("StartCQ");
          blackboard.waitForGate("RegistrationReqReceived");
        } catch (TimeoutException | InterruptedException e) {
          // Do nothing
        }
      });

      cache.getRegion(REGION_NAME).put("Key-1", "value-1");
      txManager.commit();
    });

    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(REGION_NAME);

      QueryService queryService = clientCache.getQueryService();
      CqAttributesFactory cqaf = new CqAttributesFactory();
      TestCqListener testListener = new TestCqListener();
      cqaf.addCqListener(testListener);
      CqAttributes cqAttributes = cqaf.create();

      blackboard.waitForGate("StartCQ");
      SelectResults cqResults = queryService
          .newCq("Select * from " + SEPARATOR + REGION_NAME, cqAttributes)
          .executeWithInitialResults();
      blackboard.setMailbox("CqQueryResultCount", new Integer(cqResults.asList().size()));
    });

    GeodeAwaitility.await().untilAsserted(() -> {
      Integer CqQueryResultCount = blackboard.getMailbox("CqQueryResultCount");
      Integer CqEvents = blackboard.getMailbox("CqEvents");
      assertThat(CqQueryResultCount + CqEvents).isEqualTo(1);
    });

    serverAsync.await();
  }

  @Test
  public void verifyNoLockContentionBetweenInterestRegistrationAndTxCommit() throws Exception {
    String REGION_NAME = "region";
    Properties properties = new Properties();
    properties.put("conserve-sockets", conserveSockets);
    MemberVM locator = clusterStartupRule.startLocatorVM(0, new Properties());
    MemberVM server1 = clusterStartupRule.startServerVM(1, properties, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, properties, locator.getPort());
    ClientVM client = clusterStartupRule.startClientVM(3,
        cacheRule -> cacheRule.withServerConnection(server2.getPort()).withPoolSubscription(true));

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region region = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(1).setTotalNumBuckets(1).create())
          .create(REGION_NAME);

      // Force primary bucket to get created.
      region.put("Key-1", "value-1");
      region.destroy("Key-1");
    });

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(1).setTotalNumBuckets(1).create())
          .create(REGION_NAME);
    });

    AsyncInvocation serverAsync = server1.invokeAsync(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof FilterProfile.OperationMessage) {
            blackboard.signalGate("RegistrationReqReceived");
          }
        }
      });

      TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
      txManager.begin();

      TXStateInterface txState =
          ((TXStateProxyImpl) txManager.getTXState()).getRealDeal(null, null);

      ((TXState) txState).setDuringApplyChanges(() -> {
        try {
          blackboard.signalGate("StartReg");
          blackboard.waitForGate("RegistrationReqReceived");
        } catch (TimeoutException | InterruptedException e) {
          // Do nothing
        }
      });

      cache.getRegion(REGION_NAME).put("Key-5", "value-1");
      txManager.commit();
    });

    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region region = clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(REGION_NAME);
      blackboard.waitForGate("StartReg");
      region.registerInterest("Key-5", InterestResultPolicy.KEYS_VALUES);
      region.registerInterest("Key-6", InterestResultPolicy.KEYS_VALUES);

      GeodeAwaitility.await().untilAsserted(() -> {
        assertThat(region.size()).isEqualTo(1);
      });
    });

    serverAsync.await();
  }

  private class TestCqListener implements CqListener, Serializable {

    int numEvents = 0;

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents++;
      blackboard.setMailbox("CqEvents", new Integer(numEvents));
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    public int getNumEvents() {
      return numEvents;
    }
  }

}
