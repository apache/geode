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
package org.apache.geode.test.dunit.rules;


import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.test.junit.rules.Member;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.VMProvider;

public class MemberVM extends VMProvider implements Member {
  private Logger logger = LogService.getLogger();
  protected Member member;
  protected VM vm;

  public MemberVM(Member member, VM vm) {
    this.member = member;
    this.vm = vm;
  }

  public boolean isLocator() {
    return (member instanceof Locator);
  }

  public VM getVM() {
    return vm;
  }

  public Member getMember() {
    return member;
  }

  @Override
  public int getPort() {
    return member.getPort();
  }

  @Override
  public int getJmxPort() {
    return member.getJmxPort();
  }

  @Override
  public int getHttpPort() {
    return member.getHttpPort();
  }

  @Override
  public String getName() {
    return member.getName();
  }

  public int getEmbeddedLocatorPort() {
    if (!(member instanceof Server)) {
      throw new RuntimeException("member needs to be a server");
    }
    return ((Server) member).getEmbeddedLocatorPort();
  }

  /**
   * this disconnects the distributed system of the member. The member will automatically try to
   * reconnect after 5 seconds.
   */
  public void forceDisconnect() {
    vm.invoke("force disconnect", () -> ClusterStartupRule.memberStarter.forceDisconnectMember());
  }

  /**
   * This disconnects the distributed system of the member. The reconnect thread will wait for at
   * least the given delay before completing the attempt.
   *
   * @param delayReconnecting minimum delay in milliseconds before reconnect can complete.
   */
  public void forceDisconnect(final long delayReconnecting) {
    vm.invoke(() -> {
      // The reconnect thread can yield the CPU before allowing the listeners to be invoked. The
      // latch ensures that the listener is guaranteed to be called before this method returns thus
      // ensuring that reconnection has started but not yet completed.
      CountDownLatch latch = new CountDownLatch(1);
      InternalDistributedSystem.addReconnectListener(
          new InternalDistributedSystem.ReconnectListener() {
            @Override
            public void reconnecting(InternalDistributedSystem oldSystem) {
              try {
                Thread.sleep(delayReconnecting);
                latch.countDown();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }

            @Override
            public void onReconnect(InternalDistributedSystem oldSystem,
                InternalDistributedSystem newSystem) {}
          });
      ClusterStartupRule.memberStarter.forceDisconnectMember();
      latch.await();
    });
  }

  public void waitTilLocatorFullyReconnected() {
    vm.invoke(() -> {
      try {
        await().until(() -> {
          InternalLocator intLocator = ClusterStartupRule.getLocator();
          InternalCache cache = ClusterStartupRule.getCache();
          return intLocator != null && cache != null && intLocator.getDistributedSystem()
              .isConnected() && intLocator.isReconnected();
        });
      } catch (Exception e) {
        // provide more information when condition is not satisfied after one minute
        InternalLocator intLocator = ClusterStartupRule.getLocator();
        InternalCache cache = ClusterStartupRule.getCache();
        logger.info("locator is not null: " + (intLocator != null));
        logger.info("cache is not null: " + (cache != null));
        logger.info("ds is connected: " + (intLocator.getDistributedSystem().isConnected()));
        logger.info("locator is reconnected: " + (intLocator.isReconnected()));
        throw e;
      }

    });
  }

  public void waitTilServerFullyReconnected() {
    vm.invoke(() -> {
      try {
        await().until(() -> {
          InternalDistributedSystem internalDistributedSystem =
              InternalDistributedSystem.getConnectedInstance();
          return internalDistributedSystem != null
              && internalDistributedSystem.getCache() != null
              && !internalDistributedSystem.getCache().getCacheServers().isEmpty();
        });
      } catch (Exception e) {
        // provide more information when condition is not satisfied after one minute
        InternalDistributedSystem internalDistributedSystem =
            InternalDistributedSystem.getConnectedInstance();
        logger.info("ds is not null: " + (internalDistributedSystem != null));
        logger.info("cache is not null: " + (internalDistributedSystem.getCache() != null));
        logger.info("has cache server: "
            + (!internalDistributedSystem.getCache().getCacheServers().isEmpty()));
        throw e;
      }
    });
  }

  /**
   * this should called on a locatorVM or a serverVM with jmxManager enabled
   */
  public void waitUntilRegionIsReadyOnExactlyThisManyServers(String regionPath, int serverCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter
        .waitUntilRegionIsReadyOnExactlyThisManyServers(regionPath, serverCount));
  }


  /**
   * this can only be called on a locator (or a vm that is not that serverName)
   */
  public void waitTillClientsAreReadyOnServers(String serverName, int serverPort, int clientCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter.waitTillClientsAreReadyOnServer(serverName,
        serverPort, clientCount));
  }

  public void waitUntilDiskStoreIsReadyOnExactlyThisManyServers(String diskstoreName,
      int serverCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter
        .waitUntilDiskStoreIsReadyOnExactlyThisManyServers(diskstoreName, serverCount));
  }

  public void waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(String queueId,
      int serverCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter
        .waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(queueId, serverCount));
  }

  public void waitUntilGatewaySendersAreReadyOnExactlyThisManyServers(
      int expectedGatewayObjectCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter
        .waitUntilGatewaySendersAreReadyOnExactlyThisManyServers(expectedGatewayObjectCount));
  }

  public void waitTillCacheClientProxyHasBeenPaused() {
    vm.invoke(() -> ClusterStartupRule.memberStarter.waitTillCacheClientProxyHasBeenPaused());
  }

}
