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
import static org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase.getBlackboard;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.test.junit.rules.Member;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.VMProvider;

public class MemberVM extends VMProvider implements Member {
  private static final Logger logger = LogService.getLogger();
  protected Member member;
  protected VM vm;

  public MemberVM(Member member, VM vm) {
    this.member = member;
    this.vm = vm;
  }

  @Override
  public boolean isLocator() {
    return (member instanceof Locator);
  }

  @Override
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
   * This disconnects the distributed system of the member. The reconnect thread will wait until the
   * given mailbox is set to true from within the vm that this method is invoked on.
   *
   * For example, if forceDisconnect is called like this:
   * member1.forceDisconnect(300, "reconnectReady");
   * Then the reconnect should be triggered like this:
   * member1.invoke(() -> getBlackboard().setMailbox("reconnectReady", true));
   *
   * Setting this mailbox from within the test JVM will not cause the member to begin reconnecting.
   *
   * @param timeout maximum time that the reconnect can take
   * @param timeUnit the units for the timeout
   * @param reconnectBBKey String key to the blackboard mailbox containing a boolean that shall
   *        be set to true when the member should start reconnecting.
   */
  public void forceDisconnect(long timeout, TimeUnit timeUnit, String reconnectBBKey) {
    vm.invoke(() -> {
      DUnitBlackboard server1BB = getBlackboard();
      server1BB.initBlackboard();
      server1BB.setMailbox(reconnectBBKey, false);

      InternalDistributedSystem.addReconnectListener(
          new InternalDistributedSystem.ReconnectListener() {
            @Override
            public void reconnecting(InternalDistributedSystem oldSystem) {
              await().atMost(timeout, timeUnit)
                  .until(() -> server1BB.getMailbox(reconnectBBKey));
            }

            @Override
            public void onReconnect(InternalDistributedSystem oldSystem,
                InternalDistributedSystem newSystem) {}
          });

      ClusterStartupRule.memberStarter.forceDisconnectMember();
    });
  }

  public void waitTilFullyReconnected() {
    vm.invoke(() -> {
      ClusterStartupRule.memberStarter.waitTilFullyReconnected();
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

  public void waitTillCacheClientProxyHasBeenPaused() {
    vm.invoke(() -> ClusterStartupRule.memberStarter.waitTillCacheClientProxyHasBeenPaused());
  }

}
