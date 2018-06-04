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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.Locator;
import org.apache.geode.test.junit.rules.Member;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.VMProvider;

public class MemberVM extends VMProvider implements Member {
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
  public File getWorkingDir() {
    return vm.getWorkingDirectory();
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
   * this gracefully shutdown the member inside this vm
   */
  @Override
  public void stopMember(boolean cleanWorkingDir) {
    super.stopMember(cleanWorkingDir);

    if (!cleanWorkingDir) {
      return;
    }

    // if using the dunit/vm dir as the preset working dir, need to cleanup dir
    // so that regions/indexes won't get persisted across tests
    Arrays.stream(getWorkingDir().listFiles()).forEach(FileUtils::deleteQuietly);
  }

  /**
   * this disconnects the distributed system of the member. The member will automatically try to
   * reconnect after 5 seconds.
   */
  public void forceDisconnectMember() {
    vm.invoke(() -> ClusterStartupRule.memberStarter.forceDisconnectMember());
  }

  public void waitTilLocatorFullyReconnected() {
    vm.invoke(() -> Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> {
      InternalLocator intLocator = InternalLocator.getLocator();
      InternalCache cache = ClusterStartupRule.getCache();
      return intLocator != null && cache != null && intLocator.getDistributedSystem().isConnected()
          && intLocator.isReconnected();
    }));
  }

  public void waitTilServerFullyReconnected() {
    vm.invoke(() -> Awaitility.waitAtMost(30, SECONDS).until(() -> {
      InternalDistributedSystem internalDistributedSystem =
          InternalDistributedSystem.getConnectedInstance();
      return internalDistributedSystem != null
          && internalDistributedSystem.getCache() != null
          && !internalDistributedSystem.getCache().getCacheServers().isEmpty();
    }));

  }

  /**
   * this should called on a locatorVM or a serverVM with jmxManager enabled
   */
  public void waitTillRegionsAreReadyOnServers(String regionPath, int serverCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter.waitTillRegionIsReadyOnServers(regionPath,
        serverCount));
  }

  public void waitTillDiskstoreIsReady(String diskstoreName, int serverCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter.waitTillDiskStoreIsReady(diskstoreName,
        serverCount));
  }

  public void waitTillAsyncEventQueuesAreReadyOnServers(String queueId, int serverCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter
        .waitTillAsyncEventQueuesAreReadyOnServers(queueId, serverCount));
  }

  public void waitTilGatewaySendersAreReady(int expectedGatewayObjectCount) {
    vm.invoke(() -> ClusterStartupRule.memberStarter
        .waitTilGatewaySendersAreReady(expectedGatewayObjectCount));
  }

}
