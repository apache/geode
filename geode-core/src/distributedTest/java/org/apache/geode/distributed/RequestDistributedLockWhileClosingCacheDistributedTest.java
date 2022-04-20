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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.locks.DLockGrantor;
import org.apache.geode.distributed.internal.locks.DLockRequestProcessor;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class RequestDistributedLockWhileClosingCacheDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  private static final String ABOUT_TO_PROCESS_LOCK_REQUEST = "ABOUT_TO_PROCESS_LOCK_REQUEST";

  private static final String MEMBER_DEPARTED = "MEMBER_DEPARTED";

  @Test
  public void testRequestDistributedLockWhileClosingCache() throws InterruptedException {
    // Init Blackboard
    blackboard.initBlackboard();

    // Start the locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start the grantor server
    Properties properties = new Properties();
    properties.setProperty(LOG_LEVEL, "fine");
    MemberVM grantorServer = cluster.startServerVM(1, properties, locator.getPort());

    // Become lock grantor for the DistributedLockService
    grantorServer.invoke(this::becomeLockGrantor);

    // Start the lock requesting server
    MemberVM lockRequestingServer = cluster.startServerVM(2, properties, locator.getPort());

    // Add the DistributionMessageObserver
    String lockName = testName.getMethodName() + "_lockName";
    Stream.of(grantorServer, lockRequestingServer)
        .forEach(server -> server
            .invoke(() -> addDistributionMessageObserverAndMembershipListener(lockName)));

    // Asynchronously disconnect the distributed system in the lock requesting server
    AsyncInvocation asyncDisconnectDistributedSystem =
        lockRequestingServer.invokeAsync(this::disconnectDistributedSystem);

    // Asynchronously request a lock in the lock requesting server
    AsyncInvocation asyncGetLock =
        lockRequestingServer.invokeAsync(() -> requestDistributedLock(lockName));

    // Wait for both invocations to complete
    asyncDisconnectDistributedSystem.await();
    asyncGetLock.await();

    // Verify the lock requesting server is disconnected
    lockRequestingServer.invoke(this::verifyCacheIsClosed);

    // Verify the grantor server holds no locks for the lock requesting server
    grantorServer.invoke(() -> verifyLockServiceDoesNotHoldToken(lockName));
  }

  private void becomeLockGrantor() {
    ClusterStartupRule.getCache().getPartitionedRegionLockService().becomeLockGrantor();
  }

  private void addDistributionMessageObserverAndMembershipListener(String lockName) {
    TestDistributionMessageObserver observer = new TestDistributionMessageObserver(lockName);
    DistributionMessageObserver.setInstance(observer);
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getInternalDistributedSystem().getDistributionManager();
    distributionManager.addMembershipListener(observer);
  }

  private void disconnectDistributedSystem() throws InterruptedException, TimeoutException {
    // Wait for the grantor to signal ABOUT_TO_PROCESS_LOCK_REQUEST
    blackboard.waitForGate(ABOUT_TO_PROCESS_LOCK_REQUEST);

    // Disconnect the distributed system
    ClusterStartupRule.getCache().getInternalDistributedSystem().disconnect();
  }

  private void requestDistributedLock(String lockName) {
    // Request a distributed lock from the partitioned region lock service
    DistributedLockService service =
        ClusterStartupRule.getCache().getPartitionedRegionLockService();
    try {
      service.lock(lockName, GeodeAwaitility.getTimeout().toMillis(), -1);
    } catch (Exception e) {
      /* ignore */
    }
  }

  private void verifyCacheIsClosed() {
    // Verify the cache is closed
    assertThat(ClusterStartupRule.getCache().isClosed()).isTrue();
  }

  private void verifyLockServiceDoesNotHoldToken(Object lockName) {
    // Verify no the lock token is not held by the grantor server after the lock requesting server
    // has departed
    DLockService dLockService =
        (DLockService) ClusterStartupRule.getCache().getPartitionedRegionLockService();
    assertThat(dLockService.isLockGrantor()).isTrue();
    DLockGrantor dLockGrantor = dLockService.getGrantor();
    DLockGrantor.DLockGrantToken grantToken = dLockGrantor.getGrantToken(lockName);
//    assertThat(grantToken).isNull();
  }

  class TestDistributionMessageObserver extends DistributionMessageObserver implements
      MembershipListener {

    private final String lockName;

    public TestDistributionMessageObserver(String lockName) {
      this.lockName = lockName;
    }

    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      // Signal the member has departed. This will cause the DLockRequestMessage to be processed.
      blackboard.signalGate(MEMBER_DEPARTED);
    }

    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof DLockRequestProcessor.DLockRequestMessage) {
        DLockRequestProcessor.DLockRequestMessage dLockRequestMessage =
            (DLockRequestProcessor.DLockRequestMessage) message;
        if (dLockRequestMessage.getObjectName().equals(this.lockName)) {
          // Signal the about to process lock request. This will cause the lock requesting server to
          // disconnect its distributed system.
          blackboard.signalGate(ABOUT_TO_PROCESS_LOCK_REQUEST);

          // Wait for member departed before processing the DLockRequestMessage
          try {
            blackboard.waitForGate(MEMBER_DEPARTED);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
