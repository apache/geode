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
package org.apache.geode.internal.cache.persistence;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class MembershipChangeListener implements MembershipListener, PersistentStateListener {
  private static final int PAUSE_MILLIS = 100;
  static final int PERSISTENT_VIEW_RETRY =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "PERSISTENT_VIEW_RETRY", 5);

  private final int ackWaitThreshold;
  private final Runnable warning;
  private final BooleanSupplier cancelCondition;

  private boolean membershipChanged;
  private boolean warned;

  public MembershipChangeListener(InternalPersistenceAdvisor persistenceAdvisor) {
    ackWaitThreshold = persistenceAdvisor.getCacheDistributionAdvisor().getDistributionManager()
        .getConfig().getAckWaitThreshold();
    cancelCondition = createCancelCondition(persistenceAdvisor);
    warning = persistenceAdvisor::logWaitingForMembers;
  }

  public synchronized void waitForChange() throws InterruptedException {
    long now = System.nanoTime();
    long expirationTime = now + TimeUnit.SECONDS.toNanos(PERSISTENT_VIEW_RETRY);
    long warningTime = now + TimeUnit.SECONDS.toNanos(ackWaitThreshold);
    while (!membershipChanged && !cancelCondition.getAsBoolean()
        && System.nanoTime() <= expirationTime) {
      wait(PAUSE_MILLIS);
      if (System.nanoTime() > warningTime) {
        warnOnce();
      }
    }
    membershipChanged = false;
  }

  private void warnOnce() {
    if (!warned) {
      warning.run();
      warned = true;
    }
  }

  private synchronized void afterMembershipChange() {
    membershipChanged = true;
    notifyAll();
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    afterMembershipChange();
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {
    afterMembershipChange();
  }

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  @Override
  public void memberOffline(InternalDistributedMember member, PersistentMemberID persistentID) {
    afterMembershipChange();
  }

  @Override
  public void memberOnline(InternalDistributedMember member, PersistentMemberID persistentID) {
    afterMembershipChange();
  }

  @Override
  public void memberRemoved(PersistentMemberID id, boolean revoked) {
    afterMembershipChange();
  }

  private static BooleanSupplier createCancelCondition(
      InternalPersistenceAdvisor persistenceAdvisor) {
    CancelCriterion cancelCriterion =
        persistenceAdvisor.getCacheDistributionAdvisor().getAdvisee().getCancelCriterion();
    return () -> {
      persistenceAdvisor.checkInterruptedByShutdownAll();
      cancelCriterion.checkCancelInProgress(null);
      return persistenceAdvisor.isClosed();
    };
  }
}
