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

import static java.time.Instant.now;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Provides warning logging for persistence advisors while waiting for membership changes.
 */
public class MembershipChangeListener implements MembershipListener, PersistentStateListener {

  private static final int POLL_INTERVAL_MILLIS = 100;

  private final Duration pollDuration;
  private final Duration warningDelay;
  private final BooleanSupplier cancelCondition;
  private final Runnable warning;

  private boolean membershipChanged;
  private boolean warned;

  /**
   * Please use {@link MembershipChangeListenerFactory} to create instances.
   *
   * @param warningDelay delay before logging the warning once
   * @param pollDuration timeout before returning from wait for change
   * @param cancelCondition indicates if wait for change has been cancelled
   * @param warning simple runnable for logging the warning
   */
  MembershipChangeListener(Duration warningDelay, Duration pollDuration,
      BooleanSupplier cancelCondition, Runnable warning) {
    this.warningDelay = warningDelay;
    this.pollDuration = pollDuration;
    this.cancelCondition = cancelCondition;
    this.warning = warning;
  }

  /**
   * Wait for membership change and log warning after waiting at least the warning delay.
   */
  public synchronized void waitForChange() throws InterruptedException {
    Instant now = now();
    Instant timeoutTime = now.plus(pollDuration);
    Instant warningTime = now.plus(warningDelay);

    while (!membershipChanged && !cancelCondition.getAsBoolean() && now().isBefore(timeoutTime)) {
      warnOnceAfter(warningTime);
      wait(POLL_INTERVAL_MILLIS);
    }

    membershipChanged = false;
  }

  private void warnOnceAfter(Instant warningTime) {
    if (!warned && warningTime.isBefore(now())) {
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
}
