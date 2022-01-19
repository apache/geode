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
package org.apache.geode.distributed.internal;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.locks.ElderState;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.util.concurrent.StoppableReentrantLock;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClusterElderManager {
  private static final Logger logger = LogService.getLogger();

  private final ClusterDistributionManager clusterDistributionManager;
  private final StoppableReentrantLock elderLock;

  private ElderState elderState;
  private volatile boolean elderStateInitialized;
  private final Supplier<ElderState> elderStateSupplier;

  public ClusterElderManager(ClusterDistributionManager clusterDistributionManager) {
    this(clusterDistributionManager, () -> new ElderState(clusterDistributionManager));
  }

  public ClusterElderManager(ClusterDistributionManager clusterDistributionManager,
      Supplier<ElderState> elderStateSupplier) {
    this.clusterDistributionManager = clusterDistributionManager;
    elderLock = new StoppableReentrantLock(clusterDistributionManager.getCancelCriterion());
    this.elderStateSupplier = elderStateSupplier;
  }

  /**
   * Based on a recent JGroups view, return a member that might be the next elder.
   *
   * @return the elder candidate, possibly this VM.
   */
  InternalDistributedMember getElderCandidate() {
    return getElderCandidates().stream().findFirst().orElse(null);
  }

  List<InternalDistributedMember> getElderCandidates() {
    List<InternalDistributedMember> theMembers = clusterDistributionManager.getViewMembers();

    return theMembers.stream()
        .filter(member -> member.getVmKind() != ClusterDistributionManager.ADMIN_ONLY_DM_TYPE)
        .filter(
            member -> !clusterDistributionManager.getDistribution().isSurpriseMember(member))
        .collect(Collectors.toList());
  }

  public InternalDistributedMember getElderId() throws DistributedSystemDisconnectedException {
    if (clusterDistributionManager.isCloseInProgress()) {
      throw new DistributedSystemDisconnectedException(
          "no valid elder when system is shutting down",
          clusterDistributionManager.getRootCause());
    }
    clusterDistributionManager.getSystem().getCancelCriterion().checkCancelInProgress(null);

    return getElderCandidate();
  }

  public boolean isElder() {
    return clusterDistributionManager.getId().equals(getElderCandidate());
  }

  public ElderState getElderState(boolean waitToBecomeElder) throws InterruptedException {
    if (waitToBecomeElder) {
      waitForElder(clusterDistributionManager.getId());
    }

    if (!isElder() && !waitToBecomeElder) {
      return null; // early return if this clusterDistributionManager is not the elder
    }

    if (elderStateInitialized) {
      return elderState;
    } else {
      return initializeElderState();
    }
  }

  private ElderState initializeElderState() {
    elderLock.lock();

    try {
      if (elderState == null) {
        elderState = elderStateSupplier.get();
      }
      elderStateInitialized = true;
    } finally {
      elderLock.unlock();
    }

    return elderState;
  }

  /**
   * Waits until (elder is desiredElder) or (desiredElder is no longer a member) or (the local
   * member is the elder)
   *
   * @return true if desiredElder is the elder; false if it is no longer a member or the local
   *         member is the elder
   */
  public boolean waitForElder(final InternalDistributedMember desiredElder)
      throws InterruptedException {
    MembershipChangeListener changeListener =
        new MembershipChangeListener();

    clusterDistributionManager.addMembershipListener(changeListener);

    InternalDistributedMember currentElder;

    try {
      while (true) {
        if (clusterDistributionManager.isCloseInProgress()) {
          return false;
        }
        currentElder = getElderCandidate();
        if (desiredElder.equals(currentElder)) {
          return true;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Expecting Elder to be {} but it is {}.",
              desiredElder, currentElder);
        }
        if (!clusterDistributionManager.isCurrentMember(desiredElder)) {
          return false; // no longer present
        }
        if (!clusterDistributionManager.getId().equals(desiredElder)
            && clusterDistributionManager.getId().equals(currentElder)) {
          // Once we become the elder we no longer allow anyone else to be the
          // elder so don't let them wait anymore.
          return false;
        }

        if (logger.isDebugEnabled()) {
          logger.debug("Waiting for membership to change");
        }
        changeListener.waitForMembershipChange();
      }
    } finally {
      clusterDistributionManager.removeMembershipListener(changeListener);
    }
  }

  private static class MembershipChangeListener implements MembershipListener {
    private boolean changeOccurred = false;

    @Override
    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember theId) {
      signalChange();
      // nothing needed
    }

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember theId, boolean crashed) {
      signalChange();
    }

    @Override
    public void memberSuspect(DistributionManager distributionManager,
        InternalDistributedMember id, InternalDistributedMember whoSuspected,
        String reason) {
      signalChange();
    }

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      signalChange();
    }

    private synchronized void signalChange() {
      changeOccurred = true;
      notifyAll();
    }

    public synchronized void waitForMembershipChange() throws InterruptedException {
      if (!changeOccurred) {
        wait(100);
      }
      changeOccurred = false;
    }
  }
}
