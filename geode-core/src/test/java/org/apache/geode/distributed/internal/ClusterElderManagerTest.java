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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.locks.ElderState;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class ClusterElderManagerTest {
  private MembershipManager memberManager;
  private CancelCriterion systemCancelCriterion;
  private InternalDistributedSystem system;
  private CancelCriterion cancelCriterion;
  private ClusterDistributionManager clusterDistributionManager;
  private InternalDistributedMember member0;
  private final InternalDistributedMember member1 = mock(InternalDistributedMember.class);
  private final InternalDistributedMember member2 = mock(InternalDistributedMember.class);

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Before
  public void before() {
    member0 = mock(InternalDistributedMember.class);
    clusterDistributionManager = mock(ClusterDistributionManager.class);
    cancelCriterion = mock(CancelCriterion.class);
    system = mock(InternalDistributedSystem.class);
    systemCancelCriterion = mock(CancelCriterion.class);
    memberManager = mock(MembershipManager.class);

    when(clusterDistributionManager.getCancelCriterion()).thenReturn(cancelCriterion);
    when(clusterDistributionManager.getSystem()).thenReturn(system);
    when(system.getCancelCriterion()).thenReturn(systemCancelCriterion);
    when(clusterDistributionManager.getMembershipManager()).thenReturn(memberManager);
  }

  @Test
  public void getElderIdReturnsOldestMember() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member2));

    assertThat(clusterElderManager.getElderId()).isEqualTo(member1);

  }

  @Test
  public void getElderIdWithNoMembers() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList());

    assertThat(clusterElderManager.getElderId()).isNull();
  }

  @Test
  public void getElderIdIgnoresAdminMembers() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(member1.getVmKind()).thenReturn(ClusterDistributionManager.ADMIN_ONLY_DM_TYPE);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member2));

    assertThat(clusterElderManager.getElderId()).isEqualTo(member2);
  }


  @Test
  public void getElderIdIgnoresSurpriseMembers() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(memberManager.isSurpriseMember(eq(member1))).thenReturn(true);

    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member2));

    assertThat(clusterElderManager.getElderId()).isEqualTo(member2);
  }

  @Test
  public void isElderIfOldestMember() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member0, member1));
    when(clusterDistributionManager.getId()).thenReturn(member0);
    assertThat(clusterElderManager.isElder()).isTrue();
  }

  @Test
  public void isNotElderIfOldestMember() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member0));
    when(clusterDistributionManager.getId()).thenReturn(member0);
    assertThat(clusterElderManager.isElder()).isFalse();
  }

  @Test
  public void waitForElderReturnsTrueIfAnotherMemberIsElder() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member0));
    assertThat(clusterElderManager.waitForElder(member1)).isTrue();
  }

  @Test
  public void waitForElderReturnsFalseIfWeAreElder() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.isCurrentMember(eq(member1))).thenReturn(true);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member0, member1));
    assertThat(clusterElderManager.waitForElder(member1)).isFalse();
  }

  @Test
  public void waitForElderReturnsFalseIfDesiredElderIsNotACurrentMember() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers())
        .thenReturn(Arrays.asList(member2, member0, member1));
    assertThat(clusterElderManager.waitForElder(member1)).isFalse();
  }

  @Test
  public void waitForElderWaits() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member0));
    when(clusterDistributionManager.isCurrentMember(eq(member0))).thenReturn(true);

    assertThatRunnableWaits(() -> clusterElderManager.waitForElder(member0));
  }

  @Test
  public void waitForElderStopsWaitingWhenUpdated() {
    ClusterElderManager clusterElderManager = new ClusterElderManager(clusterDistributionManager);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.isCurrentMember(eq(member0))).thenReturn(true);

    AtomicReference<List<InternalDistributedMember>> currentMembers =
        new AtomicReference<>(Arrays.asList(member1, member0));
    when(clusterDistributionManager.getViewMembers()).then(invocation -> currentMembers.get());

    AtomicReference<MembershipListener> membershipListener = new AtomicReference<>();
    doAnswer(invocation -> {
      membershipListener.set(invocation.getArgument(0));
      return null;
    }).when(clusterDistributionManager).addMembershipListener(any());

    Callable<Boolean> waitForElder = () -> clusterElderManager.waitForElder(member0);

    Callable<Void> updateMembershipView = () -> {
      // Wait for membership listener to be added
      await().until(() -> membershipListener.get() != null);

      currentMembers.set(Arrays.asList(member0));
      membershipListener.get().memberDeparted(clusterDistributionManager, member1, true);
      return null;
    };

    concurrencyRule.add(waitForElder).expectValue(true);
    concurrencyRule.add(updateMembershipView);
    concurrencyRule.executeInParallel();

    assertThat(clusterElderManager.getElderId()).isEqualTo(member0);
  }

  @Test
  public void getElderStateAsElder() {
    Supplier<ElderState> elderStateSupplier = mock(Supplier.class);
    ElderState elderState = mock(ElderState.class);
    when(elderStateSupplier.get()).thenReturn(elderState);
    ClusterElderManager clusterElderManager =
        new ClusterElderManager(clusterDistributionManager, elderStateSupplier);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member0, member1));

    assertThat(clusterElderManager.getElderState(false)).isEqualTo(elderState);
    verify(elderStateSupplier, times(1)).get();
  }

  @Test
  public void getElderStateGetsBuiltOnceAsElder() {
    Supplier<ElderState> elderStateSupplier = mock(Supplier.class);
    ElderState elderState = mock(ElderState.class);
    when(elderStateSupplier.get()).thenReturn(elderState);
    ClusterElderManager clusterElderManager =
        new ClusterElderManager(clusterDistributionManager, elderStateSupplier);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member0, member1));

    assertThat(clusterElderManager.getElderState(false)).isEqualTo(elderState);
    assertThat(clusterElderManager.getElderState(false)).isEqualTo(elderState);

    // Make sure that we only create the elder state once
    verify(elderStateSupplier, times(1)).get();
  }

  @Test
  public void getElderStateFromMultipleThreadsAsElder() {
    Supplier<ElderState> elderStateSupplier = mock(Supplier.class);
    ElderState elderState = mock(ElderState.class);
    when(elderStateSupplier.get()).thenReturn(elderState);
    ClusterElderManager clusterElderManager =
        new ClusterElderManager(clusterDistributionManager, elderStateSupplier);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member0, member1));

    Callable<ElderState> callable = () -> clusterElderManager.getElderState(false);

    concurrencyRule.add(callable).expectValue(elderState);
    concurrencyRule.add(callable).expectValue(elderState);
    concurrencyRule.executeInParallel();

    // Make sure that we only create the elder state once
    verify(elderStateSupplier, times(1)).get();
  }

  @Test
  public void getElderStateNotAsElder() {
    Supplier<ElderState> elderStateSupplier = mock(Supplier.class);
    ClusterElderManager clusterElderManager =
        new ClusterElderManager(clusterDistributionManager, elderStateSupplier);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member0));

    assertThat(clusterElderManager.getElderState(false)).isEqualTo(null);
    verify(elderStateSupplier, times(0)).get();
  }

  @Test
  public void getElderStateWaitsToBecomeElder() {
    Supplier<ElderState> elderStateSupplier = mock(Supplier.class);
    ClusterElderManager clusterElderManager =
        new ClusterElderManager(clusterDistributionManager, elderStateSupplier);
    when(clusterDistributionManager.getId()).thenReturn(member0);
    when(clusterDistributionManager.getViewMembers()).thenReturn(Arrays.asList(member1, member0));
    when(clusterDistributionManager.isCurrentMember(eq(member0))).thenReturn(true);

    assertThatRunnableWaits(() -> clusterElderManager.getElderState(true));

    verify(elderStateSupplier, times(0)).get();
  }

  private void assertThatRunnableWaits(Runnable runnable) {
    Thread waitThread = new Thread(runnable);

    waitThread.start();

    EnumSet<Thread.State> waitingStates =
        EnumSet.of(Thread.State.WAITING, Thread.State.TIMED_WAITING);
    try {
      await()
          .until(() -> waitingStates.contains(waitThread.getState()));
    } finally {
      waitThread.interrupt();
    }
  }
}
