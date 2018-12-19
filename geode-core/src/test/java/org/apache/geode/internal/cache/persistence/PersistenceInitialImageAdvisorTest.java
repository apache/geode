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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.mockito.InOrder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.internal.lang.SystemPropertyHelper;

public class PersistenceInitialImageAdvisorTest {
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private InternalPersistenceAdvisor persistenceAdvisor;
  private CacheDistributionAdvisor cacheDistributionAdvisor =
      mock(CacheDistributionAdvisor.class, RETURNS_DEEP_STUBS);
  private PersistenceInitialImageAdvisor persistenceInitialImageAdvisor;

  @Before
  public void setup() {
    when(cacheDistributionAdvisor.getDistributionManager().getConfig().getAckWaitThreshold())
        .thenReturn(15);

    persistenceAdvisor = mock(InternalPersistenceAdvisor.class);
    when(persistenceAdvisor.getCacheDistributionAdvisor()).thenReturn(cacheDistributionAdvisor);
  }

  @Test
  public void clearsEqualMembers_ifHasDiskImageAndAllReplicatesAreUnequal() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceFromCacheDistributionAdvisor = adviceWithReplicates(3);
    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceFromCacheDistributionAdvisor);

    persistenceInitialImageAdvisor.getAdvice(null);

    verify(persistenceAdvisor, times(1)).clearEqualMembers();
  }

  @Test
  public void adviceIncludesAllReplicates_ifHasDiskImageAndAllReplicatesAreUnequal() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceFromCacheDistributionAdvisor = adviceWithReplicates(3);
    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceFromCacheDistributionAdvisor);

    InitialImageAdvice result = persistenceInitialImageAdvisor.getAdvice(null);

    assertThat(result.getReplicates())
        .isEqualTo(adviceFromCacheDistributionAdvisor.getReplicates());
  }

  @Test
  public void adviceIncludesNoReplicates_ifHasDiskImageAndAnyReplicateIsEqual() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceWithReplicates(9));
    when(persistenceAdvisor.checkMyStateOnMembers(any())).thenReturn(true);

    InitialImageAdvice result = persistenceInitialImageAdvisor.getAdvice(null);

    assertThat(result.getReplicates()).isEmpty();
  }

  @Test
  public void adviceIncludesAllReplicates_ifReplicatesAndNoDiskImage() {
    boolean hasDiskImage = false;
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisor(hasDiskImage);

    InitialImageAdvice adviceFromCacheDistributionAdvisor = adviceWithReplicates(9);

    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceFromCacheDistributionAdvisor);

    InitialImageAdvice result = persistenceInitialImageAdvisor.getAdvice(null);

    assertThat(result.getReplicates())
        .isEqualTo(adviceFromCacheDistributionAdvisor.getReplicates());
  }

  @Test
  public void obtainsFreshAdvice_ifAdviceIncludesNoReplicatesAndPreviousAdviceHasReplicates() {
    InitialImageAdvice previousAdviceWithReplicates = adviceWithReplicates(1);

    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    when(cacheDistributionAdvisor.adviseInitialImage(isNotNull(), anyBoolean()))
        .thenReturn(adviceWithReplicates(0));
    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceWithReplicates(1));

    persistenceInitialImageAdvisor.getAdvice(previousAdviceWithReplicates);

    InOrder inOrder = inOrder(cacheDistributionAdvisor);
    inOrder.verify(cacheDistributionAdvisor, times(1))
        .adviseInitialImage(same(previousAdviceWithReplicates), anyBoolean());
    inOrder.verify(cacheDistributionAdvisor, times(1)).adviseInitialImage(isNull(), anyBoolean());
    inOrder.verify(cacheDistributionAdvisor, times(0)).adviseInitialImage(any(), anyBoolean());
  }

  @Test
  public void attemptsToUpdateMembershipViewFromEachNonPersistentReplicate_ifAllReplicatesAreNonPersistent() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceWithNonPersistentReplicates = adviceWithNonPersistentReplicates(4);

    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceWithNonPersistentReplicates, adviceWithReplicates(1));

    // Make every attempt fail, forcing the advisor to try them all.
    doThrow(ReplyException.class).when(persistenceAdvisor).updateMembershipView(any(),
        anyBoolean());

    persistenceInitialImageAdvisor.getAdvice(null);

    for (InternalDistributedMember peer : adviceWithNonPersistentReplicates.getNonPersistent()) {
      verify(persistenceAdvisor, times(1)).updateMembershipView(peer, true);
    }
  }

  @Test
  public void updatesMembershipViewFromFirstNonPersistentReplicateThatReplies() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceWithNonPersistentReplicates = adviceWithNonPersistentReplicates(4);

    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceWithNonPersistentReplicates, adviceWithReplicates(1));

    doThrow(ReplyException.class) // Throw on first call
        .doNothing() // Then return without throwing
        .when(persistenceAdvisor).updateMembershipView(any(), anyBoolean());

    persistenceInitialImageAdvisor.getAdvice(null);

    // The second call succeeds. Expect no further calls.
    verify(persistenceAdvisor, times(2)).updateMembershipView(any(), anyBoolean());
  }

  @Test(expected = CacheClosedException.class)
  public void propagatesException_ifCancelInProgress() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cacheDistributionAdvisor.getAdvisee().getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(new CacheClosedException()).when(cancelCriterion).checkCancelInProgress(any());

    persistenceInitialImageAdvisor.getAdvice(null);
  }

  @Test
  public void returnsAdviceFromCacheDistributionAdvisor_ifNoOnlineOrPreviouslyOnlineMembers() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceFromCacheDistributionAdvisor = new InitialImageAdvice();

    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceFromCacheDistributionAdvisor);
    when(persistenceAdvisor.getPersistedOnlineOrEqualMembers()).thenReturn(new HashSet<>());

    InitialImageAdvice result = persistenceInitialImageAdvisor.getAdvice(null);

    assertThat(result).isSameAs(adviceFromCacheDistributionAdvisor);
  }

  @Test
  public void adviceIncludesReplicatesThatAppearWhileAcquiringTieLock() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceBeforeAcquiringTieLock = new InitialImageAdvice();
    InitialImageAdvice adviceAfterAcquiringTieLock = adviceWithReplicates(4);

    when(persistenceAdvisor.getPersistedOnlineOrEqualMembers()).thenReturn(persistentMemberIDs(1));
    when(persistenceAdvisor.getMembersToWaitFor(any(), any())).thenReturn(emptySet());
    when(persistenceAdvisor.acquireTieLock()).thenReturn(true);

    when(cacheDistributionAdvisor.adviseInitialImage(any(), anyBoolean()))
        .thenReturn(adviceBeforeAcquiringTieLock, adviceAfterAcquiringTieLock);

    InitialImageAdvice result = persistenceInitialImageAdvisor.getAdvice(null);

    assertThat(result.getReplicates()).isEqualTo(adviceAfterAcquiringTieLock.getReplicates());
  }

  @Test(expected = ConflictingPersistentDataException.class)
  public void propagatesException_ifIncompatibleWithReplicateThatAppearsWhileAcquiringTieLock() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceBeforeAcquiringTieLock = new InitialImageAdvice();
    InitialImageAdvice adviceAfterAcquiringTieLock = adviceWithReplicates(4);

    when(persistenceAdvisor.getPersistedOnlineOrEqualMembers()).thenReturn(persistentMemberIDs(1));
    when(persistenceAdvisor.getMembersToWaitFor(any(), any())).thenReturn(emptySet());
    when(persistenceAdvisor.acquireTieLock()).thenReturn(true);

    when(cacheDistributionAdvisor.adviseInitialImage(any(), anyBoolean()))
        .thenReturn(adviceBeforeAcquiringTieLock, adviceAfterAcquiringTieLock);

    doThrow(ConflictingPersistentDataException.class).when(persistenceAdvisor)
        .checkMyStateOnMembers(any());

    persistenceInitialImageAdvisor.getAdvice(null);
  }

  @Test
  public void announcesProgressToPersistenceAdvisor_whenWaitingForMissingMembers() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    setMembershipChangePollDuration(Duration.ofSeconds(0));
    HashSet<PersistentMemberID> offlineMembersToWaitFor = memberIDs("offline member", 1);
    Set<PersistentMemberID> membersToWaitFor = new HashSet<>(offlineMembersToWaitFor);

    when(persistenceAdvisor.getPersistedOnlineOrEqualMembers()).thenReturn(offlineMembersToWaitFor);
    when(persistenceAdvisor.getMembersToWaitFor(any(), any())).thenAnswer(invocation -> {
      Set<PersistentMemberID> offlineMembers = invocation.getArgument(1);
      offlineMembers.addAll(offlineMembersToWaitFor);
      return membersToWaitFor;
    });

    when(cacheDistributionAdvisor.adviseInitialImage(null, true))
        .thenReturn(adviceWithReplicates(0), adviceWithReplicates(1));

    persistenceInitialImageAdvisor.getAdvice(null);

    InOrder inOrder = inOrder(persistenceAdvisor);
    inOrder.verify(persistenceAdvisor, times(1)).beginWaitingForMembershipChange(membersToWaitFor);
    inOrder.verify(persistenceAdvisor, times(1)).setWaitingOnMembers(isNotNull(),
        eq(offlineMembersToWaitFor));
    inOrder.verify(persistenceAdvisor, times(1)).endWaitingForMembershipChange();
    inOrder.verify(persistenceAdvisor, times(1)).setWaitingOnMembers(isNull(), isNull());
    inOrder.verify(persistenceAdvisor, times(0)).setWaitingOnMembers(any(), any());
  }

  private PersistenceInitialImageAdvisor persistenceInitialImageAdvisor(boolean hasDiskImage) {
    return new PersistenceInitialImageAdvisor(persistenceAdvisor, "short disk store ID",
        "region path", cacheDistributionAdvisor, hasDiskImage);
  }

  private PersistenceInitialImageAdvisor persistenceInitialImageAdvisorWithDiskImage() {
    boolean hasDiskImage = true;
    return persistenceInitialImageAdvisor(hasDiskImage);
  }

  private static InitialImageAdvice adviceWithReplicates(int count) {
    Set<InternalDistributedMember> replicates = members("replicate", count);
    return new InitialImageAdvice(replicates, emptySet(), emptySet(), emptySet(), emptySet(),
        emptySet(), emptyMap());
  }

  private static InitialImageAdvice adviceWithNonPersistentReplicates(int count) {
    Set<InternalDistributedMember> nonPersistentReplicates = members("non-persistent", count);
    return new InitialImageAdvice(emptySet(), emptySet(), emptySet(), emptySet(), emptySet(),
        nonPersistentReplicates, emptyMap());
  }

  private static Set<InternalDistributedMember> members(String namePrefix, int count) {
    return IntStream.range(0, count).mapToObj(i -> namePrefix + ' ' + i)
        .map(name -> mock(InternalDistributedMember.class, name)).collect(toSet());
  }

  private static HashSet<PersistentMemberID> memberIDs(String namePrefix, int count) {
    return IntStream.range(0, count).mapToObj(i -> namePrefix + ' ' + i)
        .map(name -> mock(PersistentMemberID.class, name)).collect(toCollection(HashSet::new));
  }

  private static HashSet<PersistentMemberID> persistentMemberIDs(int count) {
    return memberIDs("persisted online or equal member", count);
  }

  private static void setMembershipChangePollDuration(Duration timeout) {
    System.setProperty(
        SystemPropertyHelper.GEODE_PREFIX
            + SystemPropertyHelper.PERSISTENT_VIEW_RETRY_TIMEOUT_SECONDS,
        String.valueOf(timeout.getSeconds()));
  }
}
