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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doThrow;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
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

  // TODO:
  // - replicates && disk image && none equal
  // - clear equal members
  // - return advice with all replicates (attempt GII)

  // TODO:
  // - no replicates && previous replicates
  // - clear previous advice
  // - fetch new advice from cache distribution advisor (which will have replicates)
  // - react to new advice

  // TODO:
  // - no replicates && no previous replicates && members to wait for
  // - return advice from cache distribution advisor


  // TODO:
  // - no replicates && no previous replicates && members to wait for
  // - tests scenarios TBD

  // - replicates && disk image && at least one equal
  // - return advice with empty replicates (load from disk)
  //
  @Ignore("Complication: Checks state on peers")
  @Test
  public void equalReplicates() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice result = persistenceInitialImageAdvisor.getAdvice(null);
    assertThat(result.getReplicates()).isEmpty();
  }

  @Test
  public void attemptsToUpdateMembershipViewFromEachNonPersistentReplicate_ifAllReplicatesAreNonPersistent() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    InitialImageAdvice adviceWithNonPersistentReplicates = adviceWithNonPersistentReplicates(4);

    when(cacheDistributionAdvisor.adviseInitialImage(isNull(), anyBoolean()))
        .thenReturn(adviceWithNonPersistentReplicates, adviceWithReplicates(1));

    doThrow(new ReplyException()).when(persistenceAdvisor).updateMembershipView(any(),
        anyBoolean());

    persistenceInitialImageAdvisor.getAdvice(null);

    for (InternalDistributedMember peer : adviceWithNonPersistentReplicates.getNonPersistent()) {
      verify(persistenceAdvisor, times(1)).updateMembershipView(peer, true);
    }
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

  @Test(expected = CacheClosedException.class)
  public void propagatesThrownException_ifCancelInProgress() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cacheDistributionAdvisor.getAdvisee().getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(new CacheClosedException()).when(cancelCriterion).checkCancelInProgress(any());

    persistenceInitialImageAdvisor.getAdvice(null);
  }

  @Test
  public void publishesListOfMissingMembers_whenWaitingForMissingMembers() {
    persistenceInitialImageAdvisor = persistenceInitialImageAdvisorWithDiskImage();

    setMembershipChangePollDuration(Duration.ofSeconds(0));
    Set<PersistentMemberID> offlineMembersToWaitFor = givenOfflineMembersToWaitFor(1);

    when(cacheDistributionAdvisor.adviseInitialImage(null, true))
        .thenReturn(adviceWithReplicates(0), adviceWithReplicates(1));

    persistenceInitialImageAdvisor.getAdvice(null);

    InOrder inOrder = inOrder(persistenceAdvisor);
    inOrder.verify(persistenceAdvisor, times(1)).setWaitingOnMembers(isNotNull(),
        eq(offlineMembersToWaitFor));
    inOrder.verify(persistenceAdvisor, times(1)).setWaitingOnMembers(isNull(), isNull());
    inOrder.verify(persistenceAdvisor, times(0)).setWaitingOnMembers(any(), any());
  }

  private Set<PersistentMemberID> givenOfflineMembersToWaitFor(int memberCount) {
    HashSet<PersistentMemberID> offlineMembersToWaitFor =
        IntStream.range(0, memberCount).mapToObj(i -> persistentMemberID("offline member " + i))
            .distinct().collect(toCollection(HashSet::new));
    Set<PersistentMemberID> membersToWaitFor = new HashSet<>(offlineMembersToWaitFor);

    when(persistenceAdvisor.getPersistedOnlineOrEqualMembers()).thenReturn(offlineMembersToWaitFor);
    when(persistenceAdvisor.getMembersToWaitFor(any(), any())).thenAnswer(invocation -> {
      Set<PersistentMemberID> previouslyOnlineMembers = invocation.getArgument(0);
      Set<PersistentMemberID> offlineMembers = invocation.getArgument(1);
      offlineMembers.addAll(previouslyOnlineMembers);
      return membersToWaitFor;
    });

    return offlineMembersToWaitFor;
  }

  private PersistenceInitialImageAdvisor persistenceInitialImageAdvisorWithDiskImage() {
    boolean hasDiskImage = true;
    return persistenceInitialImageAdvisor(hasDiskImage);
  }

  private PersistenceInitialImageAdvisor persistenceInitialImageAdvisor(boolean hasDiskImage) {
    return new PersistenceInitialImageAdvisor(persistenceAdvisor, "short disk store ID",
        "region path", cacheDistributionAdvisor, hasDiskImage);
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
    return IntStream.range(0, count).mapToObj(i -> internalDistributedMember(namePrefix + ' ' + i))
        .collect(toSet());
  }

  private static InternalDistributedMember internalDistributedMember(String name) {
    return mock(InternalDistributedMember.class, name);
  }

  private static PersistentMemberID persistentMemberID(String name) {
    return mock(PersistentMemberID.class, name);
  }

  private static void setMembershipChangePollDuration(Duration timeout) {
    System.setProperty("geode." + SystemPropertyHelper.PERSISTENT_VIEW_RETRY_TIMEOUT_SECONDS,
        String.valueOf(timeout.getSeconds()));
  }
}
