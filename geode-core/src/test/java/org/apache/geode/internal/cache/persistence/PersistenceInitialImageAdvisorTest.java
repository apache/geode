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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PersistenceInitialImageAdvisorTest {

  private InternalPersistenceAdvisor persistenceAdvisor;
  private CacheDistributionAdvisor cacheDistributionAdvisor;
  private PersistenceInitialImageAdvisor persistenceInitialImageAdvisor;

  @Before
  public void setup() {
    cacheDistributionAdvisor = mock(CacheDistributionAdvisor.class, RETURNS_DEEP_STUBS);
    when(cacheDistributionAdvisor.getDistributionManager().getConfig().getAckWaitThreshold())
        .thenReturn(15);

    persistenceAdvisor = mock(InternalPersistenceAdvisor.class);
    when(persistenceAdvisor.getCacheDistributionAdvisor()).thenReturn(cacheDistributionAdvisor);

    persistenceInitialImageAdvisor = new PersistenceInitialImageAdvisor(persistenceAdvisor,
        "short disk store ID", "region path", cacheDistributionAdvisor, true);
  }

  @Test
  public void publishesListOfMissingMembersWhenWaitingForMissingMembers() {
    Set<PersistentMemberID> offlineMembersToWaitFor = givenOfflineMembersToWaitFor(1);

    when(cacheDistributionAdvisor.adviseInitialImage(null, true))
        .thenReturn(adviceWithReplicates(0), adviceWithReplicates(1));

    persistenceInitialImageAdvisor.getAdvice(null);

    verify(persistenceAdvisor, times(2)).setWaitingOnMembers(any(), any());

    InOrder inOrder = inOrder(persistenceAdvisor);
    inOrder.verify(persistenceAdvisor, times(1)).setWaitingOnMembers(isNotNull(),
        eq(offlineMembersToWaitFor));
    inOrder.verify(persistenceAdvisor, times(1)).setWaitingOnMembers(isNull(), isNull());
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

  private static InternalDistributedMember internalDistributedMember(String name) {
    return mock(InternalDistributedMember.class, name);
  }

  private static PersistentMemberID persistentMemberID(String name) {
    return mock(PersistentMemberID.class, name);
  }

  private static InitialImageAdvice adviceWithReplicates(int replicateCount) {
    Set<InternalDistributedMember> replicates = IntStream.range(0, replicateCount)
        .mapToObj(i -> internalDistributedMember("replicate " + i)).collect(toSet());
    return new InitialImageAdvice(replicates, emptySet(), emptySet(), emptySet(), emptySet(),
        emptySet(), emptyMap());
  }
}
