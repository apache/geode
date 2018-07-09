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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.persistence.PersistentStateQueryMessage.PersistentStateQueryReplyProcessor;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PersistenceAdvisorImplTest {

  public static final long TIME_STAMP_1 = 1530300988488L;
  public static final long TIME_STAMP_2 = 1530301340401L;
  public static final long TIME_STAMP_3 = 1530301598541L;
  public static final long TIME_STAMP_4 = 1530301598616L;
  private CacheDistributionAdvisor cacheDistributionAdvisor;
  private PersistentMemberView persistentMemberView;
  private PersistentStateQueryMessageSenderFactory persistentStateQueryMessageSenderFactory;
  private PersistentStateQueryMessage persistentStateQueryMessage;
  private PersistentStateQueryResults persistentStateQueryResults;

  private PersistenceAdvisorImpl persistenceAdvisorImpl;

  private int diskStoreIDIndex = 92837487; // some random number

  @Before
  public void setUp() throws Exception {
    cacheDistributionAdvisor = mock(CacheDistributionAdvisor.class);
    persistentMemberView = mock(DiskRegion.class);
    persistentStateQueryMessageSenderFactory = mock(PersistentStateQueryMessageSenderFactory.class);
    persistentStateQueryMessage = mock(PersistentStateQueryMessage.class);
    persistentStateQueryResults = mock(PersistentStateQueryResults.class);

    when(persistentStateQueryMessageSenderFactory.createPersistentStateQueryReplyProcessor(any(),
        any())).thenReturn(mock(PersistentStateQueryReplyProcessor.class));
    when(persistentStateQueryMessageSenderFactory.createPersistentStateQueryMessage(any(), any(),
        any(), anyInt())).thenReturn(persistentStateQueryMessage);
    when(persistentStateQueryMessage.send(any(), any(), any()))
        .thenReturn(persistentStateQueryResults);

    persistenceAdvisorImpl =
        new PersistenceAdvisorImpl(cacheDistributionAdvisor, null, persistentMemberView, null, null,
            null, persistentStateQueryMessageSenderFactory);
  }

  /**
   * GEODE-5402: This test creates a scenario where a member has two versions (based on timeStamp)
   * of another member. The call to getMembersToWaitFor should return that we wait for neither of
   * them.
   */
  @Test
  public void getMembersToWaitForRemovesAllMembersWhenDiskStoreListedTwice() {
    DiskStoreID diskStoreID = getNewDiskStoreID();
    InternalDistributedMember member = createInternalDistributedMember();

    Set<PersistentMemberID> previouslyOnlineMembers = new HashSet<>();
    Map<InternalDistributedMember, PersistentMemberState> stateOnPeers = new HashMap<>();
    Map<InternalDistributedMember, PersistentMemberID> persistentIds = new HashMap<>();
    Map<InternalDistributedMember, PersistentMemberID> initializingIds = new HashMap<>();
    Map<InternalDistributedMember, DiskStoreID> diskStoreIds = new HashMap<>();

    previouslyOnlineMembers.add(createPersistentMemberID(diskStoreID, TIME_STAMP_1));
    previouslyOnlineMembers.add(createPersistentMemberID(diskStoreID, TIME_STAMP_3));

    stateOnPeers.put(member, PersistentMemberState.ONLINE);
    persistentIds.put(member, createPersistentMemberID(diskStoreID, TIME_STAMP_2));
    initializingIds.put(member, createPersistentMemberID(diskStoreID, TIME_STAMP_3));
    diskStoreIds.put(member, diskStoreID);

    when(cacheDistributionAdvisor.adviseGeneric()).thenReturn(createMemberSet(member));
    when(persistentMemberView.getMyPersistentID())
        .thenReturn(createPersistentMemberID(getNewDiskStoreID(),
            TIME_STAMP_4));
    when(persistentStateQueryResults.getDiskStoreIds()).thenReturn(diskStoreIds);
    when(persistentStateQueryResults.getInitializingIds()).thenReturn(initializingIds);
    when(persistentStateQueryResults.getPersistentIds()).thenReturn(persistentIds);
    when(persistentStateQueryResults.getStateOnPeers()).thenReturn(stateOnPeers);

    Set<PersistentMemberID> membersToWaitFor =
        persistenceAdvisorImpl.getMembersToWaitFor(previouslyOnlineMembers, new HashSet<>());

    assertThat(membersToWaitFor).isEmpty();
  }

  private Set<InternalDistributedMember> createMemberSet(InternalDistributedMember... member) {
    Set<InternalDistributedMember> members = new HashSet<>();
    members.addAll(Arrays.asList(member));
    return members;
  }

  private PersistentMemberID createPersistentMemberID(DiskStoreID diskStoreID, long timeStamp) {
    return new PersistentMemberID(diskStoreID, null, null, timeStamp, (short) 0);
  }

  private InternalDistributedMember createInternalDistributedMember() {
    return mock(InternalDistributedMember.class);
  }

  private DiskStoreID getNewDiskStoreID() {
    UUID uuid = new UUID(diskStoreIDIndex++, diskStoreIDIndex++);
    return new DiskStoreID(uuid);
  }
}
