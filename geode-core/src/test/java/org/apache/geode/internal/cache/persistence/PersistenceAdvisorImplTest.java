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

import static java.lang.System.lineSeparator;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.persistence.PersistentStateQueryMessage.PersistentStateQueryReplyProcessor;
import org.apache.geode.internal.process.StartupStatus;
import org.apache.geode.internal.util.TransformUtils.CollectionTransformer;
import org.apache.geode.internal.util.Transformer;

public class PersistenceAdvisorImplTest {

  private static final long TIME_STAMP_1 = 1530300988488L;
  private static final long TIME_STAMP_2 = 1530301340401L;
  private static final long TIME_STAMP_3 = 1530301598541L;
  private static final long TIME_STAMP_4 = 1530301598616L;

  private static final String SET_POSITION_1 = "a";
  private static final String SET_POSITION_2 = "b";
  private static final String SET_POSITION_3 = "c";
  private static final String SET_POSITION_4 = "d";
  private static final String SET_POSITION_5 = "e";
  private static final String SET_POSITION_6 = "f";
  private static final String SET_POSITION_7 = "g";

  private CacheDistributionAdvisor cacheDistributionAdvisor;
  private PersistentMemberView persistentMemberView;
  private PersistentStateQueryResults persistentStateQueryResults;

  private PersistenceAdvisorImpl persistenceAdvisorImpl;

  private int diskStoreIDIndex = 92837487; // some random number

  @Before
  public void setUp() throws Exception {
    cacheDistributionAdvisor = mock(CacheDistributionAdvisor.class);
    persistentMemberView = mock(DiskRegion.class);
    PersistentStateQueryMessageSenderFactory queryMessageSenderFactory =
        mock(PersistentStateQueryMessageSenderFactory.class);
    PersistentStateQueryMessage queryMessage =
        mock(PersistentStateQueryMessage.class);
    persistentStateQueryResults = mock(PersistentStateQueryResults.class);

    when(queryMessageSenderFactory.createPersistentStateQueryReplyProcessor(any(), any()))
        .thenReturn(mock(PersistentStateQueryReplyProcessor.class));
    when(queryMessageSenderFactory.createPersistentStateQueryMessage(any(), any(), any(), anyInt()))
        .thenReturn(queryMessage);
    when(queryMessage.send(any(), any(), any()))
        .thenReturn(persistentStateQueryResults);

    persistenceAdvisorImpl =
        new PersistenceAdvisorImpl(cacheDistributionAdvisor, null, persistentMemberView, null, null,
            null, mock(StartupStatus.class), mock(Transformer.class),
            mock(CollectionTransformer.class), queryMessageSenderFactory);
  }

  /**
   * GEODE-5402: This test creates a scenario where a member has two versions (based on timeStamp)
   * of another member. The call to getMembersToWaitFor should return that we wait for neither of
   * them.
   */
  @Test
  public void getMembersToWaitForRemovesAllMembersWhenDiskStoreListedTwice() {
    DiskStoreID diskStoreID = getNewDiskStoreID();
    Set<PersistentMemberID> previouslyOnlineMembers = new HashSet<>();
    previouslyOnlineMembers.add(createPersistentMemberID(diskStoreID, TIME_STAMP_1));
    previouslyOnlineMembers.add(createPersistentMemberID(diskStoreID, TIME_STAMP_3));

    getMembersToWaitForRemovesAllMembers(diskStoreID, previouslyOnlineMembers);
  }

  @Test
  public void getMembersToWaitForDoesNotWaitForMemberWhoIsNotInitialized() {
    DiskStoreID diskStoreID = getNewDiskStoreID();
    Set<PersistentMemberID> previouslyOnlineMembers = new HashSet<>();
    previouslyOnlineMembers.add(createPersistentMemberID(diskStoreID, TIME_STAMP_1));

    getMembersToWaitForRemovesAllMembers(diskStoreID, previouslyOnlineMembers);
  }

  @Test
  public void removeOlderMembersHandlesEmptySet() {
    Set<PersistentMemberID> aSet = new HashSet<>();

    persistenceAdvisorImpl.removeOlderMembers(aSet);

    assertThat(aSet).isEmpty();
  }

  @Test
  public void removeOlderMembersWithEqualTimeStampsInDifferentDiskStores() {
    long timeStamp = 239874; // anything
    // all diskStoreIDs in set are different so nothing to remove
    Set<PersistentMemberID> aSet = new HashSet<>();
    aSet.add(createPersistentMemberID(getNewDiskStoreID(), timeStamp));
    aSet.add(createPersistentMemberID(getNewDiskStoreID(), timeStamp));
    aSet.add(createPersistentMemberID(getNewDiskStoreID(), timeStamp));
    aSet.add(createPersistentMemberID(getNewDiskStoreID(), timeStamp));

    persistenceAdvisorImpl.removeOlderMembers(aSet);

    assertThat(aSet).hasSize(4);
  }

  @Test
  public void removeOlderMembersWhenFirstInSetIsOlder() {
    DiskStoreID diskStoreID = getNewDiskStoreID();
    Set<PersistentMemberID> aSet = new TreeSet<>(new PersistentMemberIDComparator());
    aSet.add(createPersistentMemberID(diskStoreID, 1, SET_POSITION_1));
    PersistentMemberID newest = createPersistentMemberID(diskStoreID, 2, SET_POSITION_2);
    aSet.add(newest);

    persistenceAdvisorImpl.removeOlderMembers(aSet);

    assertThat(aSet).containsExactly(newest);
  }

  @Test
  public void removeOlderMembersWhenFirstInSetIsNewer() {
    DiskStoreID diskStoreID = getNewDiskStoreID();
    Set<PersistentMemberID> aSet = new TreeSet<>(new PersistentMemberIDComparator());
    PersistentMemberID newest = createPersistentMemberID(diskStoreID, 2, SET_POSITION_1);
    aSet.add(newest);
    aSet.add(createPersistentMemberID(diskStoreID, 1, SET_POSITION_2));

    persistenceAdvisorImpl.removeOlderMembers(aSet);

    assertThat(aSet).containsExactly(newest);
  }

  @Test
  public void removeOlderMembersWithMultipleRemovals() {
    DiskStoreID diskStoreID1 = getNewDiskStoreID();
    DiskStoreID diskStoreID2 = getNewDiskStoreID();
    DiskStoreID diskStoreID3 = getNewDiskStoreID();
    Set<PersistentMemberID> aSet = new TreeSet<>(new PersistentMemberIDComparator());
    PersistentMemberID id_1 = createPersistentMemberID(diskStoreID1, 10, SET_POSITION_1);
    PersistentMemberID id_2 = createPersistentMemberID(diskStoreID2, 10, SET_POSITION_2);
    PersistentMemberID id_3 = createPersistentMemberID(diskStoreID3, 40, SET_POSITION_3);
    PersistentMemberID id_4 = createPersistentMemberID(diskStoreID3, 10, SET_POSITION_4);
    PersistentMemberID id_5 = createPersistentMemberID(diskStoreID2, 50, SET_POSITION_5);
    PersistentMemberID id_6 = createPersistentMemberID(diskStoreID2, 70, SET_POSITION_6);
    PersistentMemberID id_7 = createPersistentMemberID(diskStoreID2, 60, SET_POSITION_7);
    aSet.add(id_1);
    aSet.add(id_2);
    aSet.add(id_3);
    aSet.add(id_4);
    aSet.add(id_5);
    aSet.add(id_6);
    aSet.add(id_7);

    persistenceAdvisorImpl.removeOlderMembers(aSet);

    assertThat(aSet).containsExactly(id_1, id_3, id_6);
  }

  @Test
  public void prepareNewMemberRemovesOldPersistentMemberId() {
    InternalDistributedMember sender = mock(InternalDistributedMember.class);
    PersistentMemberID oldId = mock(PersistentMemberID.class);
    PersistentMemberID newId = mock(PersistentMemberID.class);
    when(cacheDistributionAdvisor.containsId(sender)).thenReturn(true);
    PersistenceAdvisorImpl spy = spy(persistenceAdvisorImpl);
    doNothing().when(spy).memberRemoved(oldId, false);

    spy.prepareNewMember(sender, oldId, newId);

    verify(persistentMemberView).memberOnline(newId);
    verify(spy).memberRemoved(oldId, false);
  }

  @Test
  public void waitingForMembersMessage_logsMessage1_whenHasOfflineMembersWaitingFor() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String regionPath = "/region";
    DiskRegionStats diskRegionStats = mock(DiskRegionStats.class);
    PersistentMemberManager persistentMemberManager = mock(PersistentMemberManager.class);
    StartupStatus startupStatus = mock(StartupStatus.class);
    PersistentStateQueryMessageSenderFactory persistentStateQueryMessageSenderFactory =
        mock(PersistentStateQueryMessageSenderFactory.class);

    PersistentMemberID missingPersistentId = mock(PersistentMemberID.class);
    Set<PersistentMemberID> allMembersToWaitFor = singleton(missingPersistentId);
    Set<PersistentMemberID> offlineMembersToWaitFor = singleton(missingPersistentId);

    String transformedMissingPersistentId = "myId";

    PersistenceAdvisorImpl persistenceAdvisor = new PersistenceAdvisorImpl(cacheDistributionAdvisor,
        distributedLockService, persistentMemberView, regionPath, diskRegionStats,
        persistentMemberManager, startupStatus, id -> transformedMissingPersistentId,
        mock(CollectionTransformer.class), persistentStateQueryMessageSenderFactory);

    persistenceAdvisor.setWaitingOnMembers(allMembersToWaitFor, offlineMembersToWaitFor);

    persistenceAdvisor.logWaitingForMembers();

    ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
    verify(startupStatus).startup(messageCaptor.capture(), any());

    assertThat(messageCaptor.getValue())
        .contains("Region " + regionPath
            + " has potentially stale data. It is waiting for another member to recover the latest data.")
        .contains(lineSeparator() + "My persistent id:" + lineSeparator())
        .contains(transformedMissingPersistentId)
        .contains(lineSeparator() + "Members with potentially new data:" + lineSeparator())
        // skip validation of CollectionTransformer for now
        .contains(lineSeparator()
            + "Use the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.");
  }

  @Test
  public void waitingForMembersMessage_logsMessage2_whenNoOfflineMembersWaitingFor() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String regionPath = "/region";
    DiskRegionStats diskRegionStats = mock(DiskRegionStats.class);
    PersistentMemberManager persistentMemberManager = mock(PersistentMemberManager.class);
    StartupStatus startupStatus = mock(StartupStatus.class);
    PersistentStateQueryMessageSenderFactory persistentStateQueryMessageSenderFactory =
        mock(PersistentStateQueryMessageSenderFactory.class);

    String transformedMissingPersistentId = "myId";

    PersistenceAdvisorImpl persistenceAdvisor = new PersistenceAdvisorImpl(cacheDistributionAdvisor,
        distributedLockService, persistentMemberView, regionPath, diskRegionStats,
        persistentMemberManager, startupStatus, id -> transformedMissingPersistentId,
        mock(CollectionTransformer.class), persistentStateQueryMessageSenderFactory);

    persistenceAdvisor.setWaitingOnMembers(emptySet(), emptySet());

    persistenceAdvisor.logWaitingForMembers();

    ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
    verify(startupStatus).startup(messageCaptor.capture(), any());

    assertThat(messageCaptor.getValue())
        .contains("Region " + regionPath
            + " has potentially stale data. It is waiting for another online member to recover the latest data.")
        .contains(lineSeparator() + "My persistent id:" + lineSeparator())
        .contains(transformedMissingPersistentId)
        .contains(lineSeparator() + "Members with potentially new data:" + lineSeparator())
        // skip validation of CollectionTransformer for now
        .contains(lineSeparator()
            + "Use the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.");
  }

  @Test
  public void verifyPersistentStateListenerSetIsCleanedAfterClosing() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String regionPath = "/region";
    DiskRegionStats diskRegionStats = mock(DiskRegionStats.class);
    PersistentMemberManager persistentMemberManager = mock(PersistentMemberManager.class);
    StartupStatus startupStatus = mock(StartupStatus.class);
    PersistentStateQueryMessageSenderFactory persistentStateQueryMessageSenderFactory =
        mock(PersistentStateQueryMessageSenderFactory.class);

    String transformedMissingPersistentId = "myId";

    PersistenceAdvisorImpl persistenceAdvisor = new PersistenceAdvisorImpl(cacheDistributionAdvisor,
        distributedLockService, persistentMemberView, regionPath, diskRegionStats,
        persistentMemberManager, startupStatus, id -> transformedMissingPersistentId,
        mock(CollectionTransformer.class), persistentStateQueryMessageSenderFactory);

    PersistentStateListener listener1 = mock(PersistentStateListener.class);
    PersistentStateListener listener2 = mock(PersistentStateListener.class);

    persistenceAdvisor.addListener(listener1);
    persistenceAdvisor.addListener(listener2);

    assertThat(persistenceAdvisor.getPersistentStateListenerSet().size()).isEqualTo(2);

    persistenceAdvisor.close();
    assertThat(persistenceAdvisor.isClosed()).isTrue();
    assertThat(persistenceAdvisor.getPersistentStateListenerSet().isEmpty()).isTrue();
  }

  private void getMembersToWaitForRemovesAllMembers(DiskStoreID diskStoreID,
      Set<PersistentMemberID> previouslyOnlineMembers) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);

    when(cacheDistributionAdvisor.adviseGeneric())
        .thenReturn(createMemberSet(member));
    when(persistentMemberView.getMyPersistentID())
        .thenReturn(createPersistentMemberID(getNewDiskStoreID(), TIME_STAMP_4));
    when(persistentStateQueryResults.getDiskStoreIds())
        .thenReturn(singletonMap(member, diskStoreID));
    when(persistentStateQueryResults.getInitializingIds())
        .thenReturn(singletonMap(member, createPersistentMemberID(diskStoreID, TIME_STAMP_3)));
    when(persistentStateQueryResults.getPersistentIds())
        .thenReturn(singletonMap(member, createPersistentMemberID(diskStoreID, TIME_STAMP_2)));
    when(persistentStateQueryResults.getStateOnPeers())
        .thenReturn(singletonMap(member, PersistentMemberState.ONLINE));

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

  private PersistentMemberID createPersistentMemberID(DiskStoreID diskStoreID, long timeStamp,
      String name) {
    return new PersistentMemberID(diskStoreID, null, null, name, timeStamp, (short) 0);
  }

  private DiskStoreID getNewDiskStoreID() {
    UUID uuid = new UUID(diskStoreIDIndex++, diskStoreIDIndex++);
    return new DiskStoreID(uuid);
  }

  private static class PersistentMemberIDComparator implements Comparator<PersistentMemberID> {
    @Override
    public int compare(PersistentMemberID id1, PersistentMemberID id2) {
      return id1.getName().compareTo(id2.getName());
    }
  }
}
