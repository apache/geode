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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VMRegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;

public class InitialImageOperationTest {

  private ClusterDistributionManager dm;
  private String path;
  private LocalRegion region;
  private InternalCache cache;
  private InitialImageOperation.RequestImageMessage message;
  private DistributedRegion distributedRegion;
  private InternalDistributedMember lostMember;
  private VersionSource versionSource;

  @Before
  public void setUp() {
    path = "path";

    cache = mock(InternalCache.class);
    dm = mock(ClusterDistributionManager.class);
    region = mock(LocalRegion.class);
    message = spy(new InitialImageOperation.RequestImageMessage());
    distributedRegion = mock(DistributedRegion.class);
    lostMember = mock(InternalDistributedMember.class);
    versionSource = mock(VersionSource.class);

    when(dm.getExistingCache()).thenReturn(cache);
    when(cache.getRegion(path)).thenReturn(region);
    when(region.isInitialized()).thenReturn(true);
    when(region.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
  }

  @Test
  public void getsRegionFromCacheFromDM() {
    LocalRegion value = InitialImageOperation.getGIIRegion(dm, path, false);
    assertThat(value).isSameAs(region);
  }

  @Test
  public void processRequestImageMessageWillSendFailureMessageIfGotCancelException() {
    message.regionPath = "regionPath";
    when(dm.getExistingCache()).thenThrow(new CacheClosedException());

    message.process(dm);

    verify(message).sendFailureMessage(eq(dm), eq(null));
  }

  @Test
  public void scheduleSynchronizeForLostMemberIsInvokedIfRegionHasNotScheduledOrDoneSynchronization() {
    when(distributedRegion.setRegionSynchronizedWithIfNotScheduled(versionSource)).thenReturn(true);

    message.synchronizeIfNotScheduled(distributedRegion, lostMember, versionSource);

    verify(distributedRegion).scheduleSynchronizeForLostMember(lostMember, versionSource, 0);
  }

  @Test
  public void synchronizeForLostMemberIsNotInvokedIfRegionHasScheduledOrDoneSynchronization() {
    when(distributedRegion.setRegionSynchronizedWithIfNotScheduled(versionSource))
        .thenReturn(false);

    message.synchronizeIfNotScheduled(distributedRegion, lostMember, versionSource);

    verify(distributedRegion, never()).scheduleSynchronizeForLostMember(lostMember, versionSource,
        0);
  }

  @Test
  public void processChunkDoesNotThrowIfDiskVersionTagMemberIDIsNull()
      throws IOException, ClassNotFoundException {
    ImageState imgState = mock(ImageState.class);
    when(distributedRegion.getImageState()).thenReturn(imgState);
    CachePerfStats stats = mock(CachePerfStats.class);
    when(distributedRegion.getCachePerfStats()).thenReturn(stats);
    RegionMap regionMap = mock(RegionMap.class);
    InitialImageOperation operation = spy(new InitialImageOperation(distributedRegion, regionMap));

    DiskVersionTag versionTag = spy(new DiskVersionTag());
    doReturn(null).when(versionTag).getMemberID();

    InitialImageOperation.Entry entry = mock(InitialImageOperation.Entry.class);
    when(entry.getVersionTag()).thenReturn(versionTag);
    List<InitialImageOperation.Entry> entries = new ArrayList<>();
    entries.add(entry);

    InternalDistributedMember member = mock(InternalDistributedMember.class);
    assertThat(operation.processChunk(entries, member)).isFalse();

    verify(versionTag).replaceNullIDs(member);
  }

  @Test
  public void processReceivedRVVShouldRemoveDepartedMembersFromRVV() {
    InternalDistributedMember server1 = new InternalDistributedMember("host1", 101);
    InternalDistributedMember server2 = new InternalDistributedMember("host2", 102);
    InternalDistributedMember server3 = new InternalDistributedMember("host3", 103);
    InternalDistributedMember server4 = new InternalDistributedMember("host4", 104);
    when(distributedRegion.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(distributedRegion.getVersionMember()).thenReturn(server1);

    RegionEntry re1 = mock(RegionEntry.class);
    RegionEntry re2 = mock(RegionEntry.class);
    RegionEntry re3 = mock(RegionEntry.class);
    ArrayList<RegionEntry> entries = new ArrayList<>();
    entries.add(re1);
    entries.add(re2);
    entries.add(re3);
    Iterator<RegionEntry> iterator = entries.iterator();
    when(distributedRegion.getBestIterator(false)).thenReturn(iterator);
    VersionStamp stamp1 = mock(VersionStamp.class);
    VersionStamp stamp2 = mock(VersionStamp.class);
    VersionStamp stamp3 = mock(VersionStamp.class);
    when(re1.getVersionStamp()).thenReturn(stamp1);
    when(re2.getVersionStamp()).thenReturn(stamp2);
    when(re3.getVersionStamp()).thenReturn(stamp3);
    when(stamp1.getMemberID()).thenReturn(server1);
    when(stamp2.getMemberID()).thenReturn(server2);
    when(stamp3.getMemberID()).thenReturn(server3);

    RegionMap regionMap = mock(RegionMap.class);
    InitialImageOperation operation = spy(new InitialImageOperation(distributedRegion, regionMap));

    RegionVersionVector recoveredRVV = new VMRegionVersionVector(server1);
    recoveredRVV.recordVersion(server1, 1);
    recoveredRVV.recordVersion(server2, 1);
    recoveredRVV.recordVersion(server3, 1);
    recoveredRVV.recordVersion(server4, 1);
    recoveredRVV.recordGCVersion(server2, 1);
    recoveredRVV.recordGCVersion(server3, 1);
    recoveredRVV.recordGCVersion(server4, 1);
    recoveredRVV.memberDeparted(null, server3, true);
    recoveredRVV.memberDeparted(null, server4, true);
    assertThat(recoveredRVV.isDepartedMember(server3)).isTrue();
    assertThat(recoveredRVV.isDepartedMember(server4)).isTrue();
    assertThat(recoveredRVV.getMemberToVersion().size()).isEqualTo(4);
    assertThat(recoveredRVV.getMemberToGCVersion().size()).isEqualTo(3);

    RegionVersionVector receivedRVV = new VMRegionVersionVector(server2);
    receivedRVV.recordVersion(server1, 1);
    receivedRVV.recordVersion(server2, 1);
    receivedRVV.recordVersion(server2, 2);
    receivedRVV.recordVersion(server3, 1);
    receivedRVV.recordVersion(server4, 1);
    receivedRVV.recordGCVersion(server2, 1);
    receivedRVV.recordGCVersion(server3, 1);
    receivedRVV.recordGCVersion(server4, 1);
    receivedRVV.memberDeparted(null, server3, true);
    receivedRVV.memberDeparted(null, server4, true);
    assertThat(receivedRVV.isDepartedMember(server3)).isTrue();
    assertThat(receivedRVV.isDepartedMember(server4)).isTrue();
    assertThat(receivedRVV.getMemberToVersion().size()).isEqualTo(4);
    assertThat(receivedRVV.getMemberToGCVersion().size()).isEqualTo(3);

    RegionVersionVector remoteRVV = receivedRVV.getCloneForTransmission();
    System.out
        .println(recoveredRVV.getMemberToVersion() + ":" + recoveredRVV.getMemberToGCVersion());
    System.out.println(receivedRVV.getMemberToVersion() + ":" + receivedRVV.getMemberToGCVersion());
    System.out.println(remoteRVV.getMemberToVersion() + ":" + remoteRVV.getMemberToGCVersion());

    operation.processReceivedRVV(remoteRVV, recoveredRVV, receivedRVV);
    assertThat(receivedRVV.getMemberToVersion().size()).isEqualTo(3);
    assertThat(receivedRVV.getMemberToGCVersion().size()).isEqualTo(2);
    assertThat(recoveredRVV.getMemberToVersion().size()).isEqualTo(3);
    assertThat(recoveredRVV.getMemberToGCVersion().size()).isEqualTo(2);
    assertThat(remoteRVV.getMemberToVersion().size()).isEqualTo(3);
    assertThat(remoteRVV.getMemberToGCVersion().size()).isEqualTo(2);
    assertThat(recoveredRVV.getMemberToVersion().containsKey(server3)).isTrue();
    assertThat(recoveredRVV.getMemberToVersion().containsKey(server4)).isFalse();
    assertThat(recoveredRVV.getMemberToGCVersion().containsKey(server3)).isTrue();
    assertThat(recoveredRVV.getMemberToGCVersion().containsKey(server4)).isFalse();
    assertThat(receivedRVV.getMemberToVersion().containsKey(server3)).isTrue();
    assertThat(receivedRVV.getMemberToVersion().containsKey(server4)).isFalse();
    assertThat(receivedRVV.getMemberToGCVersion().containsKey(server3)).isTrue();
    assertThat(receivedRVV.getMemberToGCVersion().containsKey(server4)).isFalse();
  }
}
