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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;

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
  public void shouldRemoveDepartedMembersFromRVVForNonPersistentRegion()
      throws IOException, ClassNotFoundException {
    InternalDistributedMember server1 = new InternalDistributedMember("host1", 101);
    InternalDistributedMember server2 = new InternalDistributedMember("host2", 102);
    InternalDistributedMember server3 = new InternalDistributedMember("host3", 103);
    InternalDistributedMember server4 = new InternalDistributedMember("host4", 104);
    ImageState imageState = mock(ImageState.class);

    RegionVersionVector recoveredRVV = new VMRegionVersionVector(server1);
    recoveredRVV.recordVersion(server1, 1);
    recoveredRVV.recordVersion(server2, 1);
    recoveredRVV.recordVersion(server3, 1);
    recoveredRVV.recordVersion(server4, 1);
    recoveredRVV.recordGCVersion(server2, 1);
    recoveredRVV.recordGCVersion(server3, 1);
    recoveredRVV.recordGCVersion(server4, 1);

    // there will be re3 from server3
    recoveredRVV.memberDeparted(null, server3, true);

    // there won't be any entry from server4, so it will be removed from MemberToVersion map after
    // GII
    recoveredRVV.memberDeparted(null, server4, true);

    assertThat(recoveredRVV.isDepartedMember(server3)).isTrue();
    assertThat(recoveredRVV.isDepartedMember(server4)).isTrue();
    assertThat(recoveredRVV.getMemberToVersion().size()).isEqualTo(4);
    assertThat(recoveredRVV.getMemberToGCVersion().size()).isEqualTo(3);
    InitialImageOperation.Entry re3 = mock(InitialImageOperation.Entry.class);
    VersionTag tag3 = VMVersionTag.create(server3);
    when(re3.getVersionTag()).thenReturn(tag3);
    ArrayList<InitialImageOperation.Entry> entries = new ArrayList<>();
    entries.add(re3);

    CachePerfStats stats = mock(CachePerfStats.class);
    doNothing().when(stats).incGetInitialImageKeysReceived();
    when(distributedRegion.getCachePerfStats()).thenReturn(stats);
    when(distributedRegion.getImageState()).thenReturn(imageState);
    when(distributedRegion.getVersionMember()).thenReturn(server1);
    when(distributedRegion.getDiskRegion()).thenReturn(null);
    when(distributedRegion.isDestroyed()).thenReturn(false);
    when(distributedRegion.getVersionVector()).thenReturn(recoveredRVV);
    when(imageState.getClearRegionFlag()).thenReturn(false);

    InternalDistributedMember giiProvider = mock(InternalDistributedMember.class);
    RegionMap regionMap = mock(RegionMap.class);
    InitialImageOperation operation = spy(new InitialImageOperation(distributedRegion, regionMap));

    when(distributedRegion.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    assertThat(operation.processChunk(entries, giiProvider)).isTrue();
    assertThat(recoveredRVV.getDepartedMembersSet().size()).isEqualTo(1);
    assertThat(recoveredRVV.getMemberToVersion().size()).isEqualTo(3);
    assertThat(recoveredRVV.getMemberToGCVersion().size()).isEqualTo(2);
    assertThat(recoveredRVV.getMemberToVersion().containsKey(server3)).isTrue();
    assertThat(recoveredRVV.getMemberToVersion().containsKey(server4)).isFalse();
    assertThat(recoveredRVV.getMemberToGCVersion().containsKey(server3)).isTrue();
    assertThat(recoveredRVV.getMemberToGCVersion().containsKey(server4)).isFalse();
  }

  @Test
  public void shouldNotRemoveDepartedMembersFromRVVForPersistentRegion()
      throws IOException, ClassNotFoundException {
    InternalDistributedMember server1 = new InternalDistributedMember("host1", 101);
    InternalDistributedMember server2 = new InternalDistributedMember("host2", 102);
    InternalDistributedMember server3 = new InternalDistributedMember("host3", 103);
    InternalDistributedMember server4 = new InternalDistributedMember("host4", 104);
    ImageState imageState = mock(ImageState.class);

    RegionVersionVector recoveredRVV = new VMRegionVersionVector(server1);
    recoveredRVV.recordVersion(server1, 1);
    recoveredRVV.recordVersion(server2, 1);
    recoveredRVV.recordVersion(server3, 1);
    recoveredRVV.recordVersion(server4, 1);
    recoveredRVV.recordGCVersion(server2, 1);
    recoveredRVV.recordGCVersion(server3, 1);
    recoveredRVV.recordGCVersion(server4, 1);
    recoveredRVV = spy(recoveredRVV);

    assertThat(recoveredRVV.getMemberToVersion().size()).isEqualTo(4);
    assertThat(recoveredRVV.getMemberToGCVersion().size()).isEqualTo(3);
    InitialImageOperation.Entry re3 = mock(InitialImageOperation.Entry.class);
    VersionTag tag3 = VMVersionTag.create(server3);
    when(re3.getVersionTag()).thenReturn(tag3);
    ArrayList<InitialImageOperation.Entry> entries = new ArrayList<>();
    entries.add(re3);

    CachePerfStats stats = mock(CachePerfStats.class);
    doNothing().when(stats).incGetInitialImageKeysReceived();
    DiskRegion diskRegion = mock(DiskRegion.class);
    when(distributedRegion.getCachePerfStats()).thenReturn(stats);
    when(distributedRegion.getImageState()).thenReturn(imageState);
    when(distributedRegion.getVersionMember()).thenReturn(server1);
    when(distributedRegion.getDiskRegion()).thenReturn(diskRegion);
    when(distributedRegion.isDestroyed()).thenReturn(false);
    when(distributedRegion.getVersionVector()).thenReturn(recoveredRVV);
    when(imageState.getClearRegionFlag()).thenReturn(false);

    InternalDistributedMember giiProvider = mock(InternalDistributedMember.class);
    RegionMap regionMap = mock(RegionMap.class);
    InitialImageOperation operation = spy(new InitialImageOperation(distributedRegion, regionMap));

    when(distributedRegion.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_REPLICATE);
    assertThat(operation.processChunk(entries, giiProvider)).isTrue();
    verify(recoveredRVV, never()).removeOldMembers(any());
  }
}
