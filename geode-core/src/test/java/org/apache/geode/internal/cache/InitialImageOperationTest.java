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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.VersionSource;

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
}
