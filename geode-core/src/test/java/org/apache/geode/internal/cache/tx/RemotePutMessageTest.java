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
package org.apache.geode.internal.cache.tx;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.test.junit.categories.UnitTest;

@PowerMockIgnore("*.UnitTest")
@RunWith(PowerMockRunner.class)
@PrepareForTest(RemotePutMessage.class)
public class RemotePutMessageTest {
  @Test
  public void testDistributeNotFailWithRegionDestroyedException() throws RemoteOperationException {
    EntryEventImpl event = mock(EntryEventImpl.class);
    DistributedRegion region = mock(DistributedRegion.class);
    CacheDistributionAdvisor advisor = mock(CacheDistributionAdvisor.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> replicates = new HashSet<>(Arrays.asList(member));
    RemotePutMessage.RemotePutResponse response = mock(RemotePutMessage.RemotePutResponse.class);
    Object expectedOldValue = new Object();

    when(event.getRegion()).thenReturn(region);
    when(region.getCacheDistributionAdvisor()).thenReturn(advisor);
    when(advisor.adviseInitializedReplicates()).thenReturn(replicates);
    when(response.waitForResult()).thenThrow(new RegionDestroyedException("", ""));

    PowerMockito.mockStatic(RemotePutMessage.class);
    PowerMockito
        .when(RemotePutMessage.distribute(event, 1, false, false, expectedOldValue, false, false))
        .thenCallRealMethod();
    PowerMockito.when(RemotePutMessage.send(member, region, event, 1, false, false,
        expectedOldValue, false, false, false)).thenReturn(response);

    RemotePutMessage.distribute(event, 1, false, false, expectedOldValue, false, false);
  }
}
