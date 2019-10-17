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


import static org.apache.geode.test.concurrency.Utilities.availableProcessors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MemberAttributes;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;


@RunWith(ConcurrentTestRunner.class)
public class CacheDistributionAdvisorConcurrentTest {
  private final int count = availableProcessors() * 2;

  @Test
  public void getAdviseAllEventsOrCachedForConcurrentUpdateShouldSucceed(
      ParallelExecutor executor) throws Exception {

    DistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = createCacheProfile();
    advisor.putProfile(profile, true);

    executor.inParallel(() -> {
      ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();
    }, count);
    executor.execute();

    assertTrue(((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached()
        .contains(profile.getDistributedMember()));
    assertEquals(((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached().size(), 1);

  }

  public void getAdviseUpdateForConcurrentUpdateShouldSucceed(
      ParallelExecutor executor) throws Exception {


    EntryEventImpl event = new EntryEventImpl();
    event.setNewValue(null);
    event.setOperation(Operation.CREATE);

    DistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = createCacheProfile();
    advisor.putProfile(profile, true);

    executor.inParallel(() -> {
      ((CacheDistributionAdvisor) advisor).adviseUpdate(event);
    }, count);
    executor.execute();

    assertTrue(((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached()
        .contains(profile.getDistributedMember()));
    assertEquals(((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached().size(), 1);

  }

  private DistributionAdvisor createCacheDistributionAdvisor() {
    CacheDistributionAdvisee advisee = mock(CacheDistributionAdvisee.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(advisee.getCancelCriterion()).thenReturn(cancelCriterion);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(advisee.getDistributionManager()).thenReturn(distributionManager);
    CacheDistributionAdvisor result =
        CacheDistributionAdvisor.createCacheDistributionAdvisor(advisee);
    when(advisee.getDistributionAdvisor()).thenReturn(result);
    return result;
  }

  private CacheProfile createCacheProfile() throws UnknownHostException {
    InternalDistributedMember member =
        new InternalDistributedMember(InetAddress.getLocalHost(), 0, false,
            false, MemberAttributes.DEFAULT);
    return new CacheProfile(member, 1);
  }
}
