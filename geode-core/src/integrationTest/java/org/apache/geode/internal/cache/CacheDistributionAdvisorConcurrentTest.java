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

import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.apache.geode.test.concurrency.Utilities.availableProcessors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.net.UnknownHostException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;

@RunWith(ConcurrentTestRunner.class)
public class CacheDistributionAdvisorConcurrentTest {

  private final int count = availableProcessors() * 2;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void getAdviseAllEventsOrCachedForConcurrentUpdateShouldSucceed(ParallelExecutor executor)
      throws Exception {

    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = createCacheProfile();
    advisor.putProfile(profile, true);

    executor.inParallel(advisor::adviseAllEventsOrCached, count);
    executor.execute();

    assertThat(advisor.adviseAllEventsOrCached())
        .contains(profile.getDistributedMember());
    assertThat(advisor.adviseAllEventsOrCached())
        .hasSize(1);
  }

  @Test
  public void getAdviseUpdateForConcurrentUpdateShouldSucceed(ParallelExecutor executor)
      throws Exception {

    EntryEventImpl event = new EntryEventImpl();
    event.setNewValue(null);
    event.setOperation(Operation.CREATE);

    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = createCacheProfile();
    advisor.putProfile(profile, true);

    executor.inParallel(() -> {
      advisor.adviseUpdate(event);
    }, count);
    executor.execute();

    assertThat(advisor.adviseAllEventsOrCached())
        .contains(profile.getDistributedMember());
    assertThat(advisor.adviseAllEventsOrCached())
        .hasSize(1);

  }

  private CacheDistributionAdvisor createCacheDistributionAdvisor() {
    CacheDistributionAdvisee advisee = mock(CacheDistributionAdvisee.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    DistributionManager distributionManager = mock(DistributionManager.class);

    when(advisee.getCancelCriterion()).thenReturn(cancelCriterion);
    when(advisee.getDistributionManager()).thenReturn(distributionManager);

    CacheDistributionAdvisor result =
        CacheDistributionAdvisor.createCacheDistributionAdvisor(advisee);

    when(advisee.getDistributionAdvisor()).thenReturn(result);

    return result;
  }

  private CacheProfile createCacheProfile() throws UnknownHostException {
    InternalDistributedMember member =
        new InternalDistributedMember(getLocalHost(), 0, false, false);
    return new CacheProfile(member, 1);
  }

  private static <T> T mock(Class<T> classToMock) {
    return Mockito.mock(classToMock, withSettings().stubOnly());
  }
}
