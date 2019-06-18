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
package org.apache.geode.internal.cache.wan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;
import org.apache.geode.test.fake.Fakes;

public class GatewaySenderEventImplTest {

  private GemFireCacheImpl cache;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUpGemFire() {
    createCache();
  }

  private void createCache() {
    // Mock cache
    cache = Fakes.cache();
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(cache.getDistributedSystem()).thenReturn(ids);
  }

  @Test
  public void testEquality() throws Exception {
    LocalRegion region = mock(LocalRegion.class);
    when(region.getFullPath()).thenReturn(testName.getMethodName() + "_region");
    when(region.getCache()).thenReturn(cache);
    Object event = ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
        "key1", "value1", 0, 0, 0, 0);

    // Basic equality tests
    assertThat(event).isNotEqualTo(null);
    assertThat(event).isEqualTo(event);

    // Verify an event is equal to a duplicate
    Object eventDuplicate =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 0, 0, 0);
    assertThat(event).isEqualTo(eventDuplicate);

    // Verify an event is not equal if any of its fields are different
    Object eventDifferentShadowKey =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 0, 0, 1);
    assertThat(event).isNotEqualTo(eventDifferentShadowKey);

    Object eventDifferentEventId =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 1, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentEventId);

    Object eventDifferentBucketId =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value1", 0, 0, 1, 0);
    assertThat(event).isNotEqualTo(eventDifferentBucketId);

    Object eventDifferentOperation =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.UPDATE,
            "key1", "value1", 0, 0, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentOperation);

    Object eventDifferentKey =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key2", "value1", 0, 0, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentKey);

    Object eventDifferentValue =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region, Operation.CREATE,
            "key1", "value2", 0, 0, 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentValue);

    LocalRegion region2 = mock(LocalRegion.class);
    when(region2.getFullPath()).thenReturn(testName.getMethodName() + "_region2");
    when(region2.getCache()).thenReturn(cache);
    Object eventDifferentRegion =
        ParallelGatewaySenderHelper.createGatewaySenderEvent(region2, Operation.CREATE,
            "key1", "value1", 0, 0);
    assertThat(event).isNotEqualTo(eventDifferentRegion);
  }
}
