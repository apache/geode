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
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class BucketAdvisorTest {

  @Test
  public void shouldBeMockable() throws Exception {
    BucketAdvisor mockBucketAdvisor = mock(BucketAdvisor.class);
    InternalDistributedMember mockInternalDistributedMember = mock(InternalDistributedMember.class);

    when(mockBucketAdvisor.basicGetPrimaryMember()).thenReturn(mockInternalDistributedMember);
    when(mockBucketAdvisor.getBucketRedundancy()).thenReturn(1);

    assertThat(mockBucketAdvisor.basicGetPrimaryMember()).isEqualTo(mockInternalDistributedMember);
    assertThat(mockBucketAdvisor.getBucketRedundancy()).isEqualTo(1);
  }

  @Test
  public void volunteerForPrimaryIgnoresMissingPrimaryElector() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getId()).thenReturn(new InternalDistributedMember("localhost", 321));

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));

    InternalDistributedMember missingElectorId = new InternalDistributedMember("localhost", 123);

    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);
    // hasPartitionedRegion() is invoked twice - once in initializePrimaryElector() and then in
    // volunteerForPrimary(). Returning true first simulates a elector being
    // there when createBucketAtomically() initiates creation of a bucket. Returning
    // false the second time simulates the elector closing its region/cache before
    // we get to the point of volunteering for primary
    when(regionAdvisor.hasPartitionedRegion(Mockito.any(InternalDistributedMember.class)))
        .thenReturn(true,
            false);

    BucketAdvisor advisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);
    BucketAdvisor advisorSpy = spy(advisor);
    doCallRealMethod().when(advisorSpy).exchangeProfiles();
    doCallRealMethod().when(advisorSpy).volunteerForPrimary();
    doReturn(true).when(advisorSpy).initializationGate();
    doReturn(true).when(advisorSpy).isHosting();

    BucketAdvisor.VolunteeringDelegate volunteeringDelegate =
        mock(BucketAdvisor.VolunteeringDelegate.class);
    advisorSpy.setVolunteeringDelegate(volunteeringDelegate);
    advisorSpy.initializePrimaryElector(missingElectorId);
    assertEquals(missingElectorId, advisorSpy.getPrimaryElector());
    advisorSpy.volunteerForPrimary();
    verify(volunteeringDelegate).volunteerForPrimary();
  }
}
