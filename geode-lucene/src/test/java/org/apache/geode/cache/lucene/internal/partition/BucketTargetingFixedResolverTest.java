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
package org.apache.geode.cache.lucene.internal.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
@RunWith(JUnitQuickcheck.class)
public class BucketTargetingFixedResolverTest {

  @Property
  public void shouldReturnCorrectPartitionForGetHashKey(
      @Size(min = 1, max = 5) List<@InRange(minInt = 1, maxInt = 20) Integer> partitionSizes,
      @InRange(minInt = 0, maxInt = 50) int bucketId) {
    BucketTargetingFixedResolver resolver = new BucketTargetingFixedResolver();

    ConcurrentMap<String, Integer[]> fakePartitions = new ConcurrentHashMap<>();
    int startingBucket = 0;
    for (int i = 0; i < partitionSizes.size(); i++) {
      fakePartitions.put("p" + i, new Integer[] {startingBucket, partitionSizes.get(i)});
      startingBucket += partitionSizes.get(i);
    }
    assumeTrue(bucketId < startingBucket);

    final PartitionedRegion region = mock(PartitionedRegion.class);
    when(region.getPartitionsMap()).thenReturn(fakePartitions);
    when(region.isFixedPartitionedRegion()).thenReturn(true);
    when(region.getPartitionResolver()).thenReturn(resolver);
    assertEquals(bucketId,
        PartitionedRegionHelper.getHashKey(region, Operation.CREATE, "key", "value", bucketId));
  }

}
