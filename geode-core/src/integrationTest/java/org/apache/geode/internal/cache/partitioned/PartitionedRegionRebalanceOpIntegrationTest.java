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
package org.apache.geode.internal.cache.partitioned;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionTestHelper;
import org.apache.geode.internal.cache.partitioned.rebalance.MoveBuckets;
import org.apache.geode.internal.cache.partitioned.rebalance.RebalanceDirector;


public class PartitionedRegionRebalanceOpIntegrationTest {
  private PartitionedRegion leaderRegion;
  private PartitionedRegion colocRegion1;
  private PartitionedRegion colocRegion2;

  @Test
  public void testRebalanceModelContainsAllColocatedRegionsWhenRebalanceTargetIsNotLeaderRegion() {
    leaderRegion =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("leader");
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes pa = paf.setColocatedWith("/leader").create();
    colocRegion1 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo1", pa);
    colocRegion2 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo2", pa);
    RebalanceDirector director = new MoveBuckets();
    PartitionedRegionRebalanceOp rebalanceOp =
        new PartitionedRegionRebalanceOp(colocRegion1, false, director, false, true,
            new AtomicBoolean(false), null);
    Set<PartitionRebalanceInfo> rebalanceInfo = rebalanceOp.execute();
    // 3 regions include the leader region and 2 colocated regions.
    assertThat(rebalanceInfo).hasSize(3);
  }
}
