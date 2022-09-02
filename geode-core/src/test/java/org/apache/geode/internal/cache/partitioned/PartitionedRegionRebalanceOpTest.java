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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionTestHelper;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;


public class PartitionedRegionRebalanceOpTest {
  private PartitionedRegion leaderRegion;
  private PartitionedRegion colocRegion1;
  private PartitionedRegion colocRegion2;
  private static final long MB = 1024 * 1024;

  @Test
  public void testAllCollocatedRegionsSetWhenRebalanceTargetIsLeaderRegion() {
    leaderRegion = (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("leader");
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes pa = paf.setColocatedWith("/leader").create();
    colocRegion1 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo1", pa);
    colocRegion2 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo2", pa);

    try {
      PartitionedRegionRebalanceOp part_op =
          new PartitionedRegionRebalanceOp(leaderRegion, false, null, false, false, null, null);
      part_op.checkAndSetColocatedRegions();
      assertEquals(3, part_op.colocatedRegions.size());
    } finally {
      colocRegion1.destroyRegion();
      colocRegion2.destroyRegion();
      leaderRegion.destroyRegion();
    }
  }

  @Test
  public void testAllCollocatedRegionsSetWhenRebalanceTargetIsCollocatedRegion() {
    leaderRegion = (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("leader");
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes pa = paf.setColocatedWith("/leader").create();
    colocRegion1 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo1", pa);
    colocRegion2 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo2", pa);

    try {
      PartitionedRegionRebalanceOp part_op =
          new PartitionedRegionRebalanceOp(colocRegion1, false, null, false, false, null, null);
      part_op.checkAndSetColocatedRegions();
      assertEquals(3, part_op.colocatedRegions.size());
    } finally {
      colocRegion1.destroyRegion();
      colocRegion2.destroyRegion();
      leaderRegion.destroyRegion();
    }
  }

  @Test
  public void testRebalanceModelWhenRebalanceTargetIsCollocatedRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes pa = paf.setLocalMaxMemory(4).create();
    leaderRegion =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("leader", pa);
    paf = new PartitionAttributesFactory();
    pa = paf.setColocatedWith("/leader").setLocalMaxMemory(4).create();
    colocRegion1 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo1", pa);
    colocRegion2 =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("colo2", pa);

    try {
      PartitionedRegionRebalanceOp part_op =
          new PartitionedRegionRebalanceOp(colocRegion1, false, null, false, false, null, null);
      part_op.checkAndSetColocatedRegions();
      InternalCache cache = leaderRegion.getCache();
      Map<PartitionedRegion, InternalPRInfo> detailsMap = part_op.fetchDetails(cache);
      InternalResourceManager resourceMgr =
          InternalResourceManager.getInternalResourceManager(cache);
      PartitionedRegionLoadModel model = part_op.buildModel(null, detailsMap, resourceMgr);
      System.out.println(model.toString());
      Member member = model.getMember(leaderRegion.getDistributionManager().getId());
      assertEquals(member.getConfiguredMaxMemory() / MB, 12);
    } finally {
      colocRegion1.destroyRegion();
      colocRegion2.destroyRegion();
      leaderRegion.destroyRegion();
    }
  }
}
