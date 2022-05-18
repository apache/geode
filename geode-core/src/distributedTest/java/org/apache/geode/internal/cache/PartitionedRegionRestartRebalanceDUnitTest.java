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

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class PartitionedRegionRestartRebalanceDUnitTest implements Serializable {
  private static final int REDUNDANT_COPIES = 2;
  private static final int TOTAL_NUM_BUCKETS = 12;
  private static final Logger logger = LogManager.getLogger();

  private String REGION_NAME = getClass().getSimpleName();;
  private VM[] datastores;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    datastores = new VM[4];
    for (int i = 0; i < datastores.length; i++) {
      datastores[i] = getVM(i);
      datastores[i].invoke(() -> cacheRule.createCache());
      datastores[i].invoke(() -> createRegion());
    }
    datastores[0].invoke(() -> feedData());
  }

  private void createRegion() {
    PartitionAttributesFactory<String, Integer> paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(REDUNDANT_COPIES);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory<String, Integer> rf = cacheRule.getCache().createRegionFactory();
    rf.setDataPolicy(DataPolicy.PARTITION);
    rf.setPartitionAttributes(paf.create());
    LocalRegion region = (LocalRegion) rf.create(REGION_NAME);
  }

  private void feedData() throws InterruptedException {
    PartitionedRegion pr = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
    for (int i = 0; i < TOTAL_NUM_BUCKETS * 2; i++) {
      pr.put(i, "VALUE-" + i);
      if (i < TOTAL_NUM_BUCKETS) {
        pr.destroy(i);
      }
    }
    cacheRule.getCache().getTombstoneService().forceBatchExpirationForTests(TOTAL_NUM_BUCKETS);
  }

  private void rebalance() throws InterruptedException {
    RebalanceOperation op =
        cacheRule.getCache().getResourceManager().createRebalanceFactory().start();
    RebalanceResults results = op.getResults();
    logger.info("Rebalance total time is " + results.getTotalTime());
  }

  private void verify() {
    PartitionedRegion pr = (PartitionedRegion) cacheRule.getCache().getRegion(REGION_NAME);
    for (BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
      Set<VersionSource> departedMemberSet = br.getVersionVector().getDepartedMembersSet();
      for (Object key : br.getRegionKeysForIteration()) {
        RegionEntry entry = br.getRegionEntry(key);
        departedMemberSet.remove(entry.getVersionStamp().getMemberID());
        if (departedMemberSet.isEmpty()) {
          break;
        }
      }
      Map map = br.getVersionVector().getMemberToVersion();
      for (Object key : br.getVersionVector().getMemberToVersion().keySet()) {
        logger.info(br.getFullPath() + ":" + key + ":"
            + br.getVersionVector().getMemberToVersion().get(key));
      }
      // The test proved that departedMemberSet is not growing
      assertThat(departedMemberSet.size()).isLessThanOrEqualTo(datastores.length);
    }
  }

  @Test
  public void restartAndRebalanceShouldNotIncreaseMemberToVersionMap() {
    for (int i = 0; i < datastores.length * 10; i++) {
      datastores[i % datastores.length].invoke(() -> {
        cacheRule.getCache().close();
      });
      datastores[(i + 1) % datastores.length].invoke(() -> {
        rebalance();
        verify();
      });
      datastores[i % datastores.length].invoke(() -> {
        cacheRule.createCache();
        createRegion();
        rebalance();
        verify();
      });
    }
  }
}
