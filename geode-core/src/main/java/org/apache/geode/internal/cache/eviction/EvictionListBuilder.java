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
package org.apache.geode.internal.cache.eviction;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.lang.SystemPropertyHelper;

public class EvictionListBuilder {

  private final boolean EVICTION_SCAN_ASYNC =
      SystemPropertyHelper.getProductBooleanProperty(SystemPropertyHelper.EVICTION_SCAN_ASYNC);

  private EvictionAlgorithm algorithm;
  private Object region;
  private EvictionController controller;
  private InternalRegionArguments args;

  public EvictionListBuilder(EvictionAlgorithm algorithm) {
    this.algorithm = algorithm;
  }

  /**
   * @param region PlaceHolderDiskRegion during disk recovery or LocalRegion
   */
  public EvictionListBuilder withRegion(Object region) {
    this.region = region;
    return this;
  }

  public EvictionListBuilder withEvictionController(EvictionController evictionController) {
    this.controller = evictionController;
    return this;
  }

  public EvictionListBuilder withArgs(InternalRegionArguments args) {
    this.args = args;
    return this;
  }

  public EvictionList create() {
    if (algorithm.isLIFO()) {
      return new LIFOList(getEvictionStats(), getBucketRegion());
    } else {
      if (EVICTION_SCAN_ASYNC) {
        return new LRUListWithAsyncSorting(getEvictionStats(), getBucketRegion());
      } else {
        return new LRUListWithSyncSorting(getEvictionStats(), getBucketRegion());
      }
    }
  }

  private EvictionCounters getEvictionStats() {
    EvictionCounters statistics = null;
//    if (region != null) {
//      if (region instanceof BucketRegion) {
//        if (args != null && args.getPartitionedRegion() != null) {
//          statistics = args.getPartitionedRegion().getEvictionController().getCounters();
//        } else {
//          statistics = new DisabledEvictionCounters();
//        }
//      } else if (region instanceof PlaceHolderDiskRegion) {
//        statistics = ((PlaceHolderDiskRegion) region).getPRLRUStats();
//      } else if (region instanceof PartitionedRegion) {
//        statistics = ((PartitionedRegion) region).getPREvictionControllerFromDiskInitialization();
//        if (statistics != null) {
//          PartitionedRegion partitionedRegion = (PartitionedRegion) region;
//          EvictionController evictionController = partitionedRegion.getEvictionController();
//          ((AbstractEvictionController) evictionController).setCounters(statistics);
//        }
//      }
//    }
//    if (statistics == null) {
//      StatisticsFactory sf = GemFireCacheImpl.getExisting("").getDistributedSystem();
//      statistics = controller.initStats(region, sf);
//    }
    return statistics;
  }

  private BucketRegion getBucketRegion() {
    return region instanceof BucketRegion ? (BucketRegion) region : null;
  }
}
