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

package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionQueryEvaluator;

/**
 * TODO: Not implemented fully for all the hooks.
 *
 */
public class IndexTrackingTestHook implements PartitionedRegionQueryEvaluator.TestHook {

  public static final String INDEX_NAME = "keyIndex1";

  IndexTrackingQueryObserver.IndexInfo rMap;
  Region regn;
  int bkts;

  public IndexTrackingTestHook(Region region, int bukts) {
    this.regn = region;
    this.bkts = bukts;
  }


  public void hook(int spot) throws RuntimeException {

    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(observer instanceof IndexTrackingQueryObserver);
    IndexTrackingQueryObserver gfObserver = (IndexTrackingQueryObserver) observer;

    if (spot == 1) { // before index lookup
    } else if (spot == 2) { // before key range index lookup
    } else if (spot == 3) { // End of afterIndexLookup call
    } else if (spot == 4) { // Before resetting indexInfoMap
      Map map = gfObserver.getUsedIndexes();
      assertEquals(1, map.size());

      assertTrue(map.get(
          INDEX_NAME) instanceof IndexTrackingQueryObserver.IndexInfo);
      rMap = (IndexTrackingQueryObserver.IndexInfo) map.get(
          INDEX_NAME);

      if (this.regn instanceof PartitionedRegion) {
        assertEquals(1, rMap.getResults().size());
      } else if (this.regn instanceof LocalRegion) {
        assertEquals(1, rMap.getResults().size());
      }
    }
  }

  public IndexTrackingQueryObserver.IndexInfo getRegionMap() {
    return rMap;
  }
}
