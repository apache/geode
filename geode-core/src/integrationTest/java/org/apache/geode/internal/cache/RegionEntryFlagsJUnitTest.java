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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;

/**
 * This test verifies the flag's on-off switching for boolean flags in AbstractRegionEntry.
 * Currently a byte array is used to maintain two flags.
 */
public class RegionEntryFlagsJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testUpdateInProgressFlag() {
    Region region = CacheUtils.createRegion("testRegion", null, Scope.DISTRIBUTED_ACK);
    // Put one entry in the region.
    region.put(1, 1);
    Set entries = region.entrySet();
    assertEquals(1, entries.size());

    Region.Entry nonTxEntry = (Entry) entries.iterator().next();
    RegionEntry entry = ((NonTXEntry) nonTxEntry).getRegionEntry();
    assertFalse(entry.isUpdateInProgress());
    entry.setUpdateInProgress(true);
    assertTrue(entry.isUpdateInProgress());
    entry.setUpdateInProgress(false);
    assertFalse(entry.isUpdateInProgress());
  }

  @Test
  public void testNetSearchFlag() {
    Region region = CacheUtils.createRegion("testRegion", null, Scope.DISTRIBUTED_ACK);
    // Put one entry in the region.
    region.put(1, 1);
    Set entries = region.entrySet();
    assertEquals(1, entries.size());

    Region.Entry nonTxEntry = (Entry) entries.iterator().next();
    RegionEntry entry = ((NonTXEntry) nonTxEntry).getRegionEntry();
    assertFalse(entry.getValueWasResultOfSearch());
    entry.setValueResultOfSearch(true);
    assertTrue(entry.getValueWasResultOfSearch());
    entry.setValueResultOfSearch(false);
    assertFalse(entry.getValueWasResultOfSearch());
  }
}
