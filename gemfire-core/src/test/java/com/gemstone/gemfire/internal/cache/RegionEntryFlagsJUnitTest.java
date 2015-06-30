/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test verifies the flag's on-off switching for
 * boolean flags in AbstractRegionEntry.
 * Currently a byte array is used to maintain two flags.
 * 
 * @author shobhit
 *
 */
@Category(IntegrationTest.class)
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
    Region region = CacheUtils.createRegion("testRegion", null,
        Scope.DISTRIBUTED_ACK);
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
    Region region = CacheUtils.createRegion("testRegion", null,
        Scope.DISTRIBUTED_ACK);
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
