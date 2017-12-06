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


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.LocalRegion.NonTXEntry;
import org.apache.geode.internal.cache.region.entry.RegionEntryFactoryBuilder;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TXStateProxyImplJUnitTest {
  @Test
  public void testGetKeyForIterator() {
    RegionEntryFactory factory = new RegionEntryFactoryBuilder().getRegionEntryFactoryOrNull(false,
        false, false, false, false);
    LocalRegion region = mock(LocalRegion.class);
    String key = "testkey";
    RegionEntry re = factory.createEntry(region, key, null);
    TXId txId = new TXId(mock(InternalDistributedMember.class), 1);
    TXStateProxyImpl tx = new TXStateProxyImpl(mock(TXManagerImpl.class), txId, false);
    LocalRegionDataView view = mock(LocalRegionDataView.class);
    boolean allowTombstones = false;
    boolean rememberReads = true;

    KeyInfo stringKeyInfo = new KeyInfo(key, null, null);
    KeyInfo regionEntryKeyInfo = new KeyInfo(re, null, null);

    when(region.getSharedDataView()).thenReturn(view);
    when(view.getEntry(stringKeyInfo, region, allowTombstones)).thenReturn(mock(NonTXEntry.class));
    when(view.getKeyForIterator(stringKeyInfo, region, rememberReads, allowTombstones))
        .thenCallRealMethod();
    when(view.getKeyForIterator(regionEntryKeyInfo, region, rememberReads, allowTombstones))
        .thenCallRealMethod();

    Object key1 = tx.getKeyForIterator(regionEntryKeyInfo, region, rememberReads, allowTombstones);
    assertTrue(key1.equals(key));
    Object key2 = tx.getKeyForIterator(stringKeyInfo, region, rememberReads, allowTombstones);
    assertTrue(key2.equals(key));
  }

}
