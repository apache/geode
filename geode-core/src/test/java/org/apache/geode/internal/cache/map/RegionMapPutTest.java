/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.AbstractRegionMapTest;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;

public class RegionMapPutTest {
/*
  @Test
  public void createOnEmptyMapAddsEntry() {
    final AbstractRegionMapTest.TestableAbstractRegionMap
        arm = new AbstractRegionMapTest.TestableAbstractRegionMap();
    final EntryEventImpl event = createEventForCreate(arm._getOwner(), "key");
    event.setNewValue("value");
    final boolean ifNew = true;
    final boolean ifOld = false;
    final boolean requireOldValue = false;
    final Object expectedOldValue = null;

    RegionEntry result =
        arm.basicPut(event, 0L, ifNew, ifOld, expectedOldValue, requireOldValue, false);

    assertThat(result).isNotNull();
    assertThat(result.getKey()).isEqualTo("key");
    assertThat(result.getValue()).isEqualTo("value");
    verify(arm._getOwner(), times(1)).basicPutPart2(eq(event), eq(result), eq(true), anyLong(),
        eq(false));
    verify(arm._getOwner(), times(1)).basicPutPart3(eq(event), eq(result), eq(true), anyLong(),
        eq(true), eq(ifNew), eq(ifOld), eq(expectedOldValue), eq(requireOldValue));
  }

  @Test
  public void verifyConcurrentCreateHasCorrectResult() throws Exception {
    CountDownLatch firstCreateAddedUninitializedEntry = new CountDownLatch(1);
    CountDownLatch secondCreateFoundFirstCreatesEntry = new CountDownLatch(1);
    TestableBasicPutMap arm = new TestableBasicPutMap(firstCreateAddedUninitializedEntry,
        secondCreateFoundFirstCreatesEntry);
    // The key needs to be long enough to not be stored inline on the region entry.
    String key1 = "lonGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGkey";
    String key2 = new String(key1);

    Future<RegionEntry> future = doFirstCreateInAnotherThread(arm, key1);
    if (!firstCreateAddedUninitializedEntry.await(5, TimeUnit.SECONDS)) {
      // something is wrong with the other thread
      // so count down the latch it may be waiting
      // on and then call get to see what went wrong with him.
      secondCreateFoundFirstCreatesEntry.countDown();
      fail("other thread took too long. It returned " + future.get());
    }
    EntryEventImpl event = createEventForCreate(arm._getOwner(), key2);
    // now do the second create
    RegionEntry result = arm.basicPut(event, 0L, true, false, null, false, false);

    RegionEntry resultFromOtherThread = future.get();

    assertThat(result).isNull();
    assertThat(resultFromOtherThread).isNotNull();
    assertThat(resultFromOtherThread.getKey()).isSameAs(key1);
  }

  private EntryEventImpl createEventForCreate(LocalRegion lr, String key) {
    when(lr.getKeyInfo(key)).thenReturn(new KeyInfo(key, null, null));
    EntryEventImpl event =
        EntryEventImpl.create(lr, Operation.CREATE, key, false, null, true, false);
    event.setNewValue("create_value");
    return event;
  }

  private Future<RegionEntry> doFirstCreateInAnotherThread(TestableBasicPutMap arm, String key) {
    Future<RegionEntry> result = CompletableFuture.supplyAsync(() -> {
      EntryEventImpl event = createEventForCreate(arm._getOwner(), key);
      return arm.basicPut(event, 0L, true, false, null, false, false);
    });
    return result;
  }

  private static class TestableBasicPutMap extends AbstractRegionMapTest.TestableAbstractRegionMap {
    private final CountDownLatch firstCreateAddedUninitializedEntry;
    private final CountDownLatch secondCreateFoundFirstCreatesEntry;
    private boolean alreadyCalledPutIfAbsentNewEntry;
    private boolean alreadyCalledAddRegionEntryToMapAndDoPut;

    public TestableBasicPutMap(CountDownLatch removePhase1Completed,
                               CountDownLatch secondCreateFoundFirstCreatesEntry) {
      super();
      this.firstCreateAddedUninitializedEntry = removePhase1Completed;
      this.secondCreateFoundFirstCreatesEntry = secondCreateFoundFirstCreatesEntry;
    }

    @Override
    protected boolean addRegionEntryToMapAndDoPut(final RegionMapPut putInfo) {
      if (!alreadyCalledAddRegionEntryToMapAndDoPut) {
        alreadyCalledAddRegionEntryToMapAndDoPut = true;
      } else {
        this.secondCreateFoundFirstCreatesEntry.countDown();
      }
      return super.addRegionEntryToMapAndDoPut(putInfo);
    }

    @Override
    protected void putIfAbsentNewEntry(final RegionMapPut putInfo) {
      super.putIfAbsentNewEntry(putInfo);
      if (!alreadyCalledPutIfAbsentNewEntry) {
        alreadyCalledPutIfAbsentNewEntry = true;
        this.firstCreateAddedUninitializedEntry.countDown();
        try {
          this.secondCreateFoundFirstCreatesEntry.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }
*/
}