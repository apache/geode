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

package org.apache.geode.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.internal.cache.RegionEntry;

/*
 * PowerMock used in this test to verify static method MemoryAllocatorImpl.debugLog
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"*.UnitTest"})
@PrepareForTest({MemoryAllocatorImpl.class})
public class ReferenceCountHelperImplTest {

  ReferenceCountHelperImpl rchi;

  @Rule
  public SystemOutRule sor = new SystemOutRule();

  @Test
  public void doTrackReferenceCountsWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    assertTrue(rchi.trackReferenceCounts());
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    assertTrue(rchi.trackReferenceCounts());
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsFalseAndTrackFreesTrue() {
    rchi = getFalseTrue();
    assertFalse(rchi.trackReferenceCounts());
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsFalseAndTrackFreesFalse() {
    rchi = getFalseFalse();
    assertFalse(rchi.trackReferenceCounts());
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    assertTrue(rchi.trackFreedReferenceCounts());
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    assertFalse(rchi.trackFreedReferenceCounts());
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsFalseAndTrackFreesTrue() {
    rchi = getFalseTrue();
    assertTrue(rchi.trackFreedReferenceCounts());
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsFalseAndTrackFreesFalse() {
    rchi = getFalseFalse();
    assertFalse(rchi.trackFreedReferenceCounts());
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    Object preOwner = rchi.getReferenceCountOwner();

    rchi.skipRefCountTracking();
    Object postOwner = rchi.getReferenceCountOwner();

    assertTrue(postOwner != preOwner); // skip sets owner to SKIP_REF_COUNT_TRACKING

    assertFalse(rchi.isRefCountTracking());

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;
    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    rchi.unskipRefCountTracking();
    postOwner = rchi.getReferenceCountOwner();
    assertEquals(postOwner, preOwner);

    assertTrue(rchi.isRefCountTracking());
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsFalseAndTrackFreesTrue() {
    rchi = getFalseTrue();
    Object preOwner = rchi.getReferenceCountOwner();
    assertEquals(null, preOwner); // getReferenceCountOwner returns null if not tracking

    rchi.skipRefCountTracking();
    assertFalse(rchi.isRefCountTracking());

    rchi.unskipRefCountTracking();
    assertFalse(rchi.isRefCountTracking()); // system prop not set
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsFalseAndTrackFreesFalse() {
    rchi = getFalseFalse();
    Object preOwner = rchi.getReferenceCountOwner();
    assertEquals(null, preOwner); // getReferenceCountOwner returns null if not tracking

    rchi.skipRefCountTracking();
    assertFalse(rchi.isRefCountTracking());

    rchi.unskipRefCountTracking();
    assertFalse(rchi.isRefCountTracking()); // system prop not set
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    Object preOwner = rchi.getReferenceCountOwner();

    rchi.skipRefCountTracking();
    Object postOwner = rchi.getReferenceCountOwner();

    assertTrue(postOwner != preOwner); // skip sets owner to SKIP_REF_COUNT_TRACKING

    assertFalse(rchi.isRefCountTracking());

    rchi.unskipRefCountTracking();
    postOwner = rchi.getReferenceCountOwner();
    assertEquals(postOwner, preOwner);

    assertTrue(rchi.isRefCountTracking());
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    String owner = null;
    rchi.setReferenceCountOwner(owner);
    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(0, ai.get());

    owner = new String("SomeOwner");
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), owner);

    String owner2 = new String("SomeOwner2");
    rchi.setReferenceCountOwner(owner2);
    ai = rchi.getReenterCount();
    assertEquals(2, ai.get());
    assertTrue(rchi.getReferenceCountOwner() != owner2); // stays original owner until cnt = 0

    String owner3 = null;
    rchi.setReferenceCountOwner(owner3);
    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), owner);

    owner = null;
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(0, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), null);

    RegionEntry re = mock(RegionEntry.class);
    rchi.setReferenceCountOwner(re);
    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), re);

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size());

  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsFalseAndTrackFreesTrue() {
    rchi = getFalseTrue();
    String owner = null;
    rchi.setReferenceCountOwner(owner);
    assertEquals(rchi.getReferenceCountOwner(), owner);
    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(null, ai);
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsFalseAndTrackFreesFalse() {
    rchi = getFalseFalse();
    String owner = null;
    rchi.setReferenceCountOwner(owner);
    assertEquals(rchi.getReferenceCountOwner(), owner);
    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(null, ai);
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    String owner = null;
    rchi.setReferenceCountOwner(owner);
    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(0, ai.get());

    owner = new String("SomeOwner");
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), owner);

    String owner2 = new String("SomeOwner2");
    rchi.setReferenceCountOwner(owner2);
    ai = rchi.getReenterCount();
    assertEquals(2, ai.get());
    assertTrue(rchi.getReferenceCountOwner() != owner2); // stays original owner until cnt = 0

    String owner3 = null;
    rchi.setReferenceCountOwner(owner3);
    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), owner);

    owner = null;
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(0, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), null);
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    Object owner = rchi.createReferenceCountOwner();
    assertFalse(owner == null);
    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(1, ai.get());

    owner = null;
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(0, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), null);
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsFalseAndTrackFreesTrue() {
    rchi = getFalseTrue();
    Object owner = rchi.createReferenceCountOwner();
    assertTrue(owner == null);
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsFalseAndTrackFreesFalse() {
    rchi = getFalseFalse();
    Object owner = rchi.createReferenceCountOwner();
    assertTrue(owner == null);
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    Object owner = rchi.createReferenceCountOwner();
    assertFalse(owner == null);
    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(1, ai.get());

    owner = null;
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(0, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), null);
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    rchi.freeRefCountInfo(address); // quick check of free of nonexistent info

    Object owner = rchi.getReferenceCountOwner();
    assertTrue(owner == null);

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size()); // inc and dec are tracked in different changeinfo objects (?)
    rcci = list.get(1);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size());
    rcci = list.get(1);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size()); // list contains 2 entries from inc/dec done above

    List<RefCountChangeInfo> freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // no freeRefCountInfo calls yet

    rchi.freeRefCountInfo(address); // when freed, moved to FreeRefCountInfo list

    List<RefCountChangeInfo> freeInfo2 = rchi.getFreeRefCountInfo(address);
    assertEquals(2, freeInfo2.size()); // the inc/dec info moved to freeRefCountInfo list

    list = rchi.getRefCountInfo(address);
    assertEquals(null, list); // the inc/dec ref count list should now be null
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsFalseAndTrackFreesTrue() {

    rchi = getFalseTrue();

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.getReferenceCountOwner();
    assertTrue(owner == null);

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    List<RefCountChangeInfo> freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // no freeRefCountInfo calls yet

    rchi.freeRefCountInfo(address); // noop when not tracking

    freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // should still be null
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsFalseAndTrackFreesFalse() {

    rchi = getFalseFalse();

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.getReferenceCountOwner();
    assertTrue(owner == null);

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    List<RefCountChangeInfo> freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // no freeRefCountInfo calls yet

    rchi.freeRefCountInfo(address); // noop when not tracking

    freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // should still be null
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.getReferenceCountOwner();
    assertTrue(owner == null);

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size()); // inc and dec are tracked in different changeinfo objects (?)
    rcci = list.get(1);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size());
    rcci = list.get(1);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size()); // list contains 2 entries from inc/dec done above

    List<RefCountChangeInfo> freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // no freeRefCountInfo calls yet

    rchi.freeRefCountInfo(address); // when freed, moved to FreeRefCountInfo list

    List<RefCountChangeInfo> freeInfo2 = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo2); // not tracking freed info

    list = rchi.getRefCountInfo(address);
    assertEquals(null, list); // the inc/dec ref count list should now be null
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.createReferenceCountOwner();
    assertFalse(owner == null);

    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(1, ai.get());

    owner = null;
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(0, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), null);

    owner = rchi.createReferenceCountOwner();
    assertFalse(owner == null);

    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size()); // inc and dec are tracked in different changeinfo objects (?)
    rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.getRefCountInfo(address);
    assertEquals(0, list.size());
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsFalseAndTrackFreesTrue() {

    rchi = getFalseTrue();

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.createReferenceCountOwner();
    assertTrue(owner == null);

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list); // inc and dec are tracked in different changeinfo objects (?)

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    List<RefCountChangeInfo> freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // no freeRefCountInfo calls yet

    rchi.freeRefCountInfo(address); // when freed, moved to FreeRefCountInfo list

    freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // the inc/dec info moved to freeRefCountInfo list
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsFalseAndTrackFreesFalse() {

    rchi = getFalseFalse();

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.createReferenceCountOwner();
    assertTrue(owner == null);

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list); // inc and dec are tracked in different changeinfo objects (?)

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    List<RefCountChangeInfo> freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // no freeRefCountInfo calls yet

    rchi.freeRefCountInfo(address); // when freed, moved to FreeRefCountInfo list

    freeInfo = rchi.getFreeRefCountInfo(address);
    assertEquals(null, freeInfo); // the inc/dec info moved to freeRefCountInfo list
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    Object owner = rchi.createReferenceCountOwner();
    assertFalse(owner == null);

    AtomicInteger ai = rchi.getReenterCount();
    assertEquals(1, ai.get());

    owner = null;
    rchi.setReferenceCountOwner(owner);
    ai = rchi.getReenterCount();
    assertEquals(0, ai.get());
    assertEquals(rchi.getReferenceCountOwner(), null);

    owner = rchi.createReferenceCountOwner();
    assertFalse(owner == null);

    ai = rchi.getReenterCount();
    assertEquals(1, ai.get());

    rchi.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size()); // inc and dec are tracked in different changeinfo objects (?)
    rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size());
  }

  @Test
  public void doGetRefCountInfoWithTrackRefsTrueAndTrackFreesTrue() {
    rchi = getTrueTrue();
    long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    List<RefCountChangeInfo> list = null;

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address); // now getRefCountInfo
    assertEquals(1, info.size());
    rcci = info.get(0);
    assertEquals(1, rcci.getUseCount());

    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size()); // getRefCountInfo leaves list LOCKED (i.e. empty)
  }

  @Test
  public void doRefCountChangedAfterGetRefCountInfoWithTrackRefsTrueAndTrackFreesTrue()
      throws Exception {
    rchi = getTrueTrue();
    long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    List<RefCountChangeInfo> list = null;

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address); // now getRefCountInfo
    assertEquals(1, info.size());
    rcci = info.get(0);
    assertEquals(1, rcci.getUseCount());

    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size()); // getRefCountInfo leaves list LOCKED (i.e. empty)

    sor.mute(); // Mute system out

    PowerMockito.spy(MemoryAllocatorImpl.class); // Watch the impl for invocation of debugLog

    rchi.refCountChanged(address, decRefCount, rc); // this line should fail. no inc after getInfo
                                                    // allowed
    PowerMockito.verifyStatic(MemoryAllocatorImpl.class);
    MemoryAllocatorImpl.debugLog("refCount inced after orphan detected for @1000", true);

    decRefCount = true;

    rchi.refCountChanged(address, decRefCount, rc); // this line should fail. no inc after getInfo
                                                    // allowed
    PowerMockito.verifyStatic(MemoryAllocatorImpl.class);
    MemoryAllocatorImpl.debugLog("refCount deced after orphan detected for @1000", true);

    rchi.freeRefCountInfo(address); // this line should fail. no free after getInfo allowed
    PowerMockito.verifyStatic(MemoryAllocatorImpl.class);
    MemoryAllocatorImpl.debugLog("freed after orphan detected for @1000", true);

  }

  @Test
  public void doGetRefCountInfoWithTrackRefsFalseAndTrackFreesTrue() {
    rchi = getFalseTrue();
    long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    List<RefCountChangeInfo> list = rchi.getRefCountInfo(address);
    assertEquals(null, list);

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address);
    assertEquals(null, info);

    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    rchi.refCountChanged(address, decRefCount, rc); // this will be ignored.
    decRefCount = true;
    rchi.refCountChanged(address, decRefCount, rc); // this will be ignored.
    rchi.freeRefCountInfo(address); // this will be ignored.

  }

  @Test
  public void doGetRefCountInfoWithTrackRefsFalseAndTrackFreesFalse() {
    rchi = getFalseFalse();
    long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    List<RefCountChangeInfo> list = rchi.getRefCountInfo(address);
    assertEquals(null, list);

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address);
    assertEquals(null, info);

    list = rchi.peekRefCountInfo(address);
    assertEquals(null, list);
  }

  @Test
  public void doGetRefCountInfoWithTrackRefsTrueAndTrackFreesFalse() {
    rchi = getTrueFalse();
    long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    List<RefCountChangeInfo> list = null;

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    RefCountChangeInfo rcci = list.get(0);
    assertEquals(0, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    rchi.refCountChanged(address, decRefCount, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // line 258 of ref cnt helper does not set useCount = 1
                                         // when adding new entry?

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address); // now getRefCountInfo
    assertEquals(1, info.size());
    rcci = info.get(0);
    assertEquals(1, rcci.getUseCount());

    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size()); // getRefCountInfo leaves list LOCKED (i.e. empty)
  }

  private ReferenceCountHelperImpl getTrueTrue() {
    return new ReferenceCountHelperImpl(true, true);
  }

  private ReferenceCountHelperImpl getTrueFalse() {
    return new ReferenceCountHelperImpl(true, false);
  }

  private ReferenceCountHelperImpl getFalseTrue() {
    return new ReferenceCountHelperImpl(false, true);
  }

  private ReferenceCountHelperImpl getFalseFalse() {
    return new ReferenceCountHelperImpl(false, false);
  }

  private ReferenceCountHelperImpl getHookedImpl() {
    return new HookedReferenceCountHelperImpl(true, true);
  }

  @Test
  public void doGetRefCountInfoNonRegionEntryConcurrencyTest() {
    rchi = getHookedImpl();
    long address = (long) 0x1000;
    int rc = 1;
    RefCountChangeInfo rcci;

    List<RefCountChangeInfo> list = null;

    rchi.setReferenceCountOwner("TestOwner"); // assume test identity

    rchi.refCountChanged(address, false, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // hooked impl simulates a concurrent update, so cnt is >
                                         // expected

    rchi.setReferenceCountOwner(null); // sets owner to null and resets count
    rchi.setReferenceCountOwner(null); // sets owner to null and resets count

    rchi.setReferenceCountOwner("TestOwner2"); // assume new identity

    rchi.refCountChanged(address, false, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(2, rcci.getUseCount()); // list is not null, so hook not used

    rchi.refCountChanged(address, true, rc); // dec ref count
    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size()); // dec adds new list of stack traces
    rcci = list.get(1);
    assertEquals(0, rcci.getUseCount()); // cnt starts at 0 for new entries

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address); // now getRefCountInfo
    assertEquals(3, info.size()); // hooked impl added one to list
    rcci = info.get(2);
    assertEquals(0, rcci.getUseCount()); // count starts at 0 for new entries

    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size()); // getRefCountInfo leaves list LOCKED (i.e. empty)
  }

  @Test
  public void doGetRefCountInfoRegionEntryConcurrencyTest() {
    rchi = getHookedImpl();
    long address = (long) 0x1000;
    int rc = 1;
    RefCountChangeInfo rcci;

    List<RefCountChangeInfo> list = null;

    RegionEntry re = mock(RegionEntry.class);
    rchi.setReferenceCountOwner(re); // set owner to region entry type

    rchi.refCountChanged(address, false, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(1, rcci.getUseCount()); // hooked impl simulates a concurrent update, so cnt is >
                                         // expected

    rchi.setReferenceCountOwner(null); // sets owner to null and resets count
    rchi.setReferenceCountOwner(null); // sets owner to null and resets count

    RegionEntry re2 = mock(RegionEntry.class);
    rchi.setReferenceCountOwner(re2); // set owner to region entry type

    rchi.refCountChanged(address, false, rc);
    list = rchi.peekRefCountInfo(address);
    assertEquals(1, list.size());
    rcci = list.get(0);
    assertEquals(2, rcci.getUseCount()); // list is not null, so hook not used

    rchi.refCountChanged(address, true, rc); // dec ref count
    list = rchi.peekRefCountInfo(address);
    assertEquals(2, list.size()); // dec adds new list of stack traces
    rcci = list.get(1);
    assertEquals(0, rcci.getUseCount()); // cnt starts at 0 for new entries

    List<RefCountChangeInfo> info = rchi.getRefCountInfo(address); // now getRefCountInfo
    assertEquals(3, info.size()); // hooked impl added one to list
    rcci = info.get(2);
    assertEquals(0, rcci.getUseCount()); // count starts at 0 for new entries

    list = rchi.peekRefCountInfo(address);
    assertEquals(0, list.size()); // getRefCountInfo leaves list LOCKED (i.e. empty)
  }

  private class HookedReferenceCountHelperImpl extends ReferenceCountHelperImpl {
    HookedReferenceCountHelperImpl(boolean trackRefCounts, boolean trackFreedRefCounts) {
      super(trackRefCounts, trackFreedRefCounts);
    }

    protected int refCountChangedTestHookCount = 0;

    /*
     * Update list of stack traces for address. Hooked SUT should see that the list changed.
     */
    @Override
    protected void getReferenceCountInfoTestHook(
        ConcurrentMap<Long, List<RefCountChangeInfo>> stacktraces, long address) {
      List<RefCountChangeInfo> updatedList =
          new ArrayList<RefCountChangeInfo>(stacktraces.get(address));
      RefCountChangeInfo rcci = new RefCountChangeInfo(false, 0, "TestOwner");
      updatedList.add(rcci);
      stacktraces.put(address, updatedList);
    }

    /*
     * Reinvoke refCountChanged to update reference count. Hooked SUT should see that the count has
     * changed.
     */
    @Override
    protected void refCountChangedTestHook(Long address, boolean decRefCount, int rc) {
      if (refCountChangedTestHookCount == 0) {
        refCountChangedTestHookCount++;
        refCountChanged(address, decRefCount, rc);
      } else {
        refCountChangedTestHookCount--;
      }
    }
  }

}
