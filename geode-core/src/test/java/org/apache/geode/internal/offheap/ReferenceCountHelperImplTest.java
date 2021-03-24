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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.RegionEntry;

public class ReferenceCountHelperImplTest {

  private BiConsumer<String, Boolean> debugLogger;

  @Before
  public void setUp() {
    debugLogger = uncheckedCast(mock(BiConsumer.class));
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    assertThat(referenceCountHelperImpl.trackReferenceCounts()).isTrue();
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();
    assertThat(referenceCountHelperImpl.trackReferenceCounts()).isTrue();
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    assertThat(referenceCountHelperImpl.trackReferenceCounts()).isFalse();
  }

  @Test
  public void doTrackReferenceCountsWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    assertThat(referenceCountHelperImpl.trackReferenceCounts()).isFalse();
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    assertThat(referenceCountHelperImpl.trackFreedReferenceCounts()).isTrue();
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();
    assertThat(referenceCountHelperImpl.trackFreedReferenceCounts()).isFalse();
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    assertThat(referenceCountHelperImpl.trackFreedReferenceCounts()).isTrue();
  }

  @Test
  public void doTrackFreedReferenceCountsWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    assertThat(referenceCountHelperImpl.trackFreedReferenceCounts()).isFalse();
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();

    Object preOwner = referenceCountHelperImpl.getReferenceCountOwner();
    referenceCountHelperImpl.skipRefCountTracking();
    Object postOwner = referenceCountHelperImpl.getReferenceCountOwner();

    // skip sets owner to SKIP_REF_COUNT_TRACKING
    assertThat(postOwner).isNotEqualTo(preOwner);
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isFalse();

    Long address = (long) 0x1000;
    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.unskipRefCountTracking();
    postOwner = referenceCountHelperImpl.getReferenceCountOwner();

    assertThat(preOwner).isEqualTo(postOwner);
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isTrue();
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    Object preOwner = referenceCountHelperImpl.getReferenceCountOwner();
    // getReferenceCountOwner returns null if not tracking
    assertThat(preOwner).isNull();

    referenceCountHelperImpl.skipRefCountTracking();
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isFalse();

    referenceCountHelperImpl.unskipRefCountTracking();
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isFalse(); // system prop not set
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    Object preOwner = referenceCountHelperImpl.getReferenceCountOwner();
    // getReferenceCountOwner returns null if not tracking
    assertThat(preOwner).isNull();

    referenceCountHelperImpl.skipRefCountTracking();
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isFalse();

    referenceCountHelperImpl.unskipRefCountTracking();
    // system prop not set
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isFalse();
  }

  @Test
  public void doSkipRefCountTrackingWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();

    Object preOwner = referenceCountHelperImpl.getReferenceCountOwner();
    referenceCountHelperImpl.skipRefCountTracking();
    Object postOwner = referenceCountHelperImpl.getReferenceCountOwner();
    // skip sets owner to SKIP_REF_COUNT_TRACKING
    assertThat(postOwner).isNotEqualTo(preOwner);
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isFalse();

    referenceCountHelperImpl.unskipRefCountTracking();
    postOwner = referenceCountHelperImpl.getReferenceCountOwner();
    assertThat(preOwner).isEqualTo(postOwner);
    assertThat(referenceCountHelperImpl.isRefCountTracking()).isTrue();
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    String owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();

    owner = "SomeOwner";
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isEqualTo(owner);

    String owner2 = "SomeOwner2";
    referenceCountHelperImpl.setReferenceCountOwner(owner2);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isEqualTo(2);
    // stays original owner until cnt = 0
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNotEqualTo(owner2);

    String owner3 = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner3);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isEqualTo(owner);

    owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();

    RegionEntry regionEntry = mock(RegionEntry.class);
    referenceCountHelperImpl.setReferenceCountOwner(regionEntry);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();
    assertThat(regionEntry).isEqualTo(referenceCountHelperImpl.getReferenceCountOwner());

    Long address = (long) 0x1000;
    boolean decRefCount = false;
    int rc = 1;

    referenceCountHelperImpl.refCountChanged(address, decRefCount, rc);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, decRefCount, rc);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    decRefCount = true;
    referenceCountHelperImpl.refCountChanged(address, decRefCount, rc);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, decRefCount, rc);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isEmpty();
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();

    referenceCountHelperImpl.setReferenceCountOwner(null);
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();

    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount).isNull();
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();

    referenceCountHelperImpl.setReferenceCountOwner(null);
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();

    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount).isNull();
  }

  @Test
  public void doSetReferenceCountOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();

    String owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();

    owner = "SomeOwner";
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isEqualTo(owner);

    String owner2 = "SomeOwner2";
    referenceCountHelperImpl.setReferenceCountOwner(owner2);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isEqualTo(2);
    // stays original owner until cnt = 0
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isEqualTo(owner);

    String owner3 = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner3);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isEqualTo(owner);

    owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();

    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNotNull();
    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();

    owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNull();
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNull();
  }

  @Test
  public void doCreateReferenceCountOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();
    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNotNull();
    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();

    owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    Long address = (long) 0x1000;

    // quick check of free of nonexistent info
    referenceCountHelperImpl.freeRefCountInfo(address);
    Object owner = referenceCountHelperImpl.getReferenceCountOwner();
    assertThat(owner).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);

    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);

    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // inc and dec are tracked in different changeinfo objects (?)
    assertThat(list).hasSize(2);

    refCountChangeInfo = list.get(1);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(2);

    refCountChangeInfo = list.get(1);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // list contains 2 entries from inc/dec done above
    assertThat(list).hasSize(2);

    List<RefCountChangeInfo> freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // no freeRefCountInfo calls yet
    assertThat(freeInfo).isNull();

    // when freed, moved to FreeRefCountInfo list
    referenceCountHelperImpl.freeRefCountInfo(address);
    List<RefCountChangeInfo> freeInfo2 = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // the inc/dec info moved to freeRefCountInfo list
    assertThat(freeInfo2).hasSize(2);

    list = referenceCountHelperImpl.getRefCountInfo(address);
    // the inc/dec ref count list should now be null
    assertThat(list).isNull();
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.getReferenceCountOwner();
    assertThat(owner).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    List<RefCountChangeInfo> freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // no freeRefCountInfo calls yet
    assertThat(freeInfo).isNull();

    // noop when not tracking
    referenceCountHelperImpl.freeRefCountInfo(address);
    freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // should still be null
    assertThat(freeInfo).isNull();
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.getReferenceCountOwner();
    assertThat(owner).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    List<RefCountChangeInfo> freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // no freeRefCountInfo calls yet
    assertThat(freeInfo).isNull();

    // noop when not tracking
    referenceCountHelperImpl.freeRefCountInfo(address);
    // should still be null
    freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    assertThat(freeInfo).isNull();
  }

  @Test
  public void doRefCountChangedNoOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.getReferenceCountOwner();
    assertThat(owner).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // inc and dec are tracked in different changeinfo objects (?)
    assertThat(list).hasSize(2);
    refCountChangeInfo = list.get(1);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(2);
    refCountChangeInfo = list.get(1);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // list contains 2 entries from inc/dec done above
    assertThat(list).hasSize(2);

    List<RefCountChangeInfo> freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // no freeRefCountInfo calls yet
    assertThat(freeInfo).isNull();

    // when freed, moved to FreeRefCountInfo list
    referenceCountHelperImpl.freeRefCountInfo(address);

    List<RefCountChangeInfo> freeInfo2 = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // not tracking freed info
    assertThat(freeInfo2).isNull();

    list = referenceCountHelperImpl.getRefCountInfo(address);
    // the inc/dec ref count list should now be null
    assertThat(list).isNull();
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNotNull();

    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();

    owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();

    owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNotNull();

    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // inc and dec are tracked in different changeinfo objects (?)
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(list).isEmpty();
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // inc and dec are tracked in different changeinfo objects (?)
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    List<RefCountChangeInfo> freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // no freeRefCountInfo calls yet
    assertThat(freeInfo).isNull();

    // when freed, moved to FreeRefCountInfo list
    referenceCountHelperImpl.freeRefCountInfo(address);

    freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // the inc/dec info moved to freeRefCountInfo list
    assertThat(freeInfo).isNull();
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // inc and dec are tracked in different changeinfo objects (?)
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    List<RefCountChangeInfo> freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // no freeRefCountInfo calls yet
    assertThat(freeInfo).isNull();

    // when freed, moved to FreeRefCountInfo list
    referenceCountHelperImpl.freeRefCountInfo(address);

    freeInfo = referenceCountHelperImpl.getFreeRefCountInfo(address);
    // the inc/dec info moved to freeRefCountInfo list
    assertThat(freeInfo).isNull();
  }

  @Test
  public void doRefCountChangedWithOwnerWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();
    Long address = (long) 0x1000;

    Object owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNotNull();

    AtomicInteger reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();

    owner = null;
    referenceCountHelperImpl.setReferenceCountOwner(owner);
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isZero();
    assertThat(referenceCountHelperImpl.getReferenceCountOwner()).isNull();

    owner = referenceCountHelperImpl.createReferenceCountOwner();
    assertThat(owner).isNotNull();
    reenterCount = referenceCountHelperImpl.getReenterCount();
    assertThat(reenterCount.get()).isOne();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // inc and dec are tracked in different changeinfo objects (?)
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isEmpty();
  }

  @Test
  public void doGetRefCountInfoWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    Long address = (long) 0x1000;

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    // now getRefCountInfo
    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(info).hasSize(1);
    refCountChangeInfo = info.get(0);
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // getRefCountInfo leaves list LOCKED (i.e. empty)
    assertThat(list).isEmpty();
  }

  @Test
  public void doRefCountChangedAfterGetRefCountInfoWithTrackRefsTrueAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueTrue();
    Long address = (long) 0x1000;

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    // now getRefCountInfo
    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(info).hasSize(1);
    refCountChangeInfo = info.get(0);
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // getRefCountInfo leaves list LOCKED (i.e. empty)
    assertThat(list).isEmpty();

    // this line should fail. no inc after getInfo allowed
    referenceCountHelperImpl.refCountChanged(address, false, 1);
    verify(debugLogger).accept("refCount inced after orphan detected for @1000", true);

    // this line should fail. no inc after getInfo allowed
    referenceCountHelperImpl.refCountChanged(address, true, 1);
    verify(debugLogger).accept("refCount deced after orphan detected for @1000", true);

    // this line should fail. no free after getInfo allowed
    referenceCountHelperImpl.freeRefCountInfo(address);
    verify(debugLogger).accept("freed after orphan detected for @1000", true);
  }

  @Test
  public void doGetRefCountInfoWithTrackRefsFalseAndTrackFreesTrue() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseTrue();
    Long address = (long) 0x1000;

    List<RefCountChangeInfo> list = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(info).isNull();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    // TODO: add assertions for the following

    // this will be ignored.
    referenceCountHelperImpl.refCountChanged(address, false, 1);
    // this will be ignored.
    referenceCountHelperImpl.refCountChanged(address, true, 1);
    // this will be ignored.
    referenceCountHelperImpl.freeRefCountInfo(address);
  }

  @Test
  public void doGetRefCountInfoWithTrackRefsFalseAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_FalseFalse();
    Long address = (long) 0x1000;

    List<RefCountChangeInfo> list = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(list).isNull();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();

    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(info).isNull();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).isNull();
  }

  @Test
  public void doGetRefCountInfoWithTrackRefsTrueAndTrackFreesFalse() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newReferenceCountHelperImpl_TrueFalse();
    long address = (long) 0x1000;

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // line 258 of ref cnt helper does not set useCount = 1 when adding new entry?
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    // now getRefCountInfo
    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    assertThat(info).hasSize(1);
    refCountChangeInfo = info.get(0);
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // getRefCountInfo leaves list LOCKED (i.e. empty)
    assertThat(list).isEmpty();
  }

  @Test
  public void doGetRefCountInfoNonRegionEntryConcurrencyTest() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newHookedReferenceCountHelperImpl();
    Long address = (long) 0x1000;

    // assume test identity
    referenceCountHelperImpl.setReferenceCountOwner("TestOwner");

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // hooked impl simulates a concurrent update, so cnt is > expected
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    // sets owner to null and resets count
    referenceCountHelperImpl.setReferenceCountOwner(null);
    // sets owner to null and resets count // sets owner to null and resets count

    // assume new identity
    referenceCountHelperImpl.setReferenceCountOwner("TestOwner2");

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // list is not null, so hook not used
    assertThat(refCountChangeInfo.getUseCount()).isEqualTo(2);

    // dec ref count
    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // dec adds new list of stack traces
    assertThat(list).hasSize(2);
    refCountChangeInfo = list.get(1);
    // cnt starts at 0 for new entries
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    // now getRefCountInfo
    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    // hooked impl added one to list
    assertThat(info).hasSize(3);
    refCountChangeInfo = info.get(2);
    // count starts at 0 for new entries
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // getRefCountInfo leaves list LOCKED (i.e. empty)
    assertThat(list).isEmpty();
  }

  @Test
  public void doGetRefCountInfoRegionEntryConcurrencyTest() {
    ReferenceCountHelperImpl referenceCountHelperImpl = newHookedReferenceCountHelperImpl();
    Long address = (long) 0x1000;

    RegionEntry regionEntry1 = mock(RegionEntry.class);
    // set owner to region entry type
    referenceCountHelperImpl.setReferenceCountOwner(regionEntry1);

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    List<RefCountChangeInfo> list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    RefCountChangeInfo refCountChangeInfo = list.get(0);
    // hooked impl simulates a concurrent update, so cnt is > expected
    assertThat(refCountChangeInfo.getUseCount()).isOne();

    // sets owner to null and resets count
    referenceCountHelperImpl.setReferenceCountOwner(null);
    // sets owner to null and resets count
    referenceCountHelperImpl.setReferenceCountOwner(null);

    RegionEntry regionEntry2 = mock(RegionEntry.class);
    // set owner to region entry type
    referenceCountHelperImpl.setReferenceCountOwner(regionEntry2);

    referenceCountHelperImpl.refCountChanged(address, false, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    assertThat(list).hasSize(1);
    refCountChangeInfo = list.get(0);
    // list is not null, so hook not used
    assertThat(refCountChangeInfo.getUseCount()).isEqualTo(2);

    // dec ref count
    referenceCountHelperImpl.refCountChanged(address, true, 1);
    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // dec adds new list of stack traces
    assertThat(list).hasSize(2);
    refCountChangeInfo = list.get(1);
    // cnt starts at 0 for new entries
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    // now getRefCountInfo
    List<RefCountChangeInfo> info = referenceCountHelperImpl.getRefCountInfo(address);
    // hooked impl added one to list
    assertThat(info).hasSize(3);
    refCountChangeInfo = info.get(2);
    // count starts at 0 for new entries
    assertThat(refCountChangeInfo.getUseCount()).isZero();

    list = referenceCountHelperImpl.peekRefCountInfo(address);
    // getRefCountInfo leaves list LOCKED (i.e. empty)
    assertThat(list).isEmpty();
  }

  private ReferenceCountHelperImpl newReferenceCountHelperImpl_TrueTrue() {
    return new ReferenceCountHelperImpl(true, true, debugLogger);
  }

  private ReferenceCountHelperImpl newReferenceCountHelperImpl_TrueFalse() {
    return new ReferenceCountHelperImpl(true, false, debugLogger);
  }

  private ReferenceCountHelperImpl newReferenceCountHelperImpl_FalseTrue() {
    return new ReferenceCountHelperImpl(false, true, debugLogger);
  }

  private ReferenceCountHelperImpl newReferenceCountHelperImpl_FalseFalse() {
    return new ReferenceCountHelperImpl(false, false, debugLogger);
  }

  private ReferenceCountHelperImpl newHookedReferenceCountHelperImpl() {
    return new HookedReferenceCountHelperImpl(true, true, debugLogger);
  }

  private static class HookedReferenceCountHelperImpl extends ReferenceCountHelperImpl {

    private int refCountChangedTestHookCount;

    private HookedReferenceCountHelperImpl(boolean trackRefCounts, boolean trackFreedRefCounts,
        BiConsumer<String, Boolean> debugLogger) {
      super(trackRefCounts, trackFreedRefCounts, debugLogger);
    }

    /**
     * Update list of stack traces for address. Hooked SUT should see that the list changed.
     */
    @Override
    protected void getReferenceCountInfoTestHook(
        ConcurrentMap<Long, List<RefCountChangeInfo>> stackTraces, long address) {
      List<RefCountChangeInfo> updatedList =
          new ArrayList<>(stackTraces.get(address));
      RefCountChangeInfo refCountChangeInfo = new RefCountChangeInfo(false, 0, "TestOwner");
      updatedList.add(refCountChangeInfo);
      stackTraces.put(address, updatedList);
    }

    /**
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
