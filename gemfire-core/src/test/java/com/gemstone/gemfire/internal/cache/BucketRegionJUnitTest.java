/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DSClock;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;
import static org.mockito.Mockito.*;

import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author gzhou
 *
 */
@Category(UnitTest.class)
public class BucketRegionJUnitTest extends TestCase {

  private static final Logger logger = LogService.getLogger();
  GemFireCacheImpl cache = null;
  InternalDistributedMember member = null;
  PartitionedRegion pr = null;
  BucketAdvisor ba = null;
  RegionAttributes ra = null;
  InternalRegionArguments ira = null;
  BucketRegion br = null;
  
  private EntryEventImpl prepare(boolean isConcurrencyChecksEnabled) {
    // mock cache, distributed system, distribution manager, and distributed member
    cache = mock(GemFireCacheImpl.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    DistributionManager dm = mock(DistributionManager.class);
    member = mock(InternalDistributedMember.class);
    InvalidateOperation io = mock(InvalidateOperation.class);

    // mock PR, bucket advisor, DSClock 
    pr = mock(PartitionedRegion.class);
    ba = mock(BucketAdvisor.class);
    DSClock clock = mock(DSClock.class);

    // create some real objects
    DistributionConfigImpl config = new DistributionConfigImpl(new Properties());
    ReadWriteLock primaryMoveLock = new ReentrantReadWriteLock();
    Lock activeWriteLock = primaryMoveLock.readLock();
    GemFireCacheImpl.Stopper stopper = mock(GemFireCacheImpl.Stopper.class);

    // define the mock objects' behaviors
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.getClock()).thenReturn(clock);
    when(ids.getDistributionManager()).thenReturn(dm);
    when(ids.getDistributedMember()).thenReturn(member);
    when(dm.getConfig()).thenReturn(config);
    when(member.getRoles()).thenReturn(new HashSet());
    when(ba.getActiveWriteLock()).thenReturn(activeWriteLock);
    when (cache.getCancelCriterion()).thenReturn(stopper);
    when(ids.getCancelCriterion()).thenReturn(stopper);

    // create region attributes and internal region arguments
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(isConcurrencyChecksEnabled); //
    ra = factory.create();
    ira = new InternalRegionArguments().setPartitionedRegion(pr)
        .setPartitionedRegionBucketRedundancy(1)
        .setBucketAdvisor(ba);
    // create a bucket region object
    br = new BucketRegion("testRegion", ra, null, cache, ira);
    
    // since br is a real bucket region object, we need to tell mockito to monitor it
    br = Mockito.spy(br);

    doNothing().when(br).distributeUpdateOperation(any(), anyLong());
    doNothing().when(br).distributeDestroyOperation(any());
    doNothing().when(br).checkForPrimary();
    doNothing().when(br).handleWANEvent(any());
    doReturn(false).when(br).needWriteLock(any());
    doReturn(true).when(br).hasSeenEvent(any(EntryEventImpl.class));
    
    EntryEventImpl event = createDummyEvent();
    return event;
  }
  
  private EventID createDummyEventID() {
    byte[] memId = { 1,2,3 };
    EventID eventId = new EventID(memId, 11, 12, 13);
    return eventId;
  }

  private EntryEventImpl createDummyEvent() {
    // create a dummy event id
    EventID eventId = createDummyEventID();
    String key = "key1";
    String value = "Value1";

    // create an event
    EntryEventImpl event = EntryEventImpl.create(br, Operation.CREATE, key,
        value, null,  false /* origin remote */, null,
        false /* generateCallbacks */,
        eventId);

    return event;
  }
  
  private VersionTag createVersionTag(boolean invalid) {
    VersionTag tag = VersionTag.create(member);
    if (invalid == false) {
      tag.setRegionVersion(1);
      tag.setEntryVersion(1);
    }
    return tag;
  }
  
  private void doTest(EntryEventImpl event, int cnt) {
    // do the virtualPut test
    br.virtualPut(event, false, false, null, false, 12345L, false);
    // verify the result
    if (cnt > 0) {
      verify(br, times(cnt)).distributeUpdateOperation(eq(event), eq(12345L));
    } else {
      verify(br, never()).distributeUpdateOperation(eq(event), eq(12345L));
    }
    
    // do the basicDestroy test
    br.basicDestroy(event, false, null);
    // verify the result
    if (cnt > 0) {
      verify(br, times(cnt)).distributeDestroyOperation(eq(event));
    } else {
      verify(br, never()).distributeDestroyOperation(eq(event));
    }
    
  }
  
  @Test
  public void testConcurrencyFalseTagNull() {
    // case 1: concurrencyCheckEanbled = false, version tag is null: distribute
    EntryEventImpl event = prepare(false);
    assertNull(event.getVersionTag());
    doTest(event, 1);
  }

  @Test
  public void testConcurrencyTrueTagNull() {
    // case 2: concurrencyCheckEanbled = true,  version tag is null: not to distribute
    EntryEventImpl event = prepare(true);
    assertNull(event.getVersionTag());
    doTest(event, 0);
  }
  
  @Test
  public void testConcurrencyTrueTagInvalid() {
    // case 3: concurrencyCheckEanbled = true,  version tag is invalid: not to distribute
    EntryEventImpl event = prepare(true);
    VersionTag tag = createVersionTag(true);
    event.setVersionTag(tag);
    assertFalse(tag.hasValidVersion());
    doTest(event, 0);
  }
    
  @Test
  public void testConcurrencyTrueTagValid() {
    // case 4: concurrencyCheckEanbled = true,  version tag is valid: distribute
    EntryEventImpl event = prepare(true);
    VersionTag tag = createVersionTag(false);
    event.setVersionTag(tag);
    assertTrue(tag.hasValidVersion());
    doTest(event, 1);
  }


}

