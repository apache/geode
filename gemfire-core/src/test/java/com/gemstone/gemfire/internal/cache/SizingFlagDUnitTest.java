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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * A test of the when we will use the object sizer to determine 
 * the actual size of objects wrapped in CacheDeserializables.
 * 
 * 
 * @author dsmith
 * 
 * TODO - I was intending to add tests that have an
 * index and object sizer, but it appears we don't support
 * indexes on regions with overflow to disk.
 *
 */
public class SizingFlagDUnitTest extends CacheTestCase {

  /**
   * @param name
   */
  public SizingFlagDUnitTest(String name) {
    super(name);
  }
  
  public void testRRMemLRU() {
    doRRMemLRUTest();
  }
  
  public void testRRMemLRUDeltaAndFlag() {
    doRRMemLRUDeltaTest(true);
  }
  
  public void testRRMemLRUDelta() {
    doRRMemLRUDeltaTest(false);
  }
  
  public void testRRListener() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createRR(vm0);
    createRR(vm1);
    
    addListener(vm0);
    addListener(vm1);
    
    doListenerTestRR(vm0, vm1);
  }
  
  public void testPRMemLRU() {
    doPRMemLRUTest();
  }
  
  public void testPRMemLRUAndFlagDeltaPutOnPrimary() {
    doPRDeltaTestLRU(false, false, true, false);
  }
  
  public void testPRMemLRUDeltaPutOnPrimary() {
    doPRDeltaTestLRU(false, false, true, false);
  }
  
  public void testPRMemLRUAndFlagDeltaPutOnSecondary() {
    doPRDeltaTestLRU(false, false, false, true);
  }
  
  public void testPRMemLRUDeltaPutOnSecondary() {
    doPRDeltaTestLRU(false, false, false, true);
  }
  
  public void testPRNoLRUDelta() {
    doPRNoLRUDeltaTest(false);
  }
  
  public void testPRNoLRUAndFlagDelta() {
    doPRNoLRUDeltaTest(true);
  }
  
  public void testPRListener() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPR(vm0, true);
    createPR(vm1, true);
    
    addListener(vm0);
    addListener(vm1);
    
    doListenerTestPR(vm0, vm1);
  }
  
  public void testPRHeapLRU() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPRHeapLRU(vm0);
    createPRHeapLRU(vm1);
    
    put(vm0, new TestKey("a"), new TestObject(100, 1000));
    
    assertValueType(vm0, new TestKey("a"), ValueType.CD_SERIALIZED);
    assertValueType(vm1, new TestKey("a"), ValueType.CD_SERIALIZED);
    
    assertEquals(1, getObjectSizerInvocations(vm0));
    long origSize0 = getSizeFromPRStats(vm0);
    assertTrue("Size was " + origSize0 , 1000 > origSize0);
    assertEquals(1, getObjectSizerInvocations(vm1));
    long origSize1 = getSizeFromPRStats(vm1);
    assertTrue("Size was " + origSize1 , 1000 > origSize1);
    
    get(vm0, new TestKey("a"), new TestObject(100, 1000));
    
    assertValueType(vm0, new TestKey("a"), ValueType.CD_DESERIALIZED);
    assertValueType(vm1, new TestKey("a"), ValueType.CD_SERIALIZED);
    //assertTrue(1000 < getSizeFromPRStats(vm0));
    assertEquals(3, getObjectSizerInvocations(vm0));
    //assertTrue(1000 > getSizeFromPRStats(vm1));
    assertEquals(1, getObjectSizerInvocations(vm1));
    
    //Test what happens when we reach the heap threshold??
  }
  
  public void testRRHeapLRU() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createRRHeapLRU(vm0);
    createRRHeapLRU(vm1);
    
    put(vm0, "a", new TestObject(100, 1000));
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_SERIALIZED);
    
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    
    get(vm1, "a", new TestObject(100, 1000));
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(1, getObjectSizerInvocations(vm1));
    
    //Test what happens when we reach the heap threshold??
  }
  
  public void testPRHeapLRUDeltaWithFlagPutOnPrimary() {
    doPRDeltaTestLRU(false, true, true, false);
  }  
  
  public void testPRHeapLRUDeltaPutOnPrimary() {
    doPRDeltaTestLRU(false, true, true, false);
  }  

  public void testPRHeapLRUDeltaWithFlagPutOnSecondary() {
    doPRDeltaTestLRU(false, true, false, true);
  }  
  
  public void testPRHeapLRUDeltaPutOnSecondary() {
    doPRDeltaTestLRU(false, true, false, true);
  }  

  // test to cover bug41916
  public void testLargeDelta() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    setDeltaRecalculatesSize(vm0, false);
    setDeltaRecalculatesSize(vm1, false);

    createPR(vm0, false);
    createPR(vm1, false);
    // make a string bigger than socket-buffer-size which defaults to 32k
    int BIG_DELTA_SIZE = 32 * 1024 * 2;
    StringBuilder sb = new StringBuilder(BIG_DELTA_SIZE);
    for (int i=0; i < BIG_DELTA_SIZE; i++) {
      sb.append('7');
    }
    TestDelta delta1 = new TestDelta(true, sb.toString());


    assignPRBuckets(vm0);
    boolean vm0isPrimary = prHostsBucketForKey(vm0, 0);
    if (!vm0isPrimary) {
      assertEquals(true, prHostsBucketForKey(vm1, 0));
    }
    VM primaryVm;
    VM secondaryVm;
    if (vm0isPrimary) {
      primaryVm = vm0;
      secondaryVm = vm1;
    } else {
      primaryVm = vm1;
      secondaryVm = vm0;
    }

    put(secondaryVm, 0, delta1);
  }

  void doPRDeltaTestLRU(boolean shouldSizeChange, boolean heapLRU, boolean putOnPrimary, boolean wasDelta) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    setDeltaRecalculatesSize(vm0, shouldSizeChange);
    setDeltaRecalculatesSize(vm1, shouldSizeChange);

    if (heapLRU) {
      createPRHeapLRU(vm0);
      createPRHeapLRU(vm1);
    } else {// memLRU
      createPR(vm0, true);
      createPR(vm1, true);
    }
    assignPRBuckets(vm0);
    boolean vm0isPrimary = prHostsBucketForKey(vm0, 0);
    if (!vm0isPrimary) {
      assertEquals(true, prHostsBucketForKey(vm1, 0));
    }
    VM primaryVm;
    VM secondaryVm;
    if (vm0isPrimary) {
      primaryVm = vm0;
      secondaryVm = vm1;
    } else {
      primaryVm = vm1;
      secondaryVm = vm0;
    }

    TestDelta delta1 = new TestDelta(false, "12345");
    if (putOnPrimary) {
      put(primaryVm, 0, delta1);
    } else {
      put(secondaryVm, 0, delta1);
    }
    // if the put is done on the primary then it will be CD_DESERIALIZED on the primary
    // otherwise it will be CD_SERIALIZED on the primary.
    if (putOnPrimary) {
      assertValueType(primaryVm, 0, ValueType.CD_DESERIALIZED);
      assertEquals(1, getObjectSizerInvocations(primaryVm));
    } else {
      assertValueType(primaryVm, 0, ValueType.CD_SERIALIZED);
      assertEquals(0, getObjectSizerInvocations(primaryVm));
    }
    // It will always be CD_SERIALIZED on the secondary.
    assertValueType(secondaryVm, 0, ValueType.CD_SERIALIZED);
    assertEquals(0, getObjectSizerInvocations(secondaryVm));
    
    long origEvictionSize0 = getSizeFromEvictionStats(primaryVm);
    long origEvictionSize1 = getSizeFromEvictionStats(secondaryVm);
    long origPRSize0 = getSizeFromPRStats(primaryVm);
    long origPRSize1 = getSizeFromPRStats(secondaryVm);
    delta1.info = "1234567890";
    delta1.hasDelta = true;
    //Update the delta
    if (putOnPrimary) {
      put(primaryVm, 0, delta1);
    } else {
      put(secondaryVm, 0, delta1);
    }
    
    assertValueType(primaryVm, 0, ValueType.CD_DESERIALIZED);
    assertValueType(secondaryVm, 0, ValueType.CD_DESERIALIZED);
    
    if (shouldSizeChange) {
      assertEquals(2, getObjectSizerInvocations(primaryVm));
      // once when we deserialize the value in the cache
      // and once when we size the new value from applying the delta
      assertEquals(2, getObjectSizerInvocations(secondaryVm));
    } else if (wasDelta) {
      assertEquals(0, getObjectSizerInvocations(primaryVm));
      // 1 sizer invoke since the first value needs to be deserialized
      assertEquals(0, getObjectSizerInvocations(secondaryVm));
    } else {
      assertEquals(1, getObjectSizerInvocations(primaryVm));
      // 1 sizer invoke since the first value needs to be deserialized
      assertEquals(0, getObjectSizerInvocations(secondaryVm));
    }
    
    long finalEvictionSize0 = getSizeFromEvictionStats(primaryVm);
    long finalEvictionSize1 = getSizeFromEvictionStats(secondaryVm);
    long finalPRSize0 = getSizeFromPRStats(primaryVm);
    long finalPRSize1 = getSizeFromPRStats(secondaryVm);
    if(shouldSizeChange) {
      //I'm not sure what the change in size should be, because we went
      //from serialized to deserialized
      assertTrue(finalEvictionSize0 - origEvictionSize0 != 0);
      assertTrue(finalPRSize0 - origPRSize0 != 0);
      assertTrue(finalEvictionSize1 - origEvictionSize1 != 0);
      assertTrue(finalPRSize1 - origPRSize1 != 0);
    } else {
      assertEquals(0, finalEvictionSize1 - origEvictionSize1);
      assertEquals(0, finalPRSize0 - origPRSize0);
      assertEquals(0, finalPRSize1 - origPRSize1);
    }
  }
  
  private void addListener(VM vm) {
    vm.invoke(new SerializableRunnable("Add listener") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        try {
          region.getAttributesMutator().addCacheListener(new TestCacheListener());
        } catch (Exception e) {
          Assert.fail("couldn't create index", e);
        }
      }
    });
  }


  private void doListenerTestRR(VM vm0, VM vm1) {
    assertEquals(0, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    put(vm0, "a", new TestObject(100, 100000));
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(1, getObjectSizerInvocations(vm1));
    
    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);
    
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertTrue(origEvictionSize0 >= 100000);
    assertTrue(origEvictionSize1 >= 100000);
    
    put(vm0, "a", new TestObject(200, 200000));
    assertEquals(2, getObjectSizerInvocations(vm0));
    assertEquals(2, getObjectSizerInvocations(vm1));
    
    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertEquals(100000, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100000, finalEvictionSize1 - origEvictionSize1);
  }
  
  private void doListenerTestPR(VM vm0, VM vm1) {
    assertEquals(0, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    put(vm0, "a", new TestObject(100, 100000));
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(1, getObjectSizerInvocations(vm1));
    
    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);
    long origPRSize0 = getSizeFromPRStats(vm1);
    long origPRSize1 = getSizeFromPRStats(vm1);
    
  
    assertValueType(vm0, "a", ValueType.CD_DESERIALIZED);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertTrue(origEvictionSize1 >= 100000);
    assertTrue(origEvictionSize0 >= 100000);
    assertTrue(origPRSize0 <= 500);
    assertTrue(origPRSize1 <= 500);
    
    put(vm0, "a", new TestObject(200, 200000));
    assertEquals(2, getObjectSizerInvocations(vm0));
    assertEquals(2, getObjectSizerInvocations(vm1));
    
    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);
    long finalPRSize0 = getSizeFromPRStats(vm0);
    long finalPRSize1 = getSizeFromPRStats(vm1);
    
    assertValueType(vm0, "a", ValueType.CD_DESERIALIZED);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertEquals(100000, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100000, finalEvictionSize1 - origEvictionSize1);
    assertEquals(100, finalPRSize0 - origPRSize0);
    assertEquals(100, finalPRSize1 - origPRSize1);
  }


  private void doRRMemLRUTest() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createRR(vm0);
    createRR(vm1);
    put(vm0, "a", new TestObject(100, 100000));
    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);
    put(vm0, "a", new TestObject(200, 200000));
    
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_SERIALIZED);
    assertEquals(2, getObjectSizerInvocations(vm0));
    
    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);
    assertEquals(100000, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100, finalEvictionSize1 - origEvictionSize1);
    
    assertEquals(0, getObjectSizerInvocations(vm1));
    
    //Do a get to make sure we deserialize the object and calculate
    //the size adjustment
    Object v = new TestObject(200, 200000);
    get(vm1, "a", v);
    int vSize = CachedDeserializableFactory.calcSerializedMemSize(v);
    
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    long evictionSizeAfterGet = getSizeFromEvictionStats(vm1);
    assertEquals(1, getObjectSizerInvocations(vm1));
    assertEquals(200000+CachedDeserializableFactory.overhead() - vSize, evictionSizeAfterGet - finalEvictionSize1);
    
    //Do a put that will trigger an eviction if it is deserialized
    put(vm0, "b", new TestObject(100, 1000000));
    
    assertEquals(1, getEvictions(vm0));
    assertEquals(0, getEvictions(vm1));
    
    //Do a get to make sure we deserialize the object and calculate
    //the size adjustment
    get(vm1, "b", new TestObject(100, 1000000));
    assertEquals(1, getEvictions(vm1));
  }
  
  private void doPRMemLRUTest() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createPR(vm0, true);
    createPR(vm1, true);
    put(vm0, 0, new TestObject(100, 100000));
    assertValueType(vm0, 0, ValueType.CD_SERIALIZED);
    assertValueType(vm1, 0, ValueType.CD_SERIALIZED);
    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);
    long origPRSize0 = getSizeFromPRStats(vm0);
    long origPRSize1 = getSizeFromPRStats(vm1);
    put(vm0, 0, new TestObject(200, 200000));
    
    assertEquals(0, getObjectSizerInvocations(vm0));
    
    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalPRSize0 = getSizeFromPRStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);
    long finalPRSize1 = getSizeFromPRStats(vm1);
    assertEquals(100, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100, finalEvictionSize1 - origEvictionSize1);
    assertEquals(100, finalPRSize0 - origPRSize0);
    assertEquals(100, finalPRSize1 - origPRSize1);
    
    assertEquals(0, getObjectSizerInvocations(vm1));

    //Do a get to see if we deserialize the object and calculate
    //the size adjustment
    Object v = new TestObject(200, 200000);
    get(vm0, 0, v);
    int vSize = CachedDeserializableFactory.calcSerializedMemSize(v);
    assertValueType(vm0, 0, ValueType.CD_DESERIALIZED);
    assertValueType(vm1, 0, ValueType.CD_SERIALIZED);
    long evictionSizeAfterGet = getSizeFromEvictionStats(vm0);
    long prSizeAfterGet = getSizeFromPRStats(vm0);
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    assertEquals(200000+CachedDeserializableFactory.overhead() - vSize, evictionSizeAfterGet - finalEvictionSize0);
    assertEquals(0, prSizeAfterGet - finalPRSize0);
    
    //Do a put that will trigger an eviction if it is deserialized
    // It should not be deserialized.
    put(vm0, 113, new TestObject(100, 1024*1024));
    assertValueType(vm0, 113, ValueType.CD_SERIALIZED);
    assertValueType(vm1, 113, ValueType.CD_SERIALIZED);
    long evictionSizeAfterPutVm1 = getSizeFromEvictionStats(vm1);
    
    assertEquals(0, getEvictions(vm0));
    assertEquals(0, getEvictions(vm1));
    
    //Do a get to make sure we deserialize the object and calculate
    //the size adjustment which should force an eviction
    get(vm1, 113, new TestObject(100, 1024*1024));
    long evictionSizeAfterGetVm1 = getSizeFromEvictionStats(vm1);
    assertValueType(vm0, 113, ValueType.CD_SERIALIZED);
    assertValueType(vm1, 113, ValueType.EVICTED);
    assertEquals(1, getObjectSizerInvocations(vm0)); // from the get of key 0 on vm0
    assertEquals(0, getEvictions(vm0));
    assertEquals(1, getObjectSizerInvocations(vm1));
    assertEquals(2, getEvictions(vm1));
  }
  
  private void doRRMemLRUDeltaTest(boolean shouldSizeChange) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    setDeltaRecalculatesSize(vm0, shouldSizeChange);
    setDeltaRecalculatesSize(vm1, shouldSizeChange);
    
    createRR(vm0);
    createRR(vm1);
    TestDelta delta1 = new TestDelta(false, "12345");
    put(vm0, "a", delta1);
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_SERIALIZED);
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    
    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);
    delta1.info = "1234567890";
    delta1.hasDelta = true;
    //Update the delta
    put(vm0, "a", delta1);
    
    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    
    assertEquals(2, getObjectSizerInvocations(vm0));
    
    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);
    assertEquals(5, finalEvictionSize0 - origEvictionSize0);
    if(shouldSizeChange) {
      assertEquals(1, getObjectSizerInvocations(vm1));
      //I'm not sure what the change in size should be, because we went
      //from serialized to deserialized
      assertTrue(finalEvictionSize1 - origEvictionSize1 != 0);
    } else {
      // we invoke the sizer once when we deserialize the original to apply the delta to it
      assertEquals(0, getObjectSizerInvocations(vm1));
      assertEquals(0, finalEvictionSize1 - origEvictionSize1);
    }
  }
  
  private void doPRNoLRUDeltaTest(boolean shouldSizeChange) {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    setDeltaRecalculatesSize(vm0, shouldSizeChange);
    setDeltaRecalculatesSize(vm1, shouldSizeChange);

    createPR(vm0, false);
    createPR(vm1, false);
    TestDelta delta1 = new TestDelta(false, "12345");
    put(vm0, "a", delta1);
    long origPRSize0 = getSizeFromPRStats(vm0);
    long origPRSize1 = getSizeFromPRStats(vm1);
    delta1.info = "1234567890";
    delta1.hasDelta = true;
    //Update the delta
    put(vm0, "a", delta1);
    
    
    long finalPRSize0 = getSizeFromPRStats(vm0);
    long finalPRSize1 = getSizeFromPRStats(vm1);
    if(shouldSizeChange) {
      //I'm not sure what the change in size should be, because we went
      //from serialized to deserialized
      assertTrue(finalPRSize0 - origPRSize0 != 0);
      assertTrue(finalPRSize1 - origPRSize1 != 0);
    } else {
      assertEquals(0, finalPRSize0 - origPRSize0);
      assertEquals(0, finalPRSize1 - origPRSize1);
    }
  }

  /**
   * @param vm0
   * @return
   */
  private long getSizeFromPRStats(VM vm0) {
    return (Long) vm0.invoke(new SerializableCallable() {

      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
        if(region instanceof PartitionedRegion) {
          long total = 0;
          PartitionedRegion pr = ((PartitionedRegion) region);
          for(int i =0; i < pr.getPartitionAttributes().getTotalNumBuckets(); i++) {
            total += pr.getDataStore().getBucketSize(i);
          }
          return total;
        } else {
          return 0L;
        }
      }
    });
  }
  
  

  private long getSizeFromEvictionStats(VM vm0) {
    return (Long) vm0.invoke(new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
        return getSizeFromEvictionStats(region);
      }
    });
  }
  
  private long getEvictions(VM vm0) {
    return (Long) vm0.invoke(new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
        return getEvictions(region);
      }
    });
  }
  
  private int getObjectSizerInvocations(VM vm0) {
    return (Integer) vm0.invoke(new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
        return getObjectSizerInvocations(region);
      }
    });
  }

  private void assignPRBuckets(VM vm) {
    vm.invoke(new SerializableRunnable("assignPRBuckets") {
      public void run() {
        Cache cache = getCache();
        PartitionRegionHelper.assignBucketsToPartitions(cache.getRegion("region"));
      }
    });
  }

  private boolean prHostsBucketForKey(VM vm, final Object key) {
    Boolean result = (Boolean)vm.invoke(new SerializableCallable("prHostsBucketForKey") {
      public Object call() {
        Cache cache = getCache();
        DistributedMember myId = cache.getDistributedSystem().getDistributedMember();
        Region region = cache.getRegion("region");
        DistributedMember hostMember = PartitionRegionHelper.getPrimaryMemberForKey(region, key);
        if (hostMember == null) {
          throw new IllegalStateException("bucket for key " + key + " is not hosted!");
        }
        boolean res = Boolean.valueOf(myId.equals(hostMember));
//         cache.getLogger().info("DEBUG prHostsBucketForKey=" + res);
        return res;
      }
    });
    return result.booleanValue();
  }
  
  private void put(VM vm0, final Object key, final Object value) {
    vm0.invoke(new SerializableRunnable("Put data") {
      
      public void run() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
//         cache.getLogger().info("DEBUG about to put(" + key + ", " + value + ")");
        region.put(key, value);
      }
    });
  }
  
  private void get(VM vm0, final Object key, final Object value) {
    vm0.invoke(new SerializableRunnable("Put data") {
      
      public void run() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
        assertEquals(value, region.get(key));
      }
    });
  }

  protected int getObjectSizerInvocations(LocalRegion region) {
    TestObjectSizer sizer = (TestObjectSizer) region.getEvictionAttributes().getObjectSizer();
    int result = sizer.invocations.get();
     region.getCache().getLogger().info("objectSizerInvocations=" + result);
    return result;
  }

  private long getSizeFromEvictionStats(LocalRegion region) {
    long result = region.getEvictionController().getLRUHelper().getStats().getCounter();
//     region.getCache().getLogger().info("DEBUG evictionSize=" + result);
    return result;
  }
  
  private long getEvictions(LocalRegion region) {
    return region.getEvictionController().getLRUHelper().getStats().getEvictions();
  }

  private void setDeltaRecalculatesSize(VM vm, final boolean shouldSizeChange) {
    vm.invoke(new SerializableRunnable("setDeltaRecalculatesSize") {
      public void run() {
        GemFireCacheImpl.DELTAS_RECALCULATE_SIZE = shouldSizeChange;
      }
    });
  }
  
  private void createRR(VM vm) {
    vm.invoke(new SerializableRunnable("Create rr") {
      public void run() {
        Cache cache = getCache();
        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
        attr.setDiskSynchronous(true);
        attr.setDataPolicy(DataPolicy.REPLICATE);
        attr.setScope(Scope.DISTRIBUTED_ACK);
        attr.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(1, new TestObjectSizer(), EvictionAction.OVERFLOW_TO_DISK));
        attr.setDiskDirs(getMyDiskDirs());
        Region region = cache.createRegion("region", attr.create());
      }
    });
    
  }
  
  private void assertValueType(VM vm, final Object key, final ValueType expectedType) {
    vm.invoke(new SerializableRunnable("Create rr") {
      public void run() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion("region");
        Object value = region.getValueInVM(key);
        switch (expectedType) {
          case RAW_VALUE:
            assertTrue("Value was " + value + " type " + value.getClass(), !(value instanceof CachedDeserializable));
            break;
          case CD_SERIALIZED:
            assertTrue("Value was " + value + " type " + value.getClass(), value instanceof CachedDeserializable);
            assertTrue("Value not serialized", ((CachedDeserializable)value).getValue() instanceof byte[]);
            break;
          case CD_DESERIALIZED:
            assertTrue("Value was " + value + " type " + value.getClass(), value instanceof CachedDeserializable);
            assertTrue("Value was serialized", !(((CachedDeserializable)value).getValue() instanceof byte[]));
            break;
          case EVICTED:
            assertEquals(null, value);
            break;
        }
      }
    });
  }
  
  private File[] getMyDiskDirs() {
    long random = new Random().nextLong();
    File file = new File(Long.toString(random));
    file.mkdirs();
    return new File[] { file};
  }
  
  private void createPR(VM vm, final boolean enableLRU) {
    vm.invoke(new SerializableRunnable("Create pr") {
      public void run() {
        Cache cache = getCache();
        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
        attr.setDiskSynchronous(true);
        PartitionAttributesFactory<Integer, TestDelta> paf = new PartitionAttributesFactory<Integer, TestDelta>();
        paf.setRedundantCopies(1);
        if(enableLRU) {
          paf.setLocalMaxMemory(1); // memlru limit is 1 megabyte
          attr.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(new TestObjectSizer(), EvictionAction.OVERFLOW_TO_DISK));
          attr.setDiskDirs(getMyDiskDirs());
        }
        PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PARTITION);
        attr.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        Region<Integer, TestDelta> region = cache.createRegion("region", attr.create());
      }
    });
    
  }
  
  private void createRRHeapLRU(VM vm) {
    vm.invoke(new SerializableRunnable("Create rr") {
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        manager.setCriticalHeapPercentage(95);
        manager.setEvictionHeapPercentage(90);
        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
        attr.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(
            new TestObjectSizer(), EvictionAction.OVERFLOW_TO_DISK));
        attr.setDiskDirs(getMyDiskDirs());
        attr.setDataPolicy(DataPolicy.REPLICATE);
        attr.setScope(Scope.DISTRIBUTED_ACK);
        attr.setDiskDirs(getMyDiskDirs());
        Region region = cache.createRegion("region", attr.create());
      }
    });
    
  }
  
  private void createPRHeapLRU(VM vm) {
    vm.invoke(new SerializableRunnable("Create pr") {
      public void run() {
        Cache cache = getCache();
        ResourceManager manager = cache.getResourceManager();
        manager.setCriticalHeapPercentage(95);
        manager.setEvictionHeapPercentage(90);

        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
        PartitionAttributesFactory<Integer, TestDelta> paf = new PartitionAttributesFactory<Integer, TestDelta>();
        paf.setRedundantCopies(1);
        attr.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(
            new TestObjectSizer(), EvictionAction.LOCAL_DESTROY));
        PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PARTITION);
        attr.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        Region<Integer, TestDelta> region = cache.createRegion("region", attr.create());
      }
    });
    
  }

  private static class TestObjectSizer implements ObjectSizer {
    private AtomicInteger invocations = new AtomicInteger();

    public int sizeof(Object o) {
      if (InternalDistributedSystem.getLoggerI18n().fineEnabled()) {
        InternalDistributedSystem.getLoggerI18n().fine("TestObjectSizer invoked"/*, new Exception("stack trace")*/);
      }
      if(o instanceof TestObject) {
//         com.gemstone.gemfire.internal.cache.GemFireCache.getInstance().getLogger().info("DEBUG TestObjectSizer: sizeof o=" + o, new RuntimeException("STACK"));
        invocations.incrementAndGet();
        return ((TestObject) o).sizeForSizer;
      }
      if(o instanceof TestDelta) {
//         com.gemstone.gemfire.internal.cache.GemFireCache.getInstance().getLogger().info("DEBUG TestObjectSizer: sizeof delta o=" + o, new RuntimeException("STACK"));
        invocations.incrementAndGet();
        return ((TestDelta) o).info.length();
      }
      //This is the key. Why don't we handle Integers internally?
      if(o instanceof Integer) {
        return 0;
      }
      if(o instanceof TestKey) {
//         com.gemstone.gemfire.internal.cache.GemFireCache.getInstance().getLogger().info("DEBUG TestObjectSizer: sizeof TestKey o=" + o, new RuntimeException("STACK"));
        invocations.incrementAndGet();
        return ((TestKey) o).value.length();
      }
      throw new RuntimeException("Unpected type to be sized " + o.getClass() + ", object=" + o);
    }
  }
  
  private static class TestKey implements DataSerializable {
    String value;
    
    public TestKey() {
      
    }

    public TestKey(String value) {
      this.value = value;
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      value = DataSerializer.readString(in);
    }

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(value, out);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof TestKey))
        return false;
      TestKey other = (TestKey) obj;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }
    
  }
  
  private static class TestObject implements DataSerializable {
    public int sizeForSizer;
    public int sizeForSerialization;

    public TestObject() {
      
    }
    

    public TestObject(int sizeForSerialization, int sizeForSizer) {
      super();
      this.sizeForSizer = sizeForSizer;
      this.sizeForSerialization = sizeForSerialization;
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      sizeForSizer = in.readInt();
      sizeForSerialization = in.readInt();
      //We don't actually need these things.
      in.skipBytes(sizeForSerialization);
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(sizeForSizer);
      out.writeInt(sizeForSerialization);
      out.write(new byte[sizeForSerialization]);
      
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + sizeForSerialization;
      result = prime * result + sizeForSizer;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof TestObject))
        return false;
      TestObject other = (TestObject) obj;
      if (sizeForSerialization != other.sizeForSerialization)
        return false;
      if (sizeForSizer != other.sizeForSizer)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "TestObject [sizeForSerialization=" + sizeForSerialization
          + ", sizeForSizer=" + sizeForSizer + "]";
    }
  }
  
  public static class TestCacheListener extends CacheListenerAdapter {

    @Override
    public void afterCreate(EntryEvent event) {
      //Make sure we deserialize the new value
      event.getRegion().getCache().getLoggerI18n().fine("invoked afterCreate with " + event);
      event.getRegion().getCache().getLoggerI18n().info(LocalizedStrings.DEBUG, "value is " + event.getNewValue());
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      //Make sure we deserialize the new value
      event.getRegion().getCache().getLoggerI18n().fine("invoked afterUpdate with ");
      event.getRegion().getCache().getLoggerI18n().info(LocalizedStrings.DEBUG, "value is " + event.getNewValue());
    }
    
  }
  
  enum ValueType {
    RAW_VALUE,
    CD_SERIALIZED,
    CD_DESERIALIZED,
    EVICTED  
  }

}
