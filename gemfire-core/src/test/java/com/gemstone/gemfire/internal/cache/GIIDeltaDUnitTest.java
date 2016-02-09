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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.DestroyOperation.DestroyMessage;
import com.gemstone.gemfire.internal.cache.DistributedCacheOperation;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.DistributedTombstoneOperation.TombstoneMessage;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHook;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHookType;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.RequestImageMessage;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.RequestRVVMessage;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.TombstoneService;
import com.gemstone.gemfire.internal.cache.UpdateOperation.UpdateMessage;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * @author Xiaojian Zhou (Gester)
 *
 */
public class GIIDeltaDUnitTest extends CacheTestCase {

  VM P; // GII provider
  VM R; // GII requester
  InternalDistributedMember R_ID; // DistributedMember ID of R
  
  private static final long MAX_WAIT = 30000;
//  protected static String REGION_NAME = GIIDeltaDUnitTest.class.getSimpleName()+"_Region";
  protected static String REGION_NAME = "_Region";
  final String expectedExceptions = GemFireIOException.class.getName();
  protected IgnoredException expectedEx;
  static Object giiSyncObject = new Object();
  
  /**
   * @param name
   */
  public GIIDeltaDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Invoke.invokeInEveryVM(GIIDeltaDUnitTest.class,"setRegionName", new Object[]{getUniqueName()});
    setRegionName(getUniqueName());
  }
  
  public static void setRegionName(String testName) {
    REGION_NAME = testName + "_Region";
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    P.invoke(GIIDeltaDUnitTest.class, "resetSlowGII");
    R.invoke(GIIDeltaDUnitTest.class, "resetSlowGII");
    P.invoke(InitialImageOperation.class, "resetAllGIITestHooks");
    R.invoke(InitialImageOperation.class, "resetAllGIITestHooks");
    changeUnfinishedOperationLimit(R, 10000);
    changeForceFullGII(R, false, false);
    changeForceFullGII(P, false, false);
    P = null;
    R = null;
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    // clean up the test hook, which can be moved to CacheTestCase
    DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;
    if (expectedEx != null) {
      expectedEx.remove();
    }
  }

//  private VersionTag getVersionTag(VM vm, final String key) {
//    SerializableCallable getVersionTag = new SerializableCallable("verify recovered entry") {
//      public Object call() {
//        VersionTag tag = CCRegion.getVersionTag(key);
//        return tag;
//
//      }
//    };
//    return (VersionTag)vm.invoke(getVersionTag);
//  }
  
  public InternalDistributedMember getDistributedMemberID(VM vm) {
    SerializableCallable getID = new SerializableCallable("get member id") {
      public Object call() {
        return getCache().getDistributedSystem().getDistributedMember();
      }
    };
    InternalDistributedMember id = (InternalDistributedMember)vm.invoke(getID);
    return id;
  }
  
  protected void prepareForEachTest() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createDistributedRegion(vm0);
    createDistributedRegion(vm1);
    assignVMsToPandR(vm0, vm1);
    // from now on, use P and R as vmhttps://wiki.gemstone.com/display/gfepersistence/DeltaGII+Spec+for+8.0
    expectedEx = IgnoredException.addIgnoredException(expectedExceptions);
  }
  
  // these steps are shared by all test cases
  private void prepareCommonTestData(int p_version) {
    // operation P1: region.put("key") at P. After the operation, region version for P becomes 1
    R_ID = getDistributedMemberID(R);
    
    assertDeltaGIICountBeZero(P);
    assertDeltaGIICountBeZero(R);
    doOnePut(P, 1, "key1");
    doOnePut(R, 1, "key2");
    doOnePut(R, 2, "key5");

    createConflictOperationsP2R3();
    
    for (int i=3; i<=p_version; i++) {
      switch(i) {
      case 3:
        doOneDestroy(P, 3, "key1");
        break;
      case 4:
        doOneDestroy(P, 4, "key2");
        break;
      case 5:
        doOnePut(P, 5, "key1");
        break;
      case 6:
        doOnePut(P, 6, "key3");
      default:
        // donothing
      }
    }
    // at this moment, cache has key1, key3, key5 
  }
  
  private void createConflictOperationsP2R3() {
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long blocklist[] = {2,3};
    
    P.invoke(GIIDeltaDUnitTest.class, "slowGII", new Object[] {blocklist});
    R.invoke(GIIDeltaDUnitTest.class, "slowGII", new Object[] {blocklist});
    AsyncInvocation async1 = doOnePutAsync(P, 2, "key1");
    AsyncInvocation async2 = doOnePutAsync(R, 3, "key1");
    
    // wait for the local puts are done at P & R before distribution
    waitForToVerifyRVV(P, memberP, 2, null, 0); // P's rvv=p2, gc=0
    waitForToVerifyRVV(P, memberR, 2, null, 0); // P's rvv=r2, gc=0
    waitForToVerifyRVV(R, memberP, 1, null, 0); // P's rvv=p2, gc=0
    waitForToVerifyRVV(R, memberR, 3, null, 0); // P's rvv=r2, gc=0
    
    // new Object[] { memberP, 2, 3, 0, 0, false }
    P.invoke(GIIDeltaDUnitTest.class, "resetSlowGII");
    R.invoke(GIIDeltaDUnitTest.class, "resetSlowGII");
    
    // should wait for async calls to finish before going on
    checkAsyncCall(async1);
    checkAsyncCall(async2);
    
    // verify RVVs
    waitForToVerifyRVV(P, memberP, 2, null, 0); // P's rvv=p2, gc=0
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    waitForToVerifyRVV(R, memberP, 2, null, 0); // P's rvv=p2, gc=0
    waitForToVerifyRVV(R, memberR, 3, null, 0); // P's rvv=r3, gc=0
    
    // verify P won the conflict check
    waitToVerifyKey(P, "key1", generateValue(P));
    waitToVerifyKey(R, "key1", generateValue(P));
  }
  
  private void createUnfinishedOperationsR4R5() {
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};
    
    // let r6 to succeed, r4,r5 to be blocked
    R.invoke(GIIDeltaDUnitTest.class, "slowGII", new Object[] {exceptionlist});
    AsyncInvocation async1 = doOnePutAsync(R, 4, "key4");
    waitForToVerifyRVV(R, memberR, 4, null, 0); // P's rvv=r4, gc=0
    
    AsyncInvocation async2 = doOneDestroyAsync(R, 5, "key5");
    waitForToVerifyRVV(R, memberR, 5, null, 0); // P's rvv=r5, gc=0
    
    doOnePut(R, 6, "key1"); // r6 will pass
    waitForToVerifyRVV(R, memberR, 6, null, 0); // P's rvv=r6, gc=0

    // P should have exception list R4,R5 now
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
  }
  
  /**
   * vm0 and vm1 are peers, each holds a DR.
   * Each does a few operations to make RVV=P7,R6, RVVGC=P4,R0 for both members.   
   * vm1 becomes offline then restarts. 
   * The deltaGII should only exchange RVV. No need to send data from vm0 to vm1.  
   */
  public void testDeltaGIIWithSameRVV() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);

    // force tombstone GC to let RVVGC to become P4:R0
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0

    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");
    
    // let p7 to succeed
    doOnePut(P, 7, "key1");

    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0

    waitForToVerifyRVV(R, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, null, 0); // P's rvv=r6, gc=0
    // now the rvv and rvvgc at P and R should be the same
    
    // save R's rvv in byte array, check if it will be fullGII
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    // shutdown R and restart
    closeCache(R);
    
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, null, 0); // P's rvv=r6, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same

    // If fullGII, the key size in gii chunk is 4, i.e. key1,key3,key4,key5(tombstone). key2 is GCed. 
    // If delta GII, the key size should be 0
    verifyDeltaSizeFromStats(R, 0, 1);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list.
   * Before GII, P's RVV is P7,R6(3-6), R's RVV is P6,R6, RVVGC are both P4,R0   
   * vm1 becomes offline then restarts. https://wiki.gemstone.com/display/gfepersistence/DeltaGII+Spec+for+8.0
   * The deltaGII should send delta to R, revoke unfinished opeation R4,R5  
   */
  public void testDeltaGIIWithExceptionList() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    VersionTag expect_tag = getVersionTag(R, "key5");

    // force tombstone GC to let RVVGC to become P4:R0
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0

    createUnfinishedOperationsR4R5();
    
    // now P's cache still only has key1, key3, key5
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    // p7 only apply at P
    doOnePut(P, 7, "key1");

    // restart and gii
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv);
    
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    // If delta GII, the key size should be 2, i.e. P7(key1) and (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 2, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    
    // restart P, since R has received exceptionlist R4,R5 from P
    closeCache(P);
    createDistributedRegion(P);
    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6, gc=0
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key4 is removed as unfinished op
    // If deltaGII, the key size should be 0
    verifyDeltaSizeFromStats(P, 0, 1);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list.
   * Before GII, P's RVV is P6,R6(3-6), R's RVV is P6,R6, RVVGC are both P4,R0   
   * vm1 becomes offline then restarts.
   * The deltaGII should send delta which only contains unfinished opeation R4,R5  
   */
  public void testDeltaGIIWithOnlyUnfinishedOp() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    VersionTag expect_tag = getVersionTag(R, "key5");

    // force tombstone GC to let RVVGC to become P4:R0
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0

    createUnfinishedOperationsR4R5();
    
    // now P's cache still only has key1, key3, key5
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    // restart and gii
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 6, null, 4); // R's rvv=p6, gc=4
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv);
    
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    // If delta GII, the key size should be 1 (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 1, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    
    // restart P, since R has received exceptionlist R4,R5 from P
    closeCache(P);
    createDistributedRegion(P);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6, gc=0
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key4 is removed as unfinished op
    // If deltaGII, the key size should be 0
    verifyDeltaSizeFromStats(P, 0, 1);
    
    // restart R, to make sure the unfinished op is handled correctly
    forceGC(R, 1); // for bug 47616
    waitForToVerifyRVV(P, memberR, 6, null, 5); // P's rvv=R6, gc=5
    waitForToVerifyRVV(R, memberR, 6, null, 5); // P's rvv=R6, gc=5
    
    closeCache(R);
    createDistributedRegion(R);
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key4 is removed as unfinished op
    // If deltaGII, the key size should be 0
    verifyDeltaSizeFromStats(R, 0, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
  }

  /**
   * This is to test a race condition for bug#47616
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list.
   * Before GII, P's RVV is P6,R6(3-6), R's RVV is P6,R6, RVVGC are both P4,R0   
   * vm1 becomes offline then restarts.
   * The deltaGII should send delta which only contains unfinished opeation R4,R5  
   */
  public void testDeltaGIIWithOnlyUnfinishedOp_GCAtR() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    VersionTag expect_tag = getVersionTag(R, "key5");

    // force tombstone GC to let RVVGC to become P4:R0
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0

    createUnfinishedOperationsR4R5();
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6, gc=0
    
    // 2
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterRequestRVV = new Mycallback(GIITestHookType.AfterRequestRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterRequestRVV);
      }
    });

    // now P's cache still only has key1, key3, key5
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    
    // restart and gii
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    
    // 2
    waitForCallbackStarted(R, GIITestHookType.AfterRequestRVV);
    forceGC(R, 1);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterRequestRVV, true});
    
    async3.join(MAX_WAIT);
    
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    verifyTombstoneExist(R, "key5", false, false);
    
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    // If delta GII, the key size should be 1 (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 3, 0);
  }
  
  /**
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list. Then shutdown R. Do tombstone GC at P only.
   * Before GII, P's RVV is P7,R6(3-6), RVVGC is P4,R0; R's RVV is P6,R6, RVVGC are both P0,R0   
   * vm1 becomes offline then restarts. https://wiki.gemstone.com/display/gfepersistence/DeltaGII+Spec+for+8.0
   * The deltaGII should send delta to R, revoke unfinished opeation R4,R5  
   */
  public void testDeltaGIIWithDifferentRVVGC() throws Throwable {
    final String testcase = "testDeltaGIIWithExceptionList";
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    waitForToVerifyRVV(P, memberP, 6, null, 0); // P's rvv=p6, gc=0
    VersionTag expect_tag = getVersionTag(R, "key5");

    createUnfinishedOperationsR4R5();
    
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    
    // force tombstone GC to let RVVGC to become P4:R0
    changeTombstoneTimout(R, MAX_WAIT);
    changeTombstoneTimout(P, MAX_WAIT);
    Wait.pause((int)MAX_WAIT);
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4

    // p7 only apply at P
    doOnePut(P, 7, "key1");
    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p7, gc=4
    verifyTombstoneExist(P, "key2", false, false);
    
    // restart R and gii
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv);
    
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    // If delta GII, the key size should be 2, i.e. P7 (key1) and (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 2, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    verifyTombstoneExist(R, "key2", false, false);
    verifyTombstoneExist(P, "key2", false, false);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * Let provider to have higher RVVGC than requester's RVV
   * It should trigger fullGII
   * 
   * This test also verify tombstoneGC happened in middle of GII, but BEFORE GII thread got GIILock.
   * i.e. before GII, P's RVVGC=P0,R0, upon received RequestImageMessage, it becomes P4,R0
   * it should cause the fullGII. 
   */
  public void testFullGIITriggeredByHigherRVVGC() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(3);
    waitForToVerifyRVV(P, memberP, 3, null, 0); // P's rvv=p3, gc=0
    
    createUnfinishedOperationsR4R5();
    waitForToVerifyRVV(R, memberP, 3, null, 0); // R's rvv=p3, gc=0
    
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    // p4-7 only apply at P
    doOneDestroy(P, 4, "key2");
    doOnePut(P, 5, "key1");
    doOnePut(P, 6, "key3");
    doOnePut(P, 7, "key1");

    
    // add test hook
    // 7  
    P.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterReceivedRequestImage = new Mycallback(GIITestHookType.AfterReceivedRequestImage, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterReceivedRequestImage);
      }
    });

    // restart R and gii, it will be blocked at test hook
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    // 7
    waitForCallbackStarted(P, GIITestHookType.AfterReceivedRequestImage);

    // force tombstone GC to let RVVGC to become P4:R0, but R already sent its old RVV/RVVGC over
    // this tombstone GC happens BEFORE GII thread got the GIILock, so it will be fullGCC
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, true);

    // let GII continue
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterReceivedRequestImage, true});
    async3.join(MAX_WAIT);

    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv);
    
    // In fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    verifyDeltaSizeFromStats(R, 3, 0);
  }

  /**
   * vm0(P), vm1(R), vm2(T) are peers, each holds a DR.
   * shutdown vm1(R), vm2(T)
   * Let provider to have higher RVVGC than requester's RVV when vm1 and vm2 are offline
   * Restart R, It should trigger fullGII
   * If R did not save the rvvgc, restarted R will have a way smaller rvvgc (maybe the same as T's) 
   * Let T requests GII from R, it wll become deltaGII, which is wrong. 
   */
  public void testSavingRVVGC() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};
    
    Host host = Host.getHost(0);
    VM T = host.getVM(2);
    createDistributedRegion(T);
    final DiskStoreID memberT = getMemberID(T);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(3);
    waitForToVerifyRVV(P, memberP, 3, null, 0); // P's rvv=p3, gc=0
    
    waitForToVerifyRVV(R, memberP, 3, null, 0); // R's rvv=p3, gc=0
    waitForToVerifyRVV(T, memberP, 3, null, 0); // T's rvv=p3, gc=0
    
    closeCache(T);
    closeCache(R);

    // p4-7 only apply at P
    doOneDestroy(P, 4, "key2");
    doOnePut(P, 5, "key1");
    doOnePut(P, 6, "key3");
    doOnePut(P, 7, "key1");
    // force tombstone GC to let RVVGC to become P4:R0
    forceGC(P, 2);
    
    // restart R and gii, it will be blocked at test hook
    createDistributedRegion(R);

    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 3, null, 0); // R's rvv=r3, gc=0

//    RegionVersionVector p_rvv = getRVV(P);
//    RegionVersionVector r_rvv = getRVV(R);
//    assertSameRVV(p_rvv, r_rvv);
    
    // In fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    verifyDeltaSizeFromStats(R, 3, 0);
    verifyTombstoneExist(P, "key2", false, false);
    verifyTombstoneExist(R, "key2", false, false);
    
    closeCache(P);
    closeCache(R);
    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 3, null, 0); // R's rvv=r3, gc=0
    verifyTombstoneExist(R, "key2", false, false);
    
    createDistributedRegion(T);
    waitForToVerifyRVV(T, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(T, memberR, 3, null, 0); // R's rvv=r3, gc=0
    // In fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    verifyDeltaSizeFromStats(T, 3, 0);
    verifyTombstoneExist(T, "key2", false, false);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list. Not to do any tombstone GC
   * Before GII, P's RVV is P7,R6(3-6), R's RVV is P6,R6, RVVGC are both P0,R0   
   * vm1 becomes offline then restarts. https://wiki.gemstone.com/display/gfepersistence/DeltaGII+Spec+for+8.0
   * The deltaGII should send delta to R, revoke unfinished opeation R4,R5  
   */
  public void testDeltaGIIWithoutRVVGC() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    waitForToVerifyRVV(P, memberP, 6, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    VersionTag expect_tag = getVersionTag(R, "key5");

    createUnfinishedOperationsR4R5();
    
    // now P's cache still only has key1, key3
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    // p7 only apply at P
    doOnePut(P, 7, "key1");

    // restart R and gii
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    
    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 7, null, 0); // R's rvv=p7, gc=0
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv);
    
    // If fullGII, the key size in gii chunk is 4. 
    // tombstone counts in deltaSize, so without rvvgc, it's 4: key1, key2(tombstone), key3, key5
    // In delta GII, it should be 1, i.e. P7 (key1) and (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 2, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * unifinished P8: destroy(key1), finished P9: put(key3).
   * Shutdown R, then GC tombstones at P.  
   * P's RVV=P9,R6(3-6), RVVGC=P8,R0, R's RVV=P9(7-9),R6, RVV
   * It should trigger fullGII
   */
  public void testFullGIINotDorminatedByProviderRVVGC() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(3);
    
    createUnfinishedOperationsR4R5();

    waitForToVerifyRVV(P, memberP, 3, null, 0); // P's rvv=p3, gc=0
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
    waitForToVerifyRVV(R, memberP, 3, null, 0); // R's rvv=p3, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0
    
    // p4-7 only apply at P
    doOneDestroy(P, 4, "key2");
    doOnePut(P, 5, "key1");
    doOnePut(P, 6, "key3");
    doOnePut(P, 7, "key1");
    
    final long[] exceptionlist2 = {8};
    // let p9 to succeed, p8 to be blocked
    P.invoke(GIIDeltaDUnitTest.class, "slowGII", new Object[] {exceptionlist2});
    AsyncInvocation async1 = doOneDestroyAsync(P, 8, "key1");
    waitForToVerifyRVV(P, memberP, 8, null, 0);
    doOnePut(P, 9, "key3");
    waitForToVerifyRVV(P, memberP, 9, null, 0);
    waitForToVerifyRVV(R, memberP, 9, exceptionlist2, 0);

    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    forceGC(P, 3);
    // now P's RVV=P9,R6(3-6), RVVGC=P8,R0, R's RVV=P9(7-9), R6
    waitForToVerifyRVV(P, memberP, 9, null, 8); // P's rvv=p9, gc=8
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0

    // restart and gii, R's rvv should be the same as P's
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, true);
    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 9, null, 8); // R's rvv=p9, gc=8
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same
    
    // In fullGII, the key size in gii chunk is 2. They are: key3, key5
    verifyDeltaSizeFromStats(R, 2, 0);
  }

  /**
   * Let R4, R5 unfinish, but R5 is the last operation from R. So P's RVV is 
   * still P:x,R3, without exception list. But actually R4, R5 are unfinished ops by 
   * all means.  
   */
  public void testUnfinishedOpsWithoutExceptionList() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    VersionTag expect_tag = getVersionTag(R, "key5");

    final long[] exceptionlist = {4,5};
    R.invoke(GIIDeltaDUnitTest.class, "slowGII", new Object[] {exceptionlist});
    AsyncInvocation async1 = doOnePutAsync(R, 4, "key4");
    waitForToVerifyRVV(R, memberR, 4, null, 0); // P's rvv=r4, gc=0
    
    AsyncInvocation async2 = doOneDestroyAsync(R, 5, "key5");
    waitForToVerifyRVV(R, memberR, 5, null, 0); // P's rvv=r5, gc=0
    
    // P should have unfinished ops R4,R5, but they did not show up in exception list
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    
    // let p7 to succeed
    doOnePut(P, 7, "key1");

    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    waitForToVerifyRVV(R, memberP, 7, null, 0); // R's rvv=p7, gc=0
    waitForToVerifyRVV(R, memberR, 5, null, 0); // R's rvv=r3, gc=0

    // now P's rvv=P7,R3, R's RVV=P7,R5
    // shutdown R and restart
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    
    waitForToVerifyRVV(R, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(R, memberR, 3, exceptionlist, 0); // P's rvv=r3, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same
    
    // full GII chunk has 4 keys: key1,2(tombstone),3,5
    // delta GII chunk has 1 key, i.e. (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 1, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    
    // shutdown R again and restart, to verify localVersion=5 will be saved and recovered
    closeCache(R);
    createDistributedRegion(R);
    
    // P will receive R6 and have exception R6(3-6)
    doOnePut(R, 6, "key1"); // r6 will pass
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
  }

  /**
   * P1, P2, P3
   * R does GII but wait at BeforeSavedReceivedRVV, so R's RVV=P3R0
   * P4, P5
   * R goes on to save received RVV. R's new RVV=P5(3-6)R0
   *
   * When image (which contains P4, P5) arrives, it should fill the special exception
   */
  public void testFillSpecialException() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    doOnePut(P, 1, "key1");
    doOnePut(P, 2, "key2");
    doOneDestroy(P, 3, "key1");
    changeForceFullGII(R, false, true);

    // 4
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myBeforeSavedReceivedRVV = new Mycallback(GIITestHookType.BeforeSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myBeforeSavedReceivedRVV);
      }
    });
    // 5
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });

    closeCache(R);
    changeForceFullGII(R, true, false);
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    waitForCallbackStarted(R, GIITestHookType.BeforeSavedReceivedRVV);

    doOneDestroy(P, 4, "key2");
    doOnePut(P, 5, "key1");
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.BeforeSavedReceivedRVV, true});
    waitForCallbackStarted(R, GIITestHookType.AfterSavedReceivedRVV);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedReceivedRVV, true});
    async3.join(MAX_WAIT);
    waitForToVerifyRVV(R, memberP, 5, null, 0); // P's rvv=r5, gc=0
    changeForceFullGII(R, true, true);
    changeForceFullGII(P, false, true);
    verifyDeltaSizeFromStats(R, 2, 0);
  }

  /**
   * P1, P2, P3
   * R does GII but wait at BeforeSavedReceivedRVV, so R's RVV=P3R0
   * P4, P5
   * R goes on to save received RVV. R's new RVV=P5(3-6)R0
   *
   * When image (which contains P4, P5) arrives, it should fill the special exception
   *
   *  This test will let the 2 operations happen before RIM, so the rvv will match between R and P and
   *  no gii will happen. In this way, the chunk will not contain the missing entries.
   */
  public void testFillSpecialException2() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    doOnePut(P, 1, "key1");
    doOnePut(P, 2, "key2");
    doOneDestroy(P, 3, "key1");
    changeForceFullGII(R, false, true);

    // 3
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterCalculatedUnfinishedOps = new Mycallback(GIITestHookType.AfterCalculatedUnfinishedOps, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterCalculatedUnfinishedOps);
      }
    });
    // 5
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });

    closeCache(R);
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    waitForCallbackStarted(R, GIITestHookType.AfterCalculatedUnfinishedOps);

    doOneDestroy(P, 4, "key2");
    doOnePut(P, 5, "key1");
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterCalculatedUnfinishedOps, true});
    waitForCallbackStarted(R, GIITestHookType.AfterSavedReceivedRVV);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedReceivedRVV, true});
    async3.join(MAX_WAIT);
    waitForToVerifyRVV(R, memberP, 5, null, 0); // P's rvv=r5, gc=0
    verifyDeltaSizeFromStats(R, 2, 1);
    changeForceFullGII(R, false, true);
    changeForceFullGII(P, false, true);
  }

  /*
   * 1. after R5 becomes R4, regenrate another R5
   * 2. verify stattic callback at provider
   * 3. gii packing should wait for tombstone GC
   * 4. gii packing should wait for other on-fly operations
   */
  public void testHooks() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    VersionTag expect_tag = getVersionTag(R, "key5");

    final long[] exceptionlist = {4,5};
    R.invoke(GIIDeltaDUnitTest.class, "slowGII", new Object[] {exceptionlist});
    AsyncInvocation async1 = doOnePutAsync(R, 4, "key4");
    waitForToVerifyRVV(R, memberR, 4, null, 0); // P's rvv=r4, gc=0
    
    AsyncInvocation async2 = doOneDestroyAsync(R, 5, "key5");
    waitForToVerifyRVV(R, memberR, 5, null, 0); // P's rvv=r5, gc=0
    
    // P should have unfinished ops R4,R5, but they did not show up in exception list
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    

    // define test hooks
    // 1
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myBeforeRequestRVV = new Mycallback(GIITestHookType.BeforeRequestRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myBeforeRequestRVV);
      }
    });
    
    // 2
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterRequestRVV = new Mycallback(GIITestHookType.AfterRequestRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterRequestRVV);
      }
    });
    
    // 3
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterCalculatedUnfinishedOps = new Mycallback(GIITestHookType.AfterCalculatedUnfinishedOps, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterCalculatedUnfinishedOps);
      }
    });

    // 4
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myBeforeSavedReceivedRVV = new Mycallback(GIITestHookType.BeforeSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myBeforeSavedReceivedRVV);
      }
    });

    // 5
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });

    // 6
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSentRequestImage = new Mycallback(GIITestHookType.AfterSentRequestImage, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSentRequestImage);
      }
    });

    // 7  
    P.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterReceivedRequestImage = new Mycallback(GIITestHookType.AfterReceivedRequestImage, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterReceivedRequestImage);
      }
    });
    
    // 8  
    P.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myDuringPackingImage = new Mycallback(GIITestHookType.DuringPackingImage, REGION_NAME);
        InitialImageOperation.setGIITestHook(myDuringPackingImage);
      }
    });

    // 9  
    P.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSentImageReply = new Mycallback(GIITestHookType.AfterSentImageReply, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSentImageReply);
      }
    });

    // 10
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterReceivedImageReply = new Mycallback(GIITestHookType.AfterReceivedImageReply, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterReceivedImageReply);
      }
    });

    // 11
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myDuringApplyDelta = new Mycallback(GIITestHookType.DuringApplyDelta, REGION_NAME);
        InitialImageOperation.setGIITestHook(myDuringApplyDelta);
      }
    });

    // 12
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myBeforeCleanExpiredTombstones = new Mycallback(GIITestHookType.BeforeCleanExpiredTombstones, REGION_NAME);
        InitialImageOperation.setGIITestHook(myBeforeCleanExpiredTombstones);
      }
    });

    // 13
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSavedRVVEnd = new Mycallback(GIITestHookType.AfterSavedRVVEnd, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedRVVEnd);
      }
    });
    

    // shutdown R and restart
    waitForToVerifyRVV(R, memberP, 6, null, 0); // R's rvv=p6, gc=0
    waitForToVerifyRVV(R, memberR, 5, null, 0); // R's rvv=r3, gc=0
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    // let p7 to succeed
    doOnePut(P, 7, "key1");

    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0
    // now P's rvv=P7,R3, R's RVV=P6,R5
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    
    // 1
    waitForCallbackStarted(R, GIITestHookType.BeforeRequestRVV);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.BeforeRequestRVV, true});
    // 2
    waitForCallbackStarted(R, GIITestHookType.AfterRequestRVV);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterRequestRVV, true});
    // 3
    waitForCallbackStarted(R, GIITestHookType.AfterCalculatedUnfinishedOps);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterCalculatedUnfinishedOps, true});
    // 4
    waitForCallbackStarted(R, GIITestHookType.BeforeSavedReceivedRVV);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.BeforeSavedReceivedRVV, true});
    // 5
    waitForCallbackStarted(R, GIITestHookType.AfterSavedReceivedRVV);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedReceivedRVV, true});

    // 6
    waitForCallbackStarted(R, GIITestHookType.AfterSentRequestImage);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSentRequestImage, true});
    
    // 7
    waitForCallbackStarted(P, GIITestHookType.AfterReceivedRequestImage);
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterReceivedRequestImage, true});
    // 8
    waitForCallbackStarted(P, GIITestHookType.DuringPackingImage);
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.DuringPackingImage, true});
    // 9
    waitForCallbackStarted(P, GIITestHookType.AfterSentImageReply);
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSentImageReply, true});

    // 10
    waitForCallbackStarted(R, GIITestHookType.AfterReceivedImageReply);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterReceivedImageReply, true});
    // 11
    waitForCallbackStarted(R, GIITestHookType.DuringApplyDelta);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.DuringApplyDelta, true});
    // 12
    waitForCallbackStarted(R, GIITestHookType.BeforeCleanExpiredTombstones);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.BeforeCleanExpiredTombstones, true});
    // 13
    waitForCallbackStarted(R, GIITestHookType.AfterSavedRVVEnd);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedRVVEnd, true});
    
    async3.join(MAX_WAIT);
    
    waitForToVerifyRVV(R, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(R, memberR, 3, exceptionlist, 0); // P's rvv=r3, gc=0
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same
    
    // full gii chunk has 4 entries: key1,2(tombstone),3,5
    // delta gii has 2 entry: p7 (key1) and (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 2, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    
    // P will receive R6 and have exception R6(3-6)
    doOnePut(R, 6, "key1"); // r6 will pass
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
    
    P.invoke(InitialImageOperation.class, "resetAllGIITestHooks");
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list. Then shutdown R. 
   * Before GII, P's RVV is P7,R6(3-6), RVVGC is P0,R0; R's RVV is P3,R6, RVVGC is P0,R0
   * vm1 becomes offline then restarts. Use testHook to pause the GII, then do tombstone GC at P only. 
   * The deltaGII should send correct tombstonedelta to R, revoke unfinished opeation R4,R5
   * 
   * There's member T doing GII from P at the same time. 
   * 
   * In this test, GII thread will get the GIILock before tombstone GC, so tombstone GC should 
   * wait for all GIIs to finish
   */
  public void testTombstoneGCInMiddleOfGII() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};
    
    Host host = Host.getHost(0);
    VM T = host.getVM(2);
    createDistributedRegion(T);
    final DiskStoreID memberT = getMemberID(T);
    closeCache(T);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(3);
    waitForToVerifyRVV(P, memberP, 3, null, 0); // P's rvv=p3, gc=0
    VersionTag expect_tag = getVersionTag(R, "key5");
    
    createUnfinishedOperationsR4R5();
    waitForToVerifyRVV(R, memberP, 3, null, 0); // R's rvv=p3, gc=0
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    // p4-7 only apply at P
    doOneDestroy(P, 4, "key2");
    doOnePut(P, 5, "key1");
    doOnePut(P, 6, "key3");
    doOnePut(P, 7, "key1");

    // add test hook
    // 8  
    P.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myDuringPackingImage = new Mycallback(GIITestHookType.DuringPackingImage, REGION_NAME);
        InitialImageOperation.setGIITestHook(myDuringPackingImage);
      }
    });
    
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);

    // restart R and gii, it will be blocked at test hook
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    // restart R and gii, it will be blocked at test hook
    AsyncInvocation async4 = createDistributedRegionAsync(T);
    // 8
    waitForCallbackStarted(P, GIITestHookType.DuringPackingImage);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        int count = getDeltaGIICount(P);
        return (count == 2);
      }
      public String description() {
        return null;
      }
    };

    Wait.waitForCriterion(ev, 30000, 200, true);
    int count = getDeltaGIICount(P);
    assertEquals(2, count);

    // force tombstone GC to let RVVGC to become P4:R0, but R already sent its old RVV/RVVGC over
    // this tombstone GC happens AFTER GII thread got the GIILock, so it will be ignored since GII is ongoing
    changeTombstoneTimout(R, MAX_WAIT);
    changeTombstoneTimout(P, MAX_WAIT);
    changeTombstoneTimout(T, MAX_WAIT);
    Wait.pause((int)MAX_WAIT);
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0

    // let GII continue
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.DuringPackingImage, false});
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.DuringPackingImage, true});
    
    WaitCriterion ev2 = new WaitCriterion() {
      public boolean done() {
        int count = getDeltaGIICount(P);
        return (count == 0);
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev2, 30000, 200, true);
    count = getDeltaGIICount(P);
    assertEquals(0, count);
    verifyTombstoneExist(P, "key2", true, true); // expect key2 is still tombstone during and after GIIs
    verifyTombstoneExist(R, "key2", true, true); // expect key2 is still tombstone during and after GIIs
    verifyTombstoneExist(T, "key2", true, true); // expect key2 is still tombstone during and after GIIs
    
    forceGC(P, 1); // trigger to GC the tombstones in expired queue
    async3.join(MAX_WAIT);
    async4.join(MAX_WAIT);

    // after GII, tombstone GC happened
    waitForToVerifyRVV(P, memberP, 7, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
    
    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, exceptionlist, 0); // R's rvv=r6, gc=0

    verifyTombstoneExist(P, "key2", false, true); // expect tombstone key2 is GCed at P
    verifyTombstoneExist(R, "key2", false, true); // expect tombstone key2 is GCed at R
    verifyTombstoneExist(T, "key2", false, true); // T got everything from P
    
    // do a put from T
    doOnePut(T, 1, "key1");

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    RegionVersionVector t_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv);
    assertSameRVV(t_rvv, r_rvv);
    
    // If fullGII, the key size in gii chunk is 4, i.e. key1,key3,key5,key2 is a tombstone. 
    // If delta GII, it should be 4, (key1, key2, key3) and (key5(T) which is unfinished operation)
    verifyDeltaSizeFromStats(R, 4, 1);
    // verify unfinished op for key5 is revoked
    waitToVerifyKey(R, "key5", generateValue(R));
    VersionTag tag = getVersionTag(R, "key5");
    assertTrue(expect_tag.equals(tag));
    //    P.invoke(InitialImageOperation.class, "resetAllGIITestHooks");
  }
  
  /**
   * vm0 and vm1 are peers, each holds a DR.
   * Let P and R have the same RVV and RVVGC: P7,R6, RVVGC is P0,R0.
   * vm1 becomes offline then restarts. Use testHook to pause the GII, then do tombstone GC 
   * triggered by expireBatch (not by forceGC) at R only.
   * The tombstone GC will be executed at R, but igored at P.   
   * The deltaGII should send nothing to R since the RVVs are the same. So after GII, P and R will 
   * have different tombstone number. But P's tombstones should be expired.  
   */
  public void testExpiredTombstoneSkippedAtProviderOnly() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    
    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);

    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");
    
    waitForToVerifyRVV(R, memberP, 6, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // P's rvv=r6, gc=0
    // now the rvv and rvvgc at P and R should be the same
    
    // save R's rvv in byte array, check if it will be fullGII
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    // shutdown R and restart
    closeCache(R);

    // let p7 to succeed
    doOnePut(P, 7, "key1");

    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0

    // add test hook
    P.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myDuringPackingImage = new Mycallback(GIITestHookType.DuringPackingImage, REGION_NAME);
        InitialImageOperation.setGIITestHook(myDuringPackingImage);
      }
    });
    
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);

    // restart R and gii, it will be blocked at test hook
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    // 8
    waitForCallbackStarted(P, GIITestHookType.DuringPackingImage);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        int count = getDeltaGIICount(P);
        return (count == 1);
      }
      public String description() {
        return null;
      }
    };

    Wait.waitForCriterion(ev, 30000, 200, true);
    int count = getDeltaGIICount(P);
    assertEquals(1, count);
    
    // let tombstone expired at R to trigger tombstoneGC. 
    // Wait for tombstone is GCed at R, but still exists in P
    changeTombstoneTimout(R, MAX_WAIT);
    changeTombstoneTimout(P, MAX_WAIT);
    Wait.pause((int)MAX_WAIT);
    forceGC(R, 3);
    forceGC(P, 3);
    
    // let GII continue
    P.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.DuringPackingImage, true});
    async3.join(MAX_WAIT*2);
    count = getDeltaGIICount(P);
    assertEquals(0, count);
    verifyDeltaSizeFromStats(R, 1, 1); // deltaGII, key1 in delta
    
    // tombstone key2, key5 should be GCed at R
    verifyTombstoneExist(R, "key2", false, false);
    verifyTombstoneExist(R, "key5", false, false);

    // tombstone key2, key5 should still exist and expired at P
    verifyTombstoneExist(P, "key2", true, true);
    verifyTombstoneExist(P, "key5", true, true);

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    System.out.println("GGG:p_rvv="+p_rvv.fullToString()+":r_rvv="+r_rvv.fullToString());

    waitForToVerifyRVV(R, memberP, 7, null, 4); // R's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, null, 5); // R's rvv=r6, gc=5
    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * Each does a few operations to make RVV=P7,R6, RVVGC=P4,R0 for both members.   
   * vm1 becomes offline then restarts. 
   * The deltaGII should only exchange RVV. No need to send data from vm0 to vm1.  
   */
  public void testRequesterHasHigherRVVGC() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);

    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");
    
    // let P pretends to be doing GII, to skip gcTombstone
    forceAddGIICount(P);
    changeTombstoneTimout(R, MAX_WAIT);
    changeTombstoneTimout(P, MAX_WAIT);
    Wait.pause((int)MAX_WAIT);
    forceGC(R, 3);
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(R, memberR, 6, null, 5); // R's rvv=r6, gc=5
    verifyTombstoneExist(R, "key5", false, false);
    verifyTombstoneExist(P, "key5", true, true);
    
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);
    // let p7 to succeed
    doOnePut(P, 7, "key1");

    // shutdown R and restart
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 7, null, 4); // P's rvv=p7, gc=4
    waitForToVerifyRVV(R, memberR, 6, null, 5); // P's rvv=r6, gc=5
    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=r7, gc still 0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc still 0
    
    // If fullGII, the key size in gii chunk is 5, i.e. key1,key2(T),key3,key4,key5(T).
    // If deltaGII, the key size is 1, i.e. P7 (key1)
    verifyDeltaSizeFromStats(R, 1, 1);
    verifyTombstoneExist(R, "key5", false, false);
    verifyTombstoneExist(P, "key5", true, true);
  }

  /**
   * P and R are peers, each holds a DR.
   * Each does a few operations to make RVV=P7,R6, RVVGC=P0,R0 for both members.   
   * P8 is clear() operation. After that, R offline. Run P9 is a put. Restart R.   
   * R will do deltaGII to get P9 as delta  
   */
  public void testDeltaGIIAfterClear() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");

    doOnePut(P, 7, "key1");

    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p7, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 7, null, 0); // R's rvv=P7, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0
    
    // Note: since R is still online, clear will do flush message which will be blocked by the 
    // test CDL (to create unfinished operation). So in this test, no exception
    doOneClear(P, 8);
    // clear() increased P's version with 1 to P8
    // after clear, P and R's RVVGC == RVV
    waitForToVerifyRVV(P, memberP, 8, null, 8); // P's rvv=p8, gc=8
    waitForToVerifyRVV(P, memberR, 6, null, 6); // P's rvv=r6, gc=6
    waitForToVerifyRVV(R, memberP, 8, null, 8); // R's rvv=p8, gc=8
    waitForToVerifyRVV(R, memberR, 6, null, 6); // R's rvv=r6, gc=6
    
    // shutdown R
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    // do a put at P to get some delta
    doOnePut(P, 9, "key3");
    waitForToVerifyRVV(P, memberP, 9, null, 8);
    waitForToVerifyRVV(P, memberR, 6, null, 6);

    // restart R to deltaGII
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    
    // shutdown P and restart
    closeCache(P);
    createDistributedRegion(P);
    waitForToVerifyRVV(P, memberP, 9, null, 8);
    waitForToVerifyRVV(P, memberR, 6, null, 6);

    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 9, null, 8);
    waitForToVerifyRVV(R, memberR, 6, null, 6);
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same

    verifyDeltaSizeFromStats(R, 1, 1);
  }

  /**
   * P and R are peers, each holds a DR.
   * Each does a few operations to make RVV=P6,R6, RVVGC=P0,R0 for both members.
   * R off line, then run P7. Restart R. It will trigger deltaGII to chunk entry P7(key1).
   * After that, do clear(). Make sure R should not contain key1 after GII.     
   */
  public void testClearAfterChunkEntries() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");

    waitForToVerifyRVV(P, memberP, 6, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 6, null, 0); // R's rvv=P6, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0

    // set tesk hook
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterReceivedImageReply = new Mycallback(GIITestHookType.AfterReceivedImageReply, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterReceivedImageReply);
      }
    });

    // shutdown R
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    // key1 will be the delta
    doOnePut(P, 7, "key1");

    // retart R
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, false);
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    
    // when chunk arrived, do clear()
    waitForCallbackStarted(R, GIITestHookType.AfterReceivedImageReply);
    doOneClear(P, 8);

    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterReceivedImageReply, true});
    async3.getResult(MAX_WAIT);

    // clear() increased P's version with 1 to P8
    // after clear, P and R's RVVGC == RVV
    waitForToVerifyRVV(P, memberP, 8, null, 8); // P's rvv=r8, gc=8
    waitForToVerifyRVV(P, memberR, 6, null, 6); // P's rvv=r6, gc=6

    // retart R again to do fullGII
    closeCache(R);
    createDistributedRegion(R);

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same

    waitForToVerifyRVV(R, memberP, 8, null, 8); // R's rvv=r8, gc=8
    waitForToVerifyRVV(R, memberR, 6, null, 6); // R's rvv=r6, gc=6
    waitToVerifyKey(P, "key1", null);
    waitToVerifyKey(R, "key1", null);
    verifyDeltaSizeFromStats(R, 0, 1);
  }

  /**
   * P and R are peers, each holds a DR.
   * Each does a few operations to make RVV=P6,R6, RVVGC=P0,R0 for both members.
   * R off line, then run P7. Restart R. When P's RVV arrives, do clear(). It should trigger
   * fullGII. Make sure R should not contain key1 after GII.     
   */
  public void testClearAfterSavedRVV() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");

    waitForToVerifyRVV(P, memberP, 6, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 6, null, 0); // R's rvv=P6, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0

    // set tesk hook
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });

    // shutdown R
    closeCache(R);

    // key1 will be the delta
    doOnePut(P, 7, "key1");

    // retart R
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    
    // when chunk arrived, do clear()
    waitForCallbackStarted(R, GIITestHookType.AfterSavedReceivedRVV);
    doOneClear(P, 8);

    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedReceivedRVV, true});
    async3.join(MAX_WAIT);

    // clear() increased P's version with 1 to P8
    // after clear, P and R's RVVGC == RVV
    waitForToVerifyRVV(P, memberP, 8, null, 8); // P's rvv=r8, gc=8
    waitForToVerifyRVV(P, memberR, 6, null, 6); // P's rvv=r6, gc=6

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same

    waitForToVerifyRVV(R, memberP, 8, null, 8); // R's rvv=r8, gc=8
    waitForToVerifyRVV(R, memberR, 6, null, 6); // R's rvv=r6, gc=6
    waitToVerifyKey(P, "key1", null);
    waitToVerifyKey(R, "key1", null);
    verifyDeltaSizeFromStats(R, 0, 0);
  }

  /**
   * P and R are peers, each holds a DR.
   * Each does a few operations to make RVV=P7,R6, RVVGC=P0,R0 for both members.   
   * R offline, then P8 is clear() operation. Run P9 is a put. Restart R.   
   * R will do fullGII since R missed a clear  
   */
  public void testFullGIIAfterClear() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    createUnfinishedOperationsR4R5();

    doOnePut(P, 7, "key1");

    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(P, memberR, 6, exceptionlist, 0); // P's rvv=r6(3-6), gc=0
    waitForToVerifyRVV(R, memberP, 7, null, 0); // R's rvv=P7, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0

    // shutdown R before clear
    byte[] R_rvv_bytes = getRVVByteArray(R, REGION_NAME);
    closeCache(R);

    doOneClear(P, 8);
    // clear() increased P's version with 1 to P8
    // after clear, P and R's RVVGC == RVV, no more exception
    waitForToVerifyRVV(P, memberP, 8, null, 8); // P's rvv=r6, gc=8
    waitForToVerifyRVV(P, memberR, 6, null, 6); // P's rvv=r6, gc=6

    // do a put at P to get some delta
    doOnePut(P, 9, "key3");
    waitForToVerifyRVV(P, memberP, 9, null, 8);
    waitForToVerifyRVV(P, memberR, 6, null, 6);

    // restart R to deltaGII
    checkIfFullGII(P, REGION_NAME, R_rvv_bytes, true);
    createDistributedRegion(R);
    waitForToVerifyRVV(R, memberP, 9, null, 8);
    waitForToVerifyRVV(R, memberR, 6, null, 6);
    
    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same

    verifyDeltaSizeFromStats(R, 1, 0);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR.
   * create some exception list.
   * Before GII, P's RVV is P7,R6(3-6), R's RVV is P6,R6, RVVGC are both P4,R0
   * By changing MAX_UNFINISHED_OPERATIONS to be 1, 2. It should be fullGII then deltaGII.    
   */
  public void testFullGIITriggeredByTooManyUnfinishedOps() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);

    // force tombstone GC to let RVVGC to become P4:R0
    forceGC(P, 2);
    waitForToVerifyRVV(P, memberP, 6, null, 4); // P's rvv=p6, gc=4
    waitForToVerifyRVV(P, memberR, 3, null, 0); // P's rvv=r3, gc=0

    createUnfinishedOperationsR4R5();
    
    // now P's cache still only has key1, key3, key5
    closeCache(R);
    // p7 only apply at P
    doOnePut(P, 7, "key1");

    // restart and gii, it should be fullGII because number of unfinished operations exceeds limit
    changeUnfinishedOperationLimit(R, 1);
    createDistributedRegion(R);
    // If fullGII, the key size in gii chunk is 3, i.e. key1,key3,key5. key2 is GCed. 
    // If delta GII, the key size should be 1, i.e. P7(key1)
    verifyDeltaSizeFromStats(R, 3, 0);
  }

  /**
   * P and R are peers, each holds a DR.
   * Each does a few operations to make RVV=P6,R6, RVVGC=P0,R0 for both members.
   * R off line, then run P7. Restart R. When P's RVV arrives, restart R. It should trigger
   * fullGII.      
   */
  public void testRestartWithOnlyGIIBegion() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);
    final long[] exceptionlist = {4,5};

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");

    waitForToVerifyRVV(P, memberP, 6, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 6, null, 0); // R's rvv=P6, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0

    // set tesk hook
    R.invoke(new SerializableRunnable() {
      public void run() {
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });

    // shutdown R
    closeCache(R);

    // key1 will be the delta
    doOnePut(P, 7, "key1");

    // retart R
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    
    // when chunk arrived, do clear()
    waitForCallbackStarted(R, GIITestHookType.AfterSavedReceivedRVV);


    // kill and restart R
    closeCache(R);
    async3.join(MAX_WAIT);
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedReceivedRVV, true});
    createDistributedRegion(R);

    waitForToVerifyRVV(P, memberP, 7, null, 0); // P's rvv=r8, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0

    RegionVersionVector p_rvv = getRVV(P);
    RegionVersionVector r_rvv = getRVV(R);
    assertSameRVV(p_rvv, r_rvv); // after gii, rvv should be the same

    waitForToVerifyRVV(R, memberP, 7, null, 0); // R's rvv=r7, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0
    
    // In fullGII, the key size in gii chunk is 2. They are: key1, key2, key3, key4, key5
    verifyDeltaSizeFromStats(R, 5, 0);
  }
  
  /**
   * Test the case where a member has an untrusted RVV and 
   * still initializes from the local data. See bug 48066
   * @throws Throwable
   */
  public void testRecoverFromUntrustedRVV() throws Throwable {
    prepareForEachTest();
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    // let r4,r5,r6 to succeed
    doOnePut(R, 4, "key4");
    doOneDestroy(R, 5, "key5");
    doOnePut(R, 6, "key1");

    waitForToVerifyRVV(P, memberP, 6, null, 0); // P's rvv=p6, gc=0
    waitForToVerifyRVV(P, memberR, 6, null, 0); // P's rvv=r6, gc=0
    waitForToVerifyRVV(R, memberP, 6, null, 0); // R's rvv=P6, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // R's rvv=r6, gc=0

    // set tesk hook
    R.invoke(new SerializableRunnable() {
      public void run() {
        //Add hooks before and after receiving the RVV
        Mycallback myBeforeSavedReceivedRVV = new Mycallback(GIITestHookType.BeforeSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myBeforeSavedReceivedRVV);
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterSavedReceivedRVV, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });

    // shutdown R
    closeCache(R);

    

    // retart R
    AsyncInvocation async3 = createDistributedRegionAsync(R);
    
 // when chunk arrived, do clear()
    waitForCallbackStarted(R, GIITestHookType.BeforeSavedReceivedRVV);
    
    // Before R saves the RVV, do a put. This will be recording in Rs region
    // and RVV, but it will be wiped out in the RVV when R applies the RVV
    // from P.
    doOnePut(P, 7, "key1");
    
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.BeforeSavedReceivedRVV, true});
    
    // Wait until the new RVV is applied
    waitForCallbackStarted(R, GIITestHookType.AfterSavedReceivedRVV);
    
    // destroy the region on P (which will force R to recover with it's own
    // data
    destroyRegion(P);
    
    //Allow the GII to continue.
    R.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterSavedReceivedRVV, true});
    
    async3.join(MAX_WAIT);
    
//    createDistributedRegion(R);

    //Make sure that Rs RVV now reflects the update from P
    waitForToVerifyRVV(R, memberP, 7, null, 0); // P's rvv=r8, gc=0
    waitForToVerifyRVV(R, memberR, 6, null, 0); // P's rvv=r6, gc=0
  }
  
  /**
   * Test case to make sure that if a tombstone GC occurs during
   * a full GII, we still have the correct RVV on the GII recipient
   * at the end. 
   * @throws Throwable
   */
  public void testTombstoneGCDuringFullGII() throws Throwable {
    prepareForEachTest();
    
    //Create the region in 1 more VM to to a tombstone GC.
    VM vm2 = Host.getHost(0).getVM(2);
    createDistributedRegion(vm2);
    
    final DiskStoreID memberP = getMemberID(P);
    final DiskStoreID memberR = getMemberID(R);

    assertEquals(0, DistributedCacheOperation.SLOW_DISTRIBUTION_MS);
    prepareCommonTestData(6);
    //All members should have "key5" at this point
    
    // shutdown R
    closeCache(R);
    
    final VM vmR = R;
    
    //Destroy key5, this will leave a tombstone
    doOneDestroy(P, 7, "key5");

    //Set tesk hook so that R will pause GII after getting the RVV
    R.invoke(new SerializableRunnable() {
      public void run() {
        //Add hooks before and after receiving the RVV
        Mycallback myAfterSavedReceivedRVV = new Mycallback(GIITestHookType.AfterCalculatedUnfinishedOps, REGION_NAME);
        InitialImageOperation.setGIITestHook(myAfterSavedReceivedRVV);
      }
    });
    
    
    //Set a trigger in vm2 so that it will start up R after determining
    //the recipients for a tombstone GC message. vm2 will wait until
    //R has already received the RVV before sending the message.
    vm2.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeSendMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof TombstoneMessage && ((TombstoneMessage) message).regionPath.contains(REGION_NAME)) {
              System.err.println("DAN DEBUG  about to send tombstone message, starting up R - " + message.getSender());
              AsyncInvocation async3 = createDistributedRegionAsync(vmR);
   
              //Wait for R to finish requesting the RVV before letting the tombstone GC proceeed.
              waitForCallbackStarted(vmR, GIITestHookType.AfterCalculatedUnfinishedOps);
              System.err.println("DAN DEBUG  R has received the RVV, sending tombstone message");
              DistributionMessageObserver.setInstance(null);
            }
          }
        });
      }
    });
    
    P.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          @Override
          public void afterProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof TombstoneMessage && ((TombstoneMessage) message).regionPath.contains(REGION_NAME)) {
              System.err.println("DAN DEBUG  P has processed the tombstone message, allowing R to proceed with the GII");
              vmR.invoke(InitialImageOperation.class, "resetGIITestHook", new Object[] {GIITestHookType.AfterCalculatedUnfinishedOps, true});
              DistributionMessageObserver.setInstance(null);
            }
          }
        });
      }
    });

    //Force tombstone GC, this will trigger the R to be started, etc.
    vm2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        try {
          cache.getTombstoneService().forceBatchExpirationForTests(20);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    

    //Wait for P to perform the tombstone GC
    waitForToVerifyRVV(P, memberP, 7, null, 7);
    
    System.err.println("DAN DEBUG P has finished the tombstone GC, waiting for R to get the correct RVV");
    
    //Make sure that Rs RVV now reflects the update from P
    waitForToVerifyRVV(R, memberP, 7, null, 7); // P's rvv=r7, gc=7
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  protected void createDistributedRegion(VM vm) {
    AsyncInvocation future = createDistributedRegionAsync(vm);
    try {
      future.join(MAX_WAIT);
    } catch (InterruptedException e) {
      Assert.fail("Create region is interrupted", e);
    }
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
  }
  
  protected AsyncInvocation createDistributedRegionAsync(VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          String value = System.getProperty("gemfire.no-flush-on-close");
          assertNull(value);
          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
//          CCRegion = (LocalRegion)f.create(REGION_NAME);
          LocalRegion lr = (LocalRegion)f.create(REGION_NAME);
          LogWriterUtils.getLogWriter().info("In createDistributedRegion, using hydra.getLogWriter()");
          LogWriterUtils.getLogWriter().fine("Unfinished Op limit="+InitialImageOperation.MAXIMUM_UNFINISHED_OPERATIONS);
        } catch (CacheException ex) {
          Assert.fail("While creating region", ex);
        }
      }
    };
    return vm.invokeAsync(createRegion);
  }
  
  protected void closeCache(VM vm) {
    SerializableRunnable close = new SerializableRunnable() {
      public void run() {
        try {
          Cache cache = getCache();
          System.setProperty("gemfire.no-flush-on-close", "true");
          cache.close();
        } finally {
          System.getProperties().remove("gemfire.no-flush-on-close");
        }
      }
    };
    vm.invoke(close);
  }
  
  protected void destroyRegion(VM vm) {
    SerializableRunnable destroy = new SerializableRunnable() {
      public void run() {
        LocalRegion lr = (LocalRegion)getCache().getRegion(REGION_NAME);
        lr.localDestroyRegion();
      }
    };
    vm.invoke(destroy);
  }

  protected void changeUnfinishedOperationLimit(VM vm, final int value) {
    SerializableRunnable change = new SerializableRunnable() {
      public void run() {
        InitialImageOperation.MAXIMUM_UNFINISHED_OPERATIONS = value;
      }
    };
    vm.invoke(change);
  }

  protected void changeTombstoneTimout(VM vm, final long value) {
    SerializableRunnable change = new SerializableRunnable() {
      public void run() {
        TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT = value;
      }
    };
    vm.invoke(change);
  }

  protected void changeForceFullGII(VM vm, final boolean value, final boolean checkOnly) {
    SerializableRunnable change = new SerializableRunnable() {
      public void run() {
        if (checkOnly) {
          assertEquals(value, InitialImageOperation.FORCE_FULL_GII);
        } else {
          InitialImageOperation.FORCE_FULL_GII = value;
        }
      }
    };
    vm.invoke(change);
  }

  protected void removeSystemPropertiesInVM(VM vm, final String prop) {
    SerializableRunnable change = new SerializableRunnable() {
      public void run() {
        LogWriterUtils.getLogWriter().info("Current prop setting: "+prop+"="+System.getProperty(prop));
        System.getProperties().remove(prop);
        LogWriterUtils.getLogWriter().info(prop+"="+System.getProperty(prop));
      }
    };
    vm.invoke(change);
  }

  protected void verifyDeltaSizeFromStats(VM vm, final int expectedKeyNum, 
      final int expectedDeltaGIINum) {
    SerializableRunnable verify = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        // verify from CachePerfStats that certain amount of keys in delta
        LocalRegion lr = (LocalRegion)getCache().getRegion(REGION_NAME);
        CachePerfStats stats = lr.getRegionPerfStats();
        
        // we saved GII completed count in RegionPerfStats only
        int size = stats.getGetInitialImageKeysReceived();
        cache.getLogger().info("Delta contains: "+ size +" keys");
        assertEquals(expectedKeyNum, size);
        
        int num = stats.getDeltaGetInitialImagesCompleted();
        cache.getLogger().info("Delta GII completed: "+ num +" times");
        assertEquals(expectedDeltaGIINum, num);
      }
    };
    vm.invoke(verify);
  }
  
  // P should have smaller diskstore ID than R's
  protected void assignVMsToPandR(final VM vm0, final VM vm1) {
    DiskStoreID dsid0 = getMemberID(vm0);
    DiskStoreID dsid1 = getMemberID(vm1);
    int compare = dsid0.compareTo(dsid1);
    LogWriterUtils.getLogWriter().info("Before assignVMsToPandR, dsid0 is "+dsid0+",dsid1 is "+dsid1+",compare="+compare);
    if (compare > 0) {
      P = vm0;
      R = vm1;
    } else {
      P = vm1;
      R = vm0;
    }
    LogWriterUtils.getLogWriter().info("After assignVMsToPandR, P is "+P.getPid()+"; R is "+R.getPid()+" for region "+REGION_NAME);
  }
  
  private DiskStoreID getMemberID(VM vm) {
    SerializableCallable getDiskStoreID = new SerializableCallable("get DiskStoreID as member id") {
      public Object call() {
        LocalRegion lr = (LocalRegion)getCache().getRegion(REGION_NAME);
        assertTrue(lr!=null && lr.getDiskStore()!=null);
        DiskStoreID dsid = lr.getDiskStore().getDiskStoreID();
        return dsid;
      }
    };
    return (DiskStoreID)vm.invoke(getDiskStoreID);
  }
  
  private void forceGC(VM vm, final int count) {
    vm.invoke(new SerializableCallable("force GC") {
      public Object call() throws Exception {
        ((GemFireCacheImpl)getCache()).getTombstoneService().forceBatchExpirationForTests(count);
        return null;
      }
    });
  }

  private void forceAddGIICount(VM vm) {
    vm.invoke(new SerializableCallable("force to add gii count") {
      public Object call() throws Exception {
        ((GemFireCacheImpl)getCache()).getTombstoneService().incrementGCBlockCount();
        return null;
      }
    });
  }
  
  private void assertDeltaGIICountBeZero(VM vm) {
    vm.invoke(new SerializableCallable("assert progressingDeltaGIICount == 0") {
      public Object call() throws Exception {
        int count = ((GemFireCacheImpl)getCache()).getTombstoneService().getGCBlockCount();
        assertEquals(0, count);
        return null;
      }
    });
  }

  public void waitForToVerifyRVV(final VM vm, final DiskStoreID member, final long expectedRegionVersion, 
      final long[] exceptionList, final long expectedGCVersion) {
    SerializableRunnable waitForVerifyRVV = new SerializableRunnable() {
      
      private boolean verifyExceptionList(final DiskStoreID member, final long regionversion, 
          final RegionVersionVector rvv, final long[] exceptionList) {
        boolean exceptionListVerified = true;
        if (exceptionList != null) {
          for (long i:exceptionList) {
            exceptionListVerified = !rvv.contains(member, i);
            if (!exceptionListVerified) {
              LogWriterUtils.getLogWriter().finer("DeltaGII:missing exception "+i+":"+rvv);
              break;
            }
          }
        } else {
          // expect no exceptionlist
          for (long i = 1; i<=regionversion; i++) {
            if (!rvv.contains(member, i)) {
              exceptionListVerified = false;
              LogWriterUtils.getLogWriter().finer("DeltaGII:unexpected exception "+i);
              break;
            }
          }
        }
        return exceptionListVerified;
      }

      public void run() {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            RegionVersionVector rvv = ((LocalRegion)getCache().getRegion(REGION_NAME)).getVersionVector().getCloneForTransmission();
            long regionversion = getRegionVersionForMember(rvv, member, false);
            long gcversion = getRegionVersionForMember(rvv, member, true);
            
            boolean exceptionListVerified = verifyExceptionList(member, regionversion, rvv, exceptionList);
            LogWriterUtils.getLogWriter().info("DeltaGII:expected:"+expectedRegionVersion+":"+expectedGCVersion);
            LogWriterUtils.getLogWriter().info("DeltaGII:actual:"+regionversion+":"+gcversion+":"+exceptionListVerified+":"+rvv);

            boolean match = true;
            if (expectedRegionVersion != -1) {
              match = match &&  (regionversion == expectedRegionVersion);
            }
            if (expectedGCVersion != -1) {
              match = match && (gcversion == expectedGCVersion);
            }
            return match && exceptionListVerified;
          }
          public String description() {
            RegionVersionVector rvv = ((LocalRegion)getCache().getRegion(REGION_NAME)).getVersionVector().getCloneForTransmission();
            long regionversion = getRegionVersionForMember(rvv, member, false);
            long gcversion = getRegionVersionForMember(rvv, member, true);
            return "expected (rv" + expectedRegionVersion + ", gc" + expectedGCVersion + ")" + " got (rv" + regionversion + ", gc" + gcversion + ")";
          }
        };
        
        Wait.waitForCriterion(ev, 10 * 1000, 200, true);
        RegionVersionVector rvv = ((LocalRegion)getCache().getRegion(REGION_NAME)).getVersionVector().getCloneForTransmission();
        long regionversion = getRegionVersionForMember(rvv, member, false);
        long gcversion = getRegionVersionForMember(rvv, member, true);
        if (expectedRegionVersion != -1) {
          assertEquals(expectedRegionVersion, regionversion);
        }
        if (expectedGCVersion != -1) {
          assertEquals(expectedGCVersion, gcversion);
        }
        boolean exceptionListVerified = verifyExceptionList(member, regionversion, rvv, exceptionList);
        assertTrue(exceptionListVerified);
      }
    };
    vm.invoke(waitForVerifyRVV);
  }
  
  public void waitForCallbackStarted(final VM vm, final GIITestHookType callbacktype) {
    SerializableRunnable waitForCallbackStarted = new SerializableRunnable() {
      public void run() {

        final GIITestHook callback = InitialImageOperation.getGIITestHookForCheckingPurpose(callbacktype);
        WaitCriterion ev = new WaitCriterion() {

          public boolean done() {
            return (callback != null && callback.isRunning);
          }
          public String description() {
            return null;
          }
        };

        Wait.waitForCriterion(ev, 30000, 200, true);
        if (callback == null || !callback.isRunning) {
          fail("GII tesk hook is not started yet");
        }
      }
    };
    vm.invoke(waitForCallbackStarted);
  }
  
  private VersionTag getVersionTag(VM vm, final String key) {
    SerializableCallable getVersionTag = new SerializableCallable("get version tag") {
      public Object call() {
        VersionTag tag = ((LocalRegion)getCache().getRegion(REGION_NAME)).getVersionTag(key);
        return tag;

      }
    };
    return (VersionTag)vm.invoke(getVersionTag);
  }

  public void waitToVerifyKey(final VM vm, final String key, final String expect_value) {
    SerializableRunnable waitToVerifyKey = new SerializableRunnable() {
      public void run() {
        
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            String value = (String)((LocalRegion)getCache().getRegion(REGION_NAME)).get(key);
            if (expect_value == null && value == null) {
              return true;
            } else {
              return (value != null && value.equals(expect_value));
            }
          }
          public String description() {
            return null;
          }
        };
        
        Wait.waitForCriterion(ev, 10 * 1000, 200, true);
        String value = (String)((LocalRegion)getCache().getRegion(REGION_NAME)).get(key);
        assertEquals(expect_value, value);
      }
    };
    vm.invoke(waitToVerifyKey);
  }

  protected byte[] getRVVByteArray(VM vm, final String regionName) throws IOException, ClassNotFoundException {
    SerializableCallable getRVVByteArray = new SerializableCallable("getRVVByteArray") {

      public Object call() throws Exception {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(regionName);
        RegionVersionVector rvv = region.getVersionVector();
        rvv = rvv.getCloneForTransmission();
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);

        //Using gemfire serialization because 
        //RegionVersionVector is not java serializable
        DataSerializer.writeObject(rvv, hdos);
        return hdos.toByteArray();
      }
    };
    byte[] result= (byte[]) vm.invoke(getRVVByteArray);
    return result;
  }

  protected RegionVersionVector getRVV(VM vm) throws IOException, ClassNotFoundException {
    byte[] result= getRVVByteArray(vm, REGION_NAME);
    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }
  
  protected RegionVersionVector getDiskRVV(VM vm) throws IOException, ClassNotFoundException {
    SerializableCallable createData = new SerializableCallable("getRVV") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        RegionVersionVector rvv = region.getDiskRegion().getRegionVersionVector();
        rvv = rvv.getCloneForTransmission();
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        
        //Using gemfire serialization because 
        //RegionVersionVector is not java serializable
        DataSerializer.writeObject(rvv, hdos);
        return hdos.toByteArray();
      }
    };
    byte[] result= (byte[]) vm.invoke(createData);
    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }

  private void checkIfFullGII(VM vm, final String regionName, final byte[] remote_rvv_bytearray, final boolean expectFullGII) {
    SerializableRunnable checkIfFullGII = new SerializableRunnable("check if full gii") {
      public void run() {
        DistributedRegion rr = (DistributedRegion)getCache().getRegion(regionName);
        ByteArrayInputStream bais = new ByteArrayInputStream(remote_rvv_bytearray);
        RegionVersionVector remote_rvv = null;
        try {
          remote_rvv = DataSerializer.readObject(new DataInputStream(bais));
        } catch (IOException e) {
          Assert.fail("Unexpected exception", e);
        } catch (ClassNotFoundException e) {
          Assert.fail("Unexpected exception", e);
        }
        RequestImageMessage rim = new RequestImageMessage();
        rim.setSender(R_ID);
        boolean isFullGII = rim.goWithFullGII(rr, remote_rvv);
        assertEquals(expectFullGII, isFullGII);
      }
    };
    vm.invoke(checkIfFullGII);
  }
  
  private void doOneClear(final VM vm, final long regionVersionForThisOp) {
    SerializableRunnable clearOp = oneClearOp(regionVersionForThisOp, getMemberID(vm));
    vm.invoke(clearOp);
  }

  private SerializableRunnable oneClearOp(final long regionVersionForThisOp, final DiskStoreID memberID) {
    SerializableRunnable clearOp = new SerializableRunnable("clear now") {
      public void run() {
        DistributedRegion rr = (DistributedRegion)getCache().getRegion(REGION_NAME);
        rr.clear();
        long region_version = getRegionVersionForMember(rr.getVersionVector(), memberID, false);
        long region_gc_version = getRegionVersionForMember(rr.getVersionVector(), memberID, true);
        assertEquals(regionVersionForThisOp, region_version);
        assertEquals(region_version, region_gc_version);
      }
    };
    return clearOp;
  }

  private SerializableRunnable onePutOp(final String key, final String value, final long regionVersionForThisOp, final DiskStoreID memberID) {
    SerializableRunnable putOp = new SerializableRunnable("put "+key) {
      public void run() {
        LocalRegion lr = (LocalRegion)getCache().getRegion(REGION_NAME);
        lr.put(key, value);
        long region_version = getRegionVersionForMember(lr.getVersionVector(), memberID, false);
        assertEquals(regionVersionForThisOp, region_version);
      }
    };
    return putOp;
  }

  private void doOnePut(final VM vm, final long regionVersionForThisOp, final String key) {
    SerializableRunnable putOp = onePutOp(key, generateValue(vm), regionVersionForThisOp, getMemberID(vm));
    vm.invoke(putOp);
  }

  private AsyncInvocation doOnePutAsync(final VM vm, final long regionVersionForThisOp, final String key) {
    SerializableRunnable putOp = onePutOp(key, generateValue(vm), regionVersionForThisOp, getMemberID(vm));
    AsyncInvocation async = vm.invokeAsync(putOp);
    return async;
  }
  
  private String generateValue(final VM vm) {
    return "VALUE from vm"+vm.getPid();
  }
  
  private SerializableRunnable oneDestroyOp(final String key, final String value, final long regionVersionForThisOp, final DiskStoreID memberID) {
    SerializableRunnable destroyOp = new SerializableRunnable("destroy "+key) {
      public void run() {
        LocalRegion lr = (LocalRegion)getCache().getRegion(REGION_NAME);
        lr.destroy(key);
        long region_version = getRegionVersionForMember(lr.getVersionVector(), memberID, false);
        assertEquals(regionVersionForThisOp, region_version);
      }
    };
    return destroyOp;
  }

  private void doOneDestroy(final VM vm, final long regionVersionForThisOp, final String key) {
    SerializableRunnable destroyOp = oneDestroyOp(key, generateValue(vm), regionVersionForThisOp, getMemberID(vm));
    vm.invoke(destroyOp);
  }
  
  private AsyncInvocation doOneDestroyAsync(final VM vm, final long regionVersionForThisOp, final String key) {
    SerializableRunnable destroyOp = oneDestroyOp(key, generateValue(vm), regionVersionForThisOp, getMemberID(vm));
    AsyncInvocation async = vm.invokeAsync(destroyOp);
    return async;
  }
  
  private long getRegionVersionForMember(RegionVersionVector rvv, DiskStoreID member, boolean isRVVGC) {
    long ret = 0;
    if (isRVVGC) {
      ret = rvv.getGCVersion(member);
    } else {
      ret = rvv.getVersionForMember(member);
    }
    return (ret == -1? 0: ret);
  }
  
  private void assertSameRVV(RegionVersionVector rvv1,
      RegionVersionVector rvv2) {
    if(!rvv1.sameAs(rvv2)) {
      fail("Expected " + rvv1 + " but was " + rvv2);
    }
  }

  protected void verifyTombstoneExist(VM vm, final String key, final boolean expectExist, final boolean expectExpired) {
    SerializableRunnable verify = new SerializableRunnable() {
      private boolean doneVerify() {
        Cache cache = getCache();
        LocalRegion lr = (LocalRegion)getCache().getRegion(REGION_NAME);
        NonTXEntry entry = (NonTXEntry)lr.getEntry(key, true);
        if (expectExist) {
          assertTrue(entry != null && entry.getRegionEntry().isTombstone());
        }
        
        System.out.println("GGG:new timeout="+TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT);
        if (entry == null || !entry.getRegionEntry().isTombstone()) {
          return (false == expectExist);
        } else {
          long ts = entry.getRegionEntry().getVersionStamp().getVersionTimeStamp();
          if (expectExpired) {
            return (ts + TombstoneService.REPLICATED_TOMBSTONE_TIMEOUT <= ((GemFireCacheImpl)cache).cacheTimeMillis()); // use MAX_WAIT as timeout
          } else {
            return (true == expectExist);
          }
        }
      }
      
      public void run() {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return doneVerify();
          }
          public String description() {
            return null;
          }
        };
        
        Wait.waitForCriterion(ev, 10 * 1000, 200, true);
        assertTrue(doneVerify());
      }
    };
    vm.invoke(verify);
  }

  protected int getDeltaGIICount(VM vm) {
    SerializableCallable getDelGIICount = new SerializableCallable("getDelGIICount") {
      public Object call() throws Exception {
        GemFireCacheImpl gfc = (GemFireCacheImpl)getCache();
        return gfc.getTombstoneService().getGCBlockCount();
      }
    };
    int result= (Integer)vm.invoke(getDelGIICount);
    return result;
  }

  private void checkAsyncCall(AsyncInvocation async) {
    try {
      async.join(30000);
      if (async.exceptionOccurred()) {
        Assert.fail("Test failed", async.getException());
      }
    } catch (InterruptedException e1) {
      Assert.fail("Test failed", e1);
    }
  }
  
  public static void slowGII(final long[] versionsToBlock) {
    DistributionMessageObserver.setInstance(new BlockMessageObserver(versionsToBlock));
  }
  
  public static void resetSlowGII() {
    BlockMessageObserver observer = (BlockMessageObserver) DistributionMessageObserver.setInstance(null);
    if (observer != null) {
      observer.cdl.countDown();
    }
  }
  
//  private void slowGII(VM vm) {
//    SerializableRunnable slowGII = new SerializableRunnable("Slow down GII") {
//      @SuppressWarnings("synthetic-access")
//      public void run() {
//        slowGII();
//      }
//    };
//    vm.invoke(slowGII);
//  }
//  
//  private void resetSlowGII(VM vm) {
//    SerializableRunnable resetSlowGII = new SerializableRunnable("Unset the slow GII") {
//      public void run() {
//        resetSlowGII();
//      }
//    };
//    vm.invoke(resetSlowGII);
//  }
  
  private static class BlockMessageObserver extends DistributionMessageObserver {
    private long[] versionsToBlock; 

    CountDownLatch cdl = new CountDownLatch(1);

    BlockMessageObserver(long[] versionsToBlock) {
      this.versionsToBlock = versionsToBlock;
    }
    
    @Override
    public void beforeSendMessage(DistributionManager dm, DistributionMessage message) {
      VersionTag tag = null;
      if (message instanceof UpdateMessage) {
        UpdateMessage um = (UpdateMessage)message;
        tag = um.getVersionTag();
      } else if (message instanceof DestroyMessage) {
        DestroyMessage um = (DestroyMessage)message;
        tag = um.getVersionTag();
      } else {
        return;
      }
      try {
        boolean toBlock = false;
        for (long blockversion:versionsToBlock) {
          if (tag.getRegionVersion() == blockversion) {
            toBlock = true;
            break;
          }
        }
        if (toBlock) {
          cdl.await();
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private class Mycallback extends GIITestHook {
    private Object lockObject = new Object();

    public Mycallback(GIITestHookType type, String region_name) {
      super(type, region_name);
    }
    
    @Override
    public void reset() {
      synchronized (this.lockObject) {
        this.lockObject.notify();
      }
    }

    @Override
    public void run() {
      synchronized (this.lockObject) {
        try {
          isRunning = true;
          this.lockObject.wait();
        } catch (InterruptedException e) {
        }
      }
    }
  } // Mycallback
}
