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
/*
 * ClearRvvLockingDUnitTest.java
 *
 * Created on September 6, 2005, 2:57 PM
 */
package com.gemstone.gemfire.internal.cache;

import static com.gemstone.gemfire.internal.lang.ThrowableUtils.hasCauseMessage;
import static com.gemstone.gemfire.test.dunit.Assert.fail;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Abstract superclass of {@link Region} tests that involve more than
 * one VM.
 */

@Category(DistributedTest.class)
public class ClearRvvLockingDUnitTest extends JUnit4CacheTestCase { // TODO: reformat

  VM vm0, vm1;
  static Cache cache;
  static Region region;
  DistributedMember vm0ID, vm1ID;
  
  static final String THE_KEY = "cckey";

  private static final Logger logger = LogService.getLogger();

  private static CacheObserver origObserver;

  //test methods

  @Test
  public void testPutOperation(){
    setupMembers();    
    runConsistencyTest(vm0, performPutOperation);
    checkForConsistencyErrors();   
  }
  
  @Test
  public void testRemoveOperation(){
    setupMembers();    
    runConsistencyTest(vm0, performRemoveOperation);
    checkForConsistencyErrors();   
  }
  
  @Test
  public void testInvalidateOperation(){
    setupMembers();    
    runConsistencyTest(vm0, performInvalidateOperation);
    checkForConsistencyErrors();   
  }
  
  @Test
  public void testPutAllOperation(){
    setupMembers();    
    runConsistencyTest(vm0, performPutAllOperation);
    checkForConsistencyErrors();   
  }
  
  @Test
  public void testRemoveAllOperation(){
    setupMembers();    
    runConsistencyTest(vm0, performRemoveAllOperation);
    checkForConsistencyErrors();   
  }
  
  private void setupMembers() {    
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm0ID = createCache(vm0);
    vm1ID = createCache(vm1);
    vm0.invoke(() -> ClearRvvLockingDUnitTest.createRegion());
    vm1.invoke(() -> ClearRvvLockingDUnitTest.createRegion());
  }
  
  private void runConsistencyTest(VM vm, SerializableRunnableIF theTest) {
    AsyncInvocation a0 = vm.invokeAsync(theTest);
    waitForAsyncProcessing(a0, "");
  }

  private void checkForConsistencyErrors() {
    StringBuffer sb = new StringBuffer();
    
    Map r0Contents = (Map)vm0.invoke(() -> this.getRegionContents());
    Map r1Contents = (Map)vm1.invoke(() -> this.getRegionContents());

    for (int i=0; i<10; i++) {
      String key = THE_KEY + i;
      try {
        assertEquals("region contents are not consistent for key " + key, r0Contents.get(key), r1Contents.get(key));
        assertEquals("region entries are not consistent for key " + key, checkRegionEntry(vm0, key), checkRegionEntry(vm1, key));
      } catch(AssertionError ae) {
        sb.append(ae.getMessage() + "\n+");
      }
      for (int subi=1; subi<3; subi++) {
        String subkey = key + "-" + subi;
        try {
        if (r0Contents.containsKey(subkey)) {
          assertEquals("region contents are not consistent", r0Contents.get(subkey), r1Contents.get(subkey));
        } else {
          assertTrue("expected containsKey("+subkey+") to return false", !r1Contents.containsKey(subkey));
        }
        assertEquals("expected checkRegionEntry("+subkey+") to match", checkRegionEntry(vm0, key), checkRegionEntry(vm1, key));
        } catch(AssertionError ae) {
          sb.append(ae.getMessage() + "\n+");
        }
      }
    }
    if(sb.length()!=0) {
      throw(new AssertionError(sb.toString()));
    }
  }
  
  public void invokePut(VM whichVM) {
    if(whichVM==null) {
      doPut();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doPut());
    }
  }
  
  public void invokePut2(VM whichVM) {
    if(whichVM==null) {
      doPut2();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doPut2());
    }
  }
  
  public static void doPut() {
    region.put(THE_KEY + 1, "VALUE1");
  }

  public static void doPut2() {
    region.put(THE_KEY + 1, "DIFFERENT1");
  }

  public void invokeRemove(VM whichVM) {
    if(whichVM==null) {
      doRemove();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doRemove());
    }
  }
  
  public static void doRemove() {
    region.remove(THE_KEY + 1);
  }

  public void invokeInvalidate(VM whichVM) {
    if(whichVM==null) {
      doInvalidate();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doInvalidate());
    }
  }
  
  public static void doInvalidate() {
    region.invalidate(THE_KEY + 1);
  }

  public void invokePutAll(VM whichVM) {
    if(whichVM==null) {
      doPutAll();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doPutAll());
    }
  }
  
  public static void doPutAll() {
    Map map = generateKeyValues();
    region.putAll(map, "putAllCallback");
  }

  public void invokeRemoveAll(VM whichVM) {
    if(whichVM==null) {
      doRemoveAll();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doRemoveAll());
    }
  }
  
  public static void doRemoveAll() {
    Map map = generateKeyValues();
    region.removeAll(map.keySet(), "removeAllCallback");
  }

  private static Map generateKeyValues() {
    String key = THE_KEY + 1;
    String value = "VALUE1";
    Map map = new HashMap();
    map.put(key, value);
    map.put(key+"-1", value);
    map.put(key+"-2", value);
    return map;
  }
  
  public void invokeClear(VM whichVM) {
    if(whichVM==null) {
      doClear();
    } else {
    whichVM.invoke(() -> ClearRvvLockingDUnitTest.doClear());
    }
  }
  
  public static void doClear() {
    region.clear();
  }

  SerializableRunnable performPutOperation = new SerializableRunnable("perform REMOVE") {
    @Override
    public void run() {
      try {
        setHook();
        invokePut(null);
      } catch (Exception e) {
        fail("while performing REMOVE", e);
      }
    }
  };

  SerializableRunnable performRemoveOperation = new SerializableRunnable("perform PUT") {
    @Override
    public void run() {
      try {
        invokePut(null);
        setHook();
        invokeRemove(null);
      } catch (Exception e) {
        fail("while performing PUT", e);
      }
    }
  };

  SerializableRunnable performInvalidateOperation = new SerializableRunnable("perform INVALIDATE") {
    @Override
    public void run() {
      try {
        invokePut(null);
        setHook();
        invokeInvalidate(null);
      } catch (Exception e) {
        fail("while performing INVALIDATE", e);
      }
    }
  };

  SerializableRunnable performPutAllOperation = new SerializableRunnable("perform PUTALL") {
    @Override
    public void run() {
      try {
        setHook();
        invokePutAll(null);
      } catch (Exception e) {
        fail("while performing PUTALL", e);
      }
    }
  };

  SerializableRunnable performRemoveAllOperation = new SerializableRunnable("perform REMOVEALL") {
    @Override
    public void run() {
      try {
        invokePutAll(null);
        setHook();
        invokeRemoveAll(null);
      } catch (Exception e) {
        fail("while performing REMOVEALL", e);
      }
    }
  };

  private void setHook() {
    RegionVersionVector.RvvLockTestHook rvvHook = new RvvTestHook();
    RegionVersionVector myRVV = ((LocalRegion) region).getVersionVector();
    myRVV.setRvvLockTestHook(rvvHook);
  }
  
  public static void setObserver() {
    CacheObserverImpl observer = new CacheObserverImpl();
    origObserver = CacheObserverHolder.setInstance(observer);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
  }

  public static void resetObserver()
  {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=false;
    CacheObserverHolder.setInstance(origObserver);
  }

  public static class CacheObserverImpl extends CacheObserverAdapter {  
    public void afterRegionClear(RegionEvent event){
      return;
    }
  }

  protected boolean waitForAsyncProcessing(AsyncInvocation async, String expectedError) {
    boolean failed = false;
    try {
      async.getResult();
    } catch (Throwable e) {
      assertTrue(hasCauseMessage(e, expectedError));
    }
    return failed;
  }

  public static Boolean checkRegionEntry(VM vm, String key) {
    Boolean target = (Boolean)vm.invoke(() -> ClearRvvLockingDUnitTest.getRegionEntry(key)); 
    return target;
  }

  public static Boolean getRegionEntry(String key) {
    Boolean exists = (!(((LocalRegion)region).getRegionEntry(key)==null));
    logger.info("getRegionEntry for " + key + " was " + exists);
    return exists;
  }

  public static Map getRegionContents() {
    Map result = new HashMap();
    for (Iterator i=region.entrySet().iterator(); i.hasNext(); ) {
      Region.Entry e = (Region.Entry)i.next();
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }

  public static long getRegionVersion(DistributedMember memberID) {    
    logger.info("vmID received by getRegionVersion is: " + memberID);
    return ((LocalRegion)region).getVersionVector().getVersionForMember((VersionSource)memberID);
  }

  private InternalDistributedMember createCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable() {
      public Object call() {
        cache = getCache();
        return getSystem().getDistributedMember();
      }
    });
  }

  public static void createRegion() {
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).setConcurrencyChecksEnabled(true).create("TestRegion");
  }
  
  /*
   * Test callback class used to hook the rvv locking mechanism
   * After the above hookRVV PUT completes, it will unlock the RVV.
   * This occcurs before the PUT is distributed to other members.
   * At this point the afterRelease will initiate a CLEAR and then
   * wait for the clear to complete, leaving VM0 region empty.
   * Once the clear completes, distribution of the PUT is allowed 
   * to complete, resulting in an empty VM0 and a put populated VM1.
   *  
   */
  public class RvvTestHook implements RegionVersionVector.RvvLockTestHook {

    @Override
    public void beforeLock(RegionVersionVector rvv) {}

    @Override
    public void afterLock(RegionVersionVector rvv) {}

    @Override
    public void beforeRelease(RegionVersionVector rvv) {}

    @Override
    public void afterRelease(RegionVersionVector rvv) {
      logger.info("SAJHOOK hit afterRelease");
      invokeClear(vm1);
    }
  }
}
