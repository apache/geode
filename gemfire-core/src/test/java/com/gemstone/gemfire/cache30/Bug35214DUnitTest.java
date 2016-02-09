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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Make sure entry expiration does not happen during gii for bug 35214
 *
 * @author darrel
 * @since 5.0
 */
public class Bug35214DUnitTest extends CacheTestCase {

  protected volatile int expirationCount = 0;

  private final static int ENTRY_COUNT = 100;

  protected static volatile boolean callbackFailure;
  
  public Bug35214DUnitTest(String name) {
    super(name);
  }

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }
    
  private void initOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("init") {
        public void run2() throws CacheException {
          getCache();
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_ACK);
          Region r1 = createRootRegion("r1", af.create());
          for (int i=1; i <= ENTRY_COUNT; i++) {
            r1.put("key"+i, "value" + i);
          }
        }
      });
  }
  
  private AsyncInvocation updateOtherVm() throws Throwable {
    VM vm = getOtherVm();
    AsyncInvocation otherUpdater = vm.invokeAsync(new CacheSerializableRunnable("update") {
        public void run2() throws CacheException {
          Region r1 = getRootRegion("r1");
          // let the main guys gii get started; we want to do updates
          // during his gii
          {
            // wait for profile of getInitialImage cache to show up
            com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor adv =
              ((com.gemstone.gemfire.internal.cache.DistributedRegion)r1).getCacheDistributionAdvisor();
            int numProfiles;
            int expectedProfiles = 1;
            for (;;) {
              numProfiles = adv.adviseInitialImage(null).getReplicates().size();
              if (numProfiles < expectedProfiles) {
                //            getLogWriter().info("PROFILE CHECK: Found " + numProfiles +
                //              " getInitialImage Profiles (waiting for " + expectedProfiles + ")");
                //pause(5);
              }
              else {
                LogWriterUtils.getLogWriter().info("PROFILE CHECK: Found " + numProfiles + " getInitialImage Profiles (OK)");
                break;
              }
            }
          }
          // start doing updates of the keys to see if we can get deadlocked
          int updateCount = 1;
          do {
            for (int i=1; i <= ENTRY_COUNT; i++) {
              String key = "key" + i;
              if (r1.containsKey(key)) {
                r1.destroy(key);
              } else {
                r1.put(key, "value" + i + "uc" + updateCount);
              }
            }
          } while (updateCount++ < 20);
          // do one more loop with no destroys
          for (int i=1; i <= ENTRY_COUNT; i++) {
            String key = "key" + i;
            if (!r1.containsKey(key)) {
              r1.put(key, "value" + i + "uc" + updateCount);
            }
          }
        }
      });
    
    // FIXME this thread does not terminate
//    DistributedTestCase.join(otherUpdater, 5 * 60 * 1000, getLogWriter());
//    if(otherUpdater.exceptionOccurred()){
//      fail("otherUpdater failed", otherUpdater.getException());
//    }
  
    return otherUpdater;
  }

  //////////////////////  Test Methods  //////////////////////

  protected boolean afterRegionCreateSeen = false;

  protected static void callbackAssertTrue(String msg, boolean cond) {
    if (cond)
      return;
    callbackFailure = true;
    // Throws ignored error, but...
    assertTrue(msg, cond);
  }
  
 
  /**
   * make sure entries do not expire during a GII
   */
  public void testNoEntryExpireDuringGII() throws Exception {
    initOtherVm();
    AsyncInvocation updater = null;
    try {
      updater = updateOtherVm();
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable e1) {
      Assert.fail("failed due to "+e1, e1);
    }
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 30;
    callbackFailure = false;
    
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.REPLICATE);
      af.setScope(Scope.DISTRIBUTED_ACK);
      af.setStatisticsEnabled(true);
      af.setEntryIdleTimeout(new ExpirationAttributes(1, ExpirationAction.INVALIDATE));
      CacheListener cl1 = new CacheListenerAdapter() {
          public void afterRegionCreate(RegionEvent re) {
            afterRegionCreateSeen = true;
          }
          public void afterInvalidate(EntryEvent e) {
            callbackAssertTrue("afterregionCreate not seen",
                afterRegionCreateSeen);
            // make sure region is initialized
            callbackAssertTrue("not initialized", 
                ((LocalRegion)e.getRegion()).isInitialized());
            expirationCount++;
            com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
          }
        };
      af.addCacheListener(cl1);
      final Region r1 = createRootRegion("r1", af.create());
      ThreadUtils.join(updater, 60 * 1000);
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return r1.values().size() == 0;
        }
        public String description() {
          return "region never became empty";
        }
      };
      Wait.waitForCriterion(ev, 2 * 1000, 200, true);
      {
        assertEquals(0, r1.values().size());
        assertEquals(ENTRY_COUNT, r1.keys().size());
      }

    } 
    finally {
      com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      assertEquals(null, System.getProperty(LocalRegion.EXPIRY_MS_PROPERTY));
    }
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }
}
