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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHook;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHookType;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionMap;

public class ConcurrentLeaveDuringGIIDUnitTest extends CacheTestCase {

  public ConcurrentLeaveDuringGIIDUnitTest(String name) {
    super(name);
  }
  
  public void testRemoveWhenBug50988IsFixed() {
    // remove this placeholder
  }
  /**
   * In #48962 a member X has replicated region and is updating it.  Members A and B
   * are started up in parallel.  At the same time X decides to close the region.
   * Member A manages to tell X that it's creating the region and receives an
   * update that B does not see.  B finishes GII with no content and then A
   * gets its initial image from B, leaving an inconsistency between them.
   * <p>
   * The test installs a GII hook in A that causes it to pause after announcing
   * creation of the region.
   * <p>
   * X then creates its region and does an operation and closes its cache.
   * <p>
   * B then starts and creates its region, not doing a GII from A since A is
   * still initializing.
   * <p>
   * A is then allowed to start its GII and pulls an image from B.
   * 
   */
  public void bug50988_testBug48962() throws Exception {
    VM X = Host.getHost(0).getVM(1);
    VM A = Host.getHost(0).getVM(2);
    VM B = Host.getHost(0).getVM(3);
    
    final String regionName = getUniqueName() + "_Region";
    
    SerializableCallable createRegionXB = new SerializableCallable("create region in X and B") {
      public Object call() {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        Object result = null;
        if (VM.getCurrentVMNum() == 1) { // VM X 
          r.put("keyFromX", "valueFromX");
          result = getCache().getDistributedSystem().getDistributedMember();
          r.getCache().getDistributedSystem().disconnect();
        } else { // VM B
          // B will not do a GII and X never knows about B
          assertFalse(r.containsKey("keyFromX"));
          result = getCache().getDistributedSystem().getDistributedMember();
        }
        return result;
      }
    };
    
    SerializableCallable createRegionA = new SerializableCallable("create region in A") {
      public Object call() {
        final GiiCallback cb = new GiiCallback(InitialImageOperation.GIITestHookType.BeforeGetInitialImage, regionName);
        InitialImageOperation.setGIITestHook(cb);
        Thread t = new Thread("create region in a thread that will block before GII") {
          public void run() {
            Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
          }
        };
        t.start();
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return cb.isRunning;
          }
          public String description() {
            return "waiting for GII test hook to be invoked";
          }
        };
        Wait.waitForCriterion(wc, 20000, 500, true);
        return getCache().getDistributedSystem().getDistributedMember();
      }
    };

    A.invoke(createRegionA);
    
    final InternalDistributedMember Xid = (InternalDistributedMember)X.invoke(createRegionXB);

    A.invoke(new SerializableRunnable("make sure A got keyFromX from X") {
      public void run() {
        // use internal methods to get the region since it's still initializing
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache(); 
        final RegionMap r = cache.getRegionByPathForProcessing(regionName).getRegionMap(); 

        // X's update should have been propagated to A and put into the cache.
        // If this throws an assertion error then there's no point in
        // continuing the test because we didn't set up the initial
        // condition needed for the next step.
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return r.containsKey("keyFromX");
          }
          public String description() {
            return "waiting for region " + regionName + " to contain keyFromX";
          }
        };
        Wait.waitForCriterion(wc, 20000, 1000, true);
      }
    });
    
    // create in B and make sure the key isn't there
    B.invoke(createRegionXB);
    
    A.invoke(new SerializableRunnable("allow A to continue GII from B") {
      public void run() {
        GiiCallback cb = (GiiCallback)InitialImageOperation.getGIITestHookForCheckingPurpose(
            InitialImageOperation.GIITestHookType.BeforeGetInitialImage);
        synchronized(cb.lockObject) {
          cb.lockObject.notify();
        }
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return getCache().getRegion(regionName) != null;
          }
          public String description() {
            return "waiting for region " + regionName + " to initialize";
          }
        };
        Wait.waitForCriterion(wc, 20000, 1000, true);
        // ensure that the RVV has recorded the event
        DistributedRegion r = (DistributedRegion)getCache().getRegion(regionName);
        if (!r.getVersionVector().contains(Xid, 1)) {
          LogWriterUtils.getLogWriter().info("r's version vector is " + r.getVersionVector().fullToString());
          ((LocalRegion)r).dumpBackingMap();
        }
        assertTrue(r.containsKey("keyFromX"));
        // if the test fails here then the op received from X was not correctly
        // picked up and recorded in the RVV
        assertTrue(r.getVersionVector().contains(Xid, 1));
      }
    });
    
    // Now ensure the B has done the sync and received the entry
    B.invoke(new SerializableRunnable("ensure B is now consistent") {
      public void run() {
        final Region r = getCache().getRegion(regionName);
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return r.containsKey("keyFromX");
          }
          public String description() {
            return "waiting for region " + regionName + " to contain keyFromX";
          }
        };
        // if the test fails here then a sync from B to A was not performed
        Wait.waitForCriterion(wc, 20000, 500, true);
        // if the test fails here something is odd because the sync was done
        // but the RVV doesn't know about it
        assertTrue(((LocalRegion)r).getVersionVector().contains(Xid, 1));
      }
    });
  }

  private class GiiCallback extends GIITestHook {
    private Object lockObject = new Object();

    public GiiCallback(GIITestHookType type, String region_name) {
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
