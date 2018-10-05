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
package org.apache.geode.cache30;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.internal.cache.InitialImageOperation.GIITestHookType.BeforeGetInitialImage;
import static org.apache.geode.internal.cache.InitialImageOperation.getGIITestHookForCheckingPurpose;
import static org.apache.geode.internal.cache.InitialImageOperation.setGIITestHook;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation.GIITestHook;
import org.apache.geode.internal.cache.InitialImageOperation.GIITestHookType;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class ConcurrentLeaveDuringGIIDUnitTest extends JUnit4CacheTestCase {

  public ConcurrentLeaveDuringGIIDUnitTest() {
    super();
  }

  @Test
  public void testRemoveWhenBug50988IsFixed() {
    // remove this placeholder
  }

  /**
   * In #48962 a member X has replicated region and is updating it. Members A and B are started up
   * in parallel. At the same time X decides to close the region. Member A manages to tell X that
   * it's creating the region and receives an update that B does not see. B finishes GII with no
   * content and then A gets its initial image from B, leaving an inconsistency between them.
   * <p>
   * The test installs a GII hook in A that causes it to pause after announcing creation of the
   * region.
   * <p>
   * X then creates its region and does an operation and closes its cache.
   * <p>
   * B then starts and creates its region, not doing a GII from A since A is still initializing.
   * <p>
   * A is then allowed to start its GII and pulls an image from B.
   *
   */
  @Ignore
  @Test
  public void testBug48962() throws Exception {
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
        final GiiCallback cb = new GiiCallback(
            BeforeGetInitialImage, regionName);
        setGIITestHook(cb);
        Thread t = new Thread("create region in a thread that will block before GII") {
          public void run() {
            Region r = getCache().createRegionFactory(REPLICATE).create(regionName);
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
        GeodeAwaitility.await().untilAsserted(wc);
        return getCache().getDistributedSystem().getDistributedMember();
      }
    };

    A.invoke(createRegionA);

    final InternalDistributedMember Xid = (InternalDistributedMember) X.invoke(createRegionXB);

    A.invoke(new SerializableRunnable("make sure A got keyFromX from X") {
      public void run() {
        // use internal methods to get the region since it's still initializing
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
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
        GeodeAwaitility.await().untilAsserted(wc);
      }
    });

    // create in B and make sure the key isn't there
    B.invoke(createRegionXB);

    A.invoke(new SerializableRunnable("allow A to continue GII from B") {
      public void run() {
        GiiCallback cb = (GiiCallback) getGIITestHookForCheckingPurpose(
            BeforeGetInitialImage);
        synchronized (cb.lockObject) {
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
        GeodeAwaitility.await().untilAsserted(wc);
        // ensure that the RVV has recorded the event
        DistributedRegion r = (DistributedRegion) getCache().getRegion(regionName);
        if (!r.getVersionVector().contains(Xid, 1)) {
          getLogWriter()
              .info("r's version vector is " + r.getVersionVector().fullToString());
          ((LocalRegion) r).dumpBackingMap();
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
        GeodeAwaitility.await().untilAsserted(wc);
        // if the test fails here something is odd because the sync was done
        // but the RVV doesn't know about it
        assertTrue(((LocalRegion) r).getVersionVector().contains(Xid, 1));
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
