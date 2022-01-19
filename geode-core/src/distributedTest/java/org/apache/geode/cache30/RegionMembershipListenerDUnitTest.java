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

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.util.RegionMembershipListenerAdapter;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Test {@link RegionMembershipListener}
 *
 * @since GemFire 5.0
 */
@Category({MembershipTest.class})
public class RegionMembershipListenerDUnitTest extends JUnit4CacheTestCase {

  private transient MyRML myListener;
  private transient MyRML mySRListener;
  private transient Region r; // root region
  private transient Region sr; // subregion
  protected transient InternalDistributedMember otherId;

  public RegionMembershipListenerDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    DistributedRegion.TEST_HOOK_ADD_PROFILE = true;
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    DistributedRegion.TEST_HOOK_ADD_PROFILE = false;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return props;
  }

  protected VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }

  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
      @Override
      public void run2() throws CacheException {
        getCache();
      }
    });
    otherId = vm.invoke(() -> getSystem().getDistributedMember());
  }

  protected void createRootOtherVm(final String rName) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
      @Override
      public void run2() throws CacheException {
        Region r = createRootRegion(rName, createRootRegionAttributes(null));
        r.createSubregion("mysub", createSubRegionAttributes(null));
      }
    });
  }

  protected RegionAttributes createRootRegionAttributes(CacheListener[] cacheListeners) {
    AttributesFactory af = new AttributesFactory();
    if (cacheListeners != null) {
      af.initCacheListeners(cacheListeners);
    }
    return af.create();
  }

  protected RegionAttributes createSubRegionAttributes(CacheListener[] cacheListeners) {
    return createRootRegionAttributes(cacheListeners);
  }

  protected void destroyRootOtherVm(final String rName) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("local destroy root") {
      @Override
      public void run2() throws CacheException {
        getRootRegion(rName).localDestroyRegion();
      }
    });
  }

  protected void closeRootOtherVm(final String rName) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("close root") {
      @Override
      public void run2() throws CacheException {
        getRootRegion(rName).close();
      }
    });
  }

  private void closeCacheOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("close cache") {
      @Override
      public void run2() throws CacheException {
        getCache().close();
      }
    });
  }

  private void crashCacheOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("crash cache") {
      @Override
      public void run2() throws CacheException {
        // shut down the gms before the distributed system to simulate
        // a crash. In post-5.1.x, this could use SystemFailure.initFailure()
        MembershipManagerHelper.crashDistributedSystem(getCache().getDistributedSystem());
      }
    });
  }

  protected void createRootRegionWithListener(String rName) throws CacheException {
    int to = getOpTimeout();
    myListener = new MyRML(to);
    r =
        createRootRegion(rName, createRootRegionAttributes(new CacheListener[] {myListener}));
    mySRListener = new MyRML(to);
    sr = r.createSubregion("mysub",
        createSubRegionAttributes(new CacheListener[] {mySRListener}));
  }

  public int getOpTimeout() {
    return getSystem().getDistributionManager().getConfig().getMemberTimeout() * 3;
  }

  ////////////////////// Test Methods //////////////////////

  /**
   * tests {@link RegionMembershipListener#initialMembers}
   */
  @Test
  public void testInitialMembers() throws CacheException {
    final String rName = getUniqueName();
    initOtherId();
    createRootRegionWithListener(rName);
    assertInitialMembers(null);
    createRootOtherVm(rName);

    // now close the region in the controller
    // and recreate it and see if initMembers includes otherId
    closeRoots();

    createRootRegionWithListener(rName);
    assertInitialMembers(otherId);
  }

  protected void closeRoots() {
    r.close();
  }

  protected List<DistributedMember> assertInitialMembers(final DistributedMember expectedId) {
    final List<DistributedMember> l;
    if (expectedId == null) {
      l = Arrays.asList(new DistributedMember[] {});
    } else {
      l = Arrays.asList(expectedId);
    }
    assertTrue(myListener.lastOpWasInitialMembers());
    assertEquals(l, myListener.getInitialMembers());
    assertTrue(mySRListener.lastOpWasInitialMembers());
    assertEquals(l, mySRListener.getInitialMembers());
    // test new methods added for #43098
    if (expectedId != null) {
      Cache cache = (Cache) r.getRegionService();
      // assertIndexDetailsEquals(l, new ArrayList(cache.getMembers()));
      assertEquals(l, new ArrayList(cache.getMembers(r)));
      assertEquals(l, new ArrayList(cache.getMembers(sr)));
    }
    return l;
  }

  /**
   * tests {@link RegionMembershipListener#afterRemoteRegionCreate}
   */
  @Test
  public void testCreate() throws CacheException {
    final String rName = getUniqueName();
    initOtherId();
    createRootRegionWithListener(rName);
    createRootOtherVm(rName);
    assertTrue(myListener.lastOpWasCreate());
    {
      RegionEvent e = myListener.getLastEvent();
      assertEquals(otherId, e.getDistributedMember());
      assertEquals(Operation.REGION_CREATE, e.getOperation());
      assertEquals(true, e.isOriginRemote());
      assertEquals(false, e.getOperation().isDistributed());
      assertEquals(r, e.getRegion());
      // the test now uses a hook to get the member's DistributionAdvisor profile in the callback
      // argument
      assertTrue(e.getCallbackArgument() instanceof Profile);
      // assertIndexDetailsEquals(null, e.getCallbackArgument());
    }
    assertTrue(mySRListener.lastOpWasCreate());
    {
      RegionEvent e = mySRListener.getLastEvent();
      assertEquals(otherId, e.getDistributedMember());
      assertEquals(Operation.REGION_CREATE, e.getOperation());
      assertEquals(true, e.isOriginRemote());
      assertEquals(false, e.getOperation().isDistributed());
      assertEquals(sr, e.getRegion());
      // the test now uses a hook to get the member's DistributionAdvisor profile in the callback
      // argument
      assertTrue(e.getCallbackArgument() instanceof Profile);
      // assertIndexDetailsEquals(null, e.getCallbackArgument());
    }
  }

  /**
   * tests {@link RegionMembershipListener#afterRemoteRegionDeparture}
   */
  @Test
  public void testDeparture() throws CacheException {
    final String rName = getUniqueName();
    initOtherId();
    createRootRegionWithListener(rName);
    createRootOtherVm(rName);
    assertOpWasCreate();

    destroyRootOtherVm(rName);
    assertOpWasDeparture();

    createRootOtherVm(rName);
    assertOpWasCreate();

    closeRootOtherVm(rName);
    assertOpWasDeparture();

    createRootOtherVm(rName);
    assertOpWasCreate();

    closeCacheOtherVm();
    assertOpWasDeparture();
  }

  protected void assertOpWasDeparture() {
    assertTrue(myListener.lastOpWasDeparture());
    assertEventStuff(myListener.getLastEvent(), otherId, r);
    assertTrue(mySRListener.lastOpWasDeparture());
    assertEventStuff(mySRListener.getLastEvent(), otherId, sr);
  }

  public static void assertEventStuff(RegionEvent e, DistributedMember em, Region er) {
    assertEquals(em, e.getDistributedMember());
    assertEquals(Operation.REGION_CLOSE, e.getOperation());
    assertEquals(true, e.isOriginRemote());
    assertEquals(false, e.getOperation().isDistributed());
    assertEquals(er, e.getRegion());
    assertEquals(null, e.getCallbackArgument());
  }

  protected void assertOpWasCreate() {
    assertTrue(myListener.lastOpWasCreate());
    assertTrue(mySRListener.lastOpWasCreate());
  }

  /**
   * tests {@link RegionMembershipListener#afterRemoteRegionCrash}
   */
  @Test
  public void testCrash() throws CacheException {
    final String rName = getUniqueName();
    initOtherId();
    createRootRegionWithListener(rName);
    createRootOtherVm(rName);
    try {
      assertTrue(myListener.lastOpWasCreate()); // root region
      assertTrue(mySRListener.lastOpWasCreate()); // subregion
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);

      crashCacheOtherVm();
      int to = getOpTimeout();
      MembershipManagerHelper.waitForMemberDeparture(basicGetSystem(), otherId, to);
      myListener.waitForCrashOp();
      {
        RegionEvent e = myListener.getLastEvent();
        assertEquals(otherId, e.getDistributedMember());
        assertEquals(Operation.REGION_CLOSE, e.getOperation());
        assertEquals(true, e.isOriginRemote());
        assertEquals(false, e.getOperation().isDistributed());
        assertEquals(r, e.getRegion());
        assertEquals(null, e.getCallbackArgument());
      }
      mySRListener.waitForCrashOp();
      {
        RegionEvent e = mySRListener.getLastEvent();
        assertEquals(otherId, e.getDistributedMember());
        assertEquals(Operation.REGION_CLOSE, e.getOperation());
        assertEquals(true, e.isOriginRemote());
        assertEquals(false, e.getOperation().isDistributed());
        assertEquals(sr, e.getRegion());
        assertEquals(null, e.getCallbackArgument());
      }
    } finally {
      MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
      disconnectAllFromDS();
    }
  }

  enum Op {
    Initial, Create, Departure, Crash
  }

  public class MyRML extends RegionMembershipListenerAdapter {
    private final int timeOut;
    volatile Op lastOp;
    private volatile RegionEvent lastEvent;
    private volatile DistributedMember[] initialMembers;
    private volatile boolean memberInitialized; // was the member initialized when
                                                // afterRemoteRegionCreate was called?

    public MyRML(int to) {
      timeOut = to;
    }

    public boolean lastOpWasInitialMembers() {
      return waitForOp(Op.Initial);
    }

    public boolean lastOpWasCreate() {
      boolean result = waitForOp(Op.Create);
      if (result) {
        // bug #44684 - afterRemoteRegionCreate should not be invoked before the remote region is
        // initialized
        assertTrue(
            "bug #44684 - expected remote member to be initialized when afterRemoteRegionCreate was invoked",
            memberInitialized);
      }
      return result;
    }

    public boolean lastOpWasDeparture() {
      return waitForOp(Op.Departure);
    }

    public String getOpName(Op op) {
      if (op == null) {
        return "null";
      }
      switch (op) {
        case Initial:
          return "Initial";
        case Create:
          return "Create";
        case Departure:
          return "Departure";
        case Crash:
          return "Crash";
        default:
          return "Unknown";
      }
    }

    private boolean waitForOp(final Op op) {
      WaitCriterion ev = new WaitCriterion() {
        @Override
        public boolean done() {
          return lastOp == op;
        }

        @Override
        public String description() {
          return MyRML.this + " waiting for Op " + op + " when lastOp was "
              + getOpName(lastOp);
        }
      };
      getLogWriter().info(this + " waiting for Op " + getOpName(op)
          + " when lastOp was " + getOpName(lastOp));
      GeodeAwaitility.await().untilAsserted(ev);
      assertEquals(op, lastOp);
      return true;
    }

    public void waitForCrashOp() {
      waitForOp(Op.Crash);
    }

    public RegionEvent getLastEvent() {
      return lastEvent;
    }

    public List getInitialMembers() {
      return Arrays.asList(initialMembers);
    }

    @Override
    public void initialMembers(Region r, DistributedMember[] initialMembers) {
      lastOp = Op.Initial;
      lastEvent = null;
      this.initialMembers = initialMembers;
      LogWriterUtils.getLogWriter()
          .info(this + " received initialMembers notification for region " + r
              + " with members " + Arrays.deepToString(initialMembers));
    }

    @Override
    public void afterRemoteRegionCreate(RegionEvent event) {
      lastOp = Op.Create;
      lastEvent = event;
      CacheProfile cacheProfile = (CacheProfile) event.getCallbackArgument();
      if (cacheProfile != null) {
        memberInitialized = cacheProfile.regionInitialized;
        if (!memberInitialized) {
          LogWriterUtils.getLogWriter().warning(
              "afterRemoteRegionCreate invoked when member is not done initializing!",
              new Exception("stack trace"));
        }
        LogWriterUtils.getLogWriter().info(
            this + " received afterRemoteRegionCreate notification for event " + event);
      } else {
        LogWriterUtils.getLogWriter().warning(
            "afterRemoteRegionCreate was expecting a profile in the event callback but there was none. "
                + " This indicates a problem with the test hook DistributedRegion.TEST_HOOK_ADD_PROFILE");
      }
    }

    @Override
    public void afterRemoteRegionDeparture(RegionEvent event) {
      lastOp = Op.Departure;
      lastEvent = event;
      LogWriterUtils.getLogWriter().info(
          this + " received afterRemoteRegionDeparture notification for event " + event);
    }

    @Override
    public void afterRemoteRegionCrash(RegionEvent event) {
      lastOp = Op.Crash;
      lastEvent = event;
      LogWriterUtils.getLogWriter().info(
          this + " received afterRemoteRegionCrash notification for event " + event);
    }
  }
}
