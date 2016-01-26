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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionMembershipListener;
import com.gemstone.gemfire.cache.util.RegionMembershipListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.CacheProfile;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test {@link RegionMembershipListener}
 *
 * @author darrel
 * @since 5.0
 */
public class RegionMembershipListenerDUnitTest extends CacheTestCase {

  private transient MyRML myListener;
  private transient MyRML mySRListener;
  private transient Region r; // root region
  private transient Region sr; // subregion
  protected transient DistributedMember otherId;
  
  public RegionMembershipListenerDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    DistributedRegion.TEST_HOOK_ADD_PROFILE = true;
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    DistributedRegion.TEST_HOOK_ADD_PROFILE = false;
  }

  protected VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }
    
  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
        public void run2() throws CacheException {
          getCache();
        }
      });
    this.otherId = (DistributedMember)vm.invoke(RegionMembershipListenerDUnitTest.class, "getVMDistributedMember");
  }
  protected void createRootOtherVm(final String rName) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
        public void run2() throws CacheException {
          Region r= createRootRegion(rName, createRootRegionAttributes(null));
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
        public void run2() throws CacheException {
          getRootRegion(rName).localDestroyRegion();
        }
      });
  }
  protected void closeRootOtherVm(final String rName) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("close root") {
        public void run2() throws CacheException {
          getRootRegion(rName).close();
        }
      });
  }
  private void closeCacheOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("close cache") {
        public void run2() throws CacheException {
          getCache().close();
        }
      });
  }

  private void crashCacheOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("crash cache") {
        public void run2() throws CacheException {
          // shut down the gms before the distributed system to simulate
          // a crash.  In post-5.1.x, this could use SystemFailure.initFailure()
          GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
          InternalDistributedSystem sys = (InternalDistributedSystem)cache.getDistributedSystem();
          MembershipManagerHelper.crashDistributedSystem(sys);
        }
      });
  }

  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
  
  protected void createRootRegionWithListener(String rName) throws CacheException {
    int to = getOpTimeout();
    this.myListener = new MyRML(to);
    this.r = createRootRegion(rName, createRootRegionAttributes(new CacheListener[]{this.myListener}));
    this.mySRListener = new MyRML(to);
    this.sr = this.r.createSubregion("mysub", createSubRegionAttributes(new CacheListener[]{this.mySRListener}));
  }
  
  public int getOpTimeout() {
    return getSystem().getDistributionManager().getConfig().getMemberTimeout() * 3;    
  }
  
  //////////////////////  Test Methods  //////////////////////

  /**
   * tests {@link RegionMembershipListener#initialMembers}
   */
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
    assertInitialMembers(this.otherId);
  }
  protected void closeRoots() {
    this.r.close();
  }
  protected List<DistributedMember> assertInitialMembers(final DistributedMember expectedId) {
    final List<DistributedMember> l;
    if (expectedId == null) {
      l = Arrays.asList(new DistributedMember[]{});
    } else {
      l = Arrays.asList(new DistributedMember[]{expectedId});
    }
    assertTrue(this.myListener.lastOpWasInitialMembers());
    assertEquals(l, this.myListener.getInitialMembers());
    assertTrue(this.mySRListener.lastOpWasInitialMembers());
    assertEquals(l, this.mySRListener.getInitialMembers());
    // test new methods added for #43098
    if (expectedId != null) {
      Cache cache = (Cache)this.r.getRegionService();
      //assertEquals(l, new ArrayList(cache.getMembers()));
      assertEquals(l, new ArrayList(cache.getMembers(this.r)));
      assertEquals(l, new ArrayList(cache.getMembers(this.sr)));
    }
    return l;
  }

  /**
   * tests {@link RegionMembershipListener#afterRemoteRegionCreate}
   */
  public void testCreate() throws CacheException {
    final String rName = getUniqueName();
    initOtherId();
    createRootRegionWithListener(rName);
    createRootOtherVm(rName);
    assertTrue(this.myListener.lastOpWasCreate());
    {
      RegionEvent e = this.myListener.getLastEvent();
      assertEquals(this.otherId, e.getDistributedMember());
      assertEquals(Operation.REGION_CREATE, e.getOperation());
      assertEquals(true, e.isOriginRemote());
      assertEquals(false, e.isDistributed());
      assertEquals(this.r, e.getRegion());
      // the test now uses a hook to get the member's DistributionAdvisor profile in the callback argument
      assertTrue(e.getCallbackArgument() instanceof Profile);
//      assertEquals(null, e.getCallbackArgument());
    }
    assertTrue(this.mySRListener.lastOpWasCreate());
    {
      RegionEvent e = this.mySRListener.getLastEvent();
      assertEquals(this.otherId, e.getDistributedMember());
      assertEquals(Operation.REGION_CREATE, e.getOperation());
      assertEquals(true, e.isOriginRemote());
      assertEquals(false, e.isDistributed());
      assertEquals(this.sr, e.getRegion());
      // the test now uses a hook to get the member's DistributionAdvisor profile in the callback argument
      assertTrue(e.getCallbackArgument() instanceof Profile);
//      assertEquals(null, e.getCallbackArgument());
    }
  }
  /**
   * tests {@link RegionMembershipListener#afterRemoteRegionDeparture}
   */
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
    assertTrue(this.myListener.lastOpWasDeparture());
    assertEventStuff(this.myListener.getLastEvent(), this.otherId, this.r);
    assertTrue(this.mySRListener.lastOpWasDeparture());
    assertEventStuff(this.mySRListener.getLastEvent(), this.otherId, this.sr);
  }
  public static void assertEventStuff(RegionEvent e, DistributedMember em, Region er) {
    assertEquals(em, e.getDistributedMember());
    assertEquals(Operation.REGION_CLOSE, e.getOperation());
    assertEquals(true, e.isOriginRemote());
    assertEquals(false, e.isDistributed());
    assertEquals(er, e.getRegion());
    assertEquals(null, e.getCallbackArgument());
  }
  protected void assertOpWasCreate() {
    assertTrue(this.myListener.lastOpWasCreate());
    assertTrue(this.mySRListener.lastOpWasCreate());
  }

  /**
   * tests {@link RegionMembershipListener#afterRemoteRegionCrash}
   */
  public void testCrash() throws CacheException {
    final String rName = getUniqueName();
    initOtherId();
    createRootRegionWithListener(rName);
    createRootOtherVm(rName);
    try {
      assertTrue(this.myListener.lastOpWasCreate()); // root region
      assertTrue(this.mySRListener.lastOpWasCreate()); // subregion
      MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
      
      crashCacheOtherVm();
      int to = getOpTimeout();
      MembershipManagerHelper.waitForMemberDeparture(system, this.otherId, to);
      this.myListener.waitForCrashOp();
      {
        RegionEvent e = this.myListener.getLastEvent();
        assertEquals(this.otherId, e.getDistributedMember());
        assertEquals(Operation.REGION_CLOSE, e.getOperation());
        assertEquals(true, e.isOriginRemote());
        assertEquals(false, e.getOperation().isDistributed());
        assertEquals(this.r, e.getRegion());
        assertEquals(null, e.getCallbackArgument());
      }
      this.mySRListener.waitForCrashOp();
      {
        RegionEvent e = this.mySRListener.getLastEvent();
        assertEquals(this.otherId, e.getDistributedMember());
        assertEquals(Operation.REGION_CLOSE, e.getOperation());
        assertEquals(true, e.isOriginRemote());
        assertEquals(false, e.getOperation().isDistributed());
        assertEquals(this.sr, e.getRegion());
        assertEquals(null, e.getCallbackArgument());
      }
    } finally {
      MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
      disconnectAllFromDS();
    }
  }
  enum Op {Initial, Create, Departure, Crash};  
  public class MyRML extends RegionMembershipListenerAdapter {
    private final int timeOut; 
    volatile Op lastOp;  
    private volatile RegionEvent lastEvent;
    private volatile DistributedMember[] initialMembers;
    private volatile boolean memberInitialized; // was the member initialized when afterRemoteRegionCreate was called?
    
    public MyRML(int to) {  this.timeOut = to;  }
    
    public boolean lastOpWasInitialMembers() {
      return waitForOp(Op.Initial);
    }
    public boolean lastOpWasCreate() {
      boolean result =  waitForOp(Op.Create);
      if (result) {
        // bug #44684 - afterRemoteRegionCreate should not be invoked before the remote region is initialized
        assertTrue("bug #44684 - expected remote member to be initialized when afterRemoteRegionCreate was invoked",
            this.memberInitialized);
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
      case Initial: return "Initial";
      case Create: return "Create";
      case Departure: return "Departure";
      case Crash: return "Crash";
      default: return "Unknown";
      }
    }
    private boolean waitForOp(final Op op) {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return MyRML.this.lastOp == op;
        }
        public String description() {
          return MyRML.this.toString() + " waiting for Op " + op + " when lastOp was " + getOpName(MyRML.this.lastOp);
        }
      };
      getLogWriter().info(this.toString() + " waiting for Op " + getOpName(op)
          + " when lastOp was " + getOpName(this.lastOp));
      DistributedTestCase.waitForCriterion(ev, this.timeOut, 200, true);
      assertEquals(op, this.lastOp);
      return true;
    }
    
    public void waitForCrashOp() {
      waitForOp(Op.Crash);
    }
    public RegionEvent getLastEvent() {
      return this.lastEvent;
    }
    public List getInitialMembers() {
      return Arrays.asList(this.initialMembers);
    }
    
    public void initialMembers(Region r, DistributedMember[] initialMembers) {
      this.lastOp = Op.Initial;
      this.lastEvent = null;
      this.initialMembers = initialMembers;
      getLogWriter().info(this.toString() + " received initialMembers notification for region " + r
          + " with members " + Arrays.deepToString(initialMembers));
    }
    public void afterRemoteRegionCreate(RegionEvent event) {
      this.lastOp = Op.Create;
      this.lastEvent = event;
      CacheProfile cacheProfile = (CacheProfile)event.getCallbackArgument();
      if (cacheProfile != null) {
        this.memberInitialized = cacheProfile.regionInitialized;
        if (!this.memberInitialized) {
          getLogWriter().warning("afterRemoteRegionCreate invoked when member is not done initializing!", new Exception("stack trace"));
        }
        getLogWriter().info(this.toString() + " received afterRemoteRegionCreate notification for event " + event);
      } else {
        getLogWriter().warning("afterRemoteRegionCreate was expecting a profile in the event callback but there was none. " +
        		" This indicates a problem with the test hook DistributedRegion.TEST_HOOK_ADD_PROFILE");
      }
    }
    public void afterRemoteRegionDeparture(RegionEvent event) {
      this.lastOp = Op.Departure;
      this.lastEvent = event;
      getLogWriter().info(this.toString() + " received afterRemoteRegionDeparture notification for event " + event);
    }
    public void afterRemoteRegionCrash(RegionEvent event) {
      this.lastOp = Op.Crash;
      this.lastEvent = event;
      getLogWriter().info(this.toString() + " received afterRemoteRegionCrash notification for event " + event);
    }
  }
}
