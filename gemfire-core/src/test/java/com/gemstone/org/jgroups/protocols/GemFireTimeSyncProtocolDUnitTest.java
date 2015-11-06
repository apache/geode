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
package com.gemstone.org.jgroups.protocols;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DSClock;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync.GFTimeSyncHeader;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync.TestHook;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.ProtocolStack;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * This test mimics the offset time calculation done by {@link GemFireTimeSync}
 * protocol implementation and verifies it with offset times set in DMs of
 * respective GemFire nodes.
 * 
 * @author shobhit
 * 
 */
public class GemFireTimeSyncProtocolDUnitTest extends DistributedTestCase {
  
  /**
   * 
   */
  private static final long serialVersionUID = -8833917578265991129L;
  
  @Override
  public void setUp() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * @param name
   */
  public GemFireTimeSyncProtocolDUnitTest(String name) {
    super(name);
  }
  // disabled for ticket #52114
  public void disabledtestGemfireTimeSyncJgroupsProtocol() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    List<Object[]> vmJoinTimeOffsets = new ArrayList<Object[]>();
    
    // Start distributed system in all VMs. 
    Object[] vmjointime0 = (Object[]) getIDAndTimeOffset(vm0);    
    vmJoinTimeOffsets.add(vmjointime0);

    Object[] vmjointime1 = (Object[]) getIDAndTimeOffset(vm1);
    vmJoinTimeOffsets.add(vmjointime1);

    Object[] vmjointime2 = (Object[]) getIDAndTimeOffset(vm2);
    vmJoinTimeOffsets.add(vmjointime2);

    // Get locator VM as that is the coordinator.
    VM locator = Host.getLocator();
    
    Map offsets = (Map) locator.invoke(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        
        InternalDistributedSystem system = (InternalDistributedSystem) InternalDistributedSystem.getAnyInstance();
        DistributedMember coord = MembershipManagerHelper.getCoordinator(system);
        DistributedMember self  = system.getDistributedMember();
        Map offs = null;
        
        if (self.getId().equalsIgnoreCase(coord.getId())) {
          // Initiate a fake view change in GemfireTimeSync.
          JGroupMembershipManager jgmm = MembershipManagerHelper.getMembershipManager(system);
          JChannel jchannel = MembershipManagerHelper.getJChannel(system);

          final UnitTestHook gftsTestHook = new UnitTestHook();
          
          // Check if protocol stack has GemfireTimeSync protocol in it in correct position.
          if (jchannel != null && jchannel.isConnected()) {
            ProtocolStack pstack = jchannel.getProtocolStack();
            GemFireTimeSync gts = null;
            
            boolean found = false;
            Protocol prot = pstack.findProtocol("GemFireTimeSync");
            // Verify prcol position in stack for GemFireTimeSync.
            if (prot != null) {

              Protocol up_prot = prot.getUpProtocol();
              Protocol down_prot = prot.getDownProtocol();

              assertEquals("UP protocol of GemFireTimeSync protocol is: " + up_prot, "AUTH", up_prot.getName());
              assertEquals("Down protocol of GemFireTimeSync protocol is: " + down_prot, "FRAG3", down_prot.getName());
              found = true;

              // Trigger viewchange in timesync protocol
              gts = (GemFireTimeSync) prot;
              gts.setTestHook(gftsTestHook);
              getLogWriter().fine(
                  "Invoking sync-therad in GemFireTimeSync protocol");
              gts.invokeServiceThreadForTest();
            }

            if (!found) {
              fail("GemFireTimeSync protocol is not found in protocol stack");
            }

            // Let GemfireTimeSync protocol kick in after VIEW_CHANGE
            waitForCriterion(new WaitCriterion() {
              @Override
              public boolean done() {
                return gftsTestHook.getBarrier() == GemFireTimeSync.TIME_RESPONSES;
              }
              
              @Override
              public String description() {
                return "Waiting for all nodes to get time offsets from co-ordinator";
              }
            }, 500, 50, false);
  
            Map respons = gftsTestHook.getResponses();
            
            // unset test hook
            if (gts != null) gts.setTestHook(null);
  
            offs = calculateOffsets(respons, gftsTestHook.getCurTime());
          }
        }
        return offs;
      }
    });

    List<Object[]> vmtimeOffsets = new ArrayList<Object[]>();

    pause(5000);
    
    Object[] vmtime0 = (Object[]) getIDAndTimeOffset(vm0);
    Object[] vmtime1 = (Object[]) getIDAndTimeOffset(vm1);    
    Object[] vmtime2 = (Object[]) getIDAndTimeOffset(vm2);
    
    vmtimeOffsets.add(vmtime0);
    vmtimeOffsets.add(vmtime1);
    vmtimeOffsets.add(vmtime2);

    // verify if they are skewed by more than 1 milli second.
    for (int i=0; i<3; i++) {
      String address = (String) vmtimeOffsets.get(i)[0];
      long offsetTime = (Long) offsets.get(address);

      // Offsets after join must always be going forward, no backward offsets allowed after join.
      if (Math.abs(offsetTime - (Long)vmtimeOffsets.get(i)[1]) > 1
          && Math.abs(((Long)vmtimeOffsets.get(i)[1]).longValue() - ((Long)vmJoinTimeOffsets.get(i)[1]).longValue()) > 1) {        
        fail("Offset calculated locally: " + offsetTime
            + " not equals to returned by GemFireTimeSync protocol: "
            + vmtimeOffsets.get(i)[1] + " and join time is: " + (Long)vmJoinTimeOffsets.get(i)[1] +" for vm " + i + " address="+ vmtimeOffsets.get(i)[0]);
      }
    }
  }

  /**
   * Tests the slowing down the cache time for a locator, or any kind of
   * GemFire cache for that matter.
   */
  public void testGemfireTimeSyncSlowDownCacheTime() {
    Host host = Host.getHost(0);
    VM newLocator = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    List<Object[]> vmJoinTimeOffsets = new ArrayList<Object[]>();
    
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String host0 = getServerHostName(host); 
    
    final Properties props = new Properties();
    props.setProperty("locators", host0 + "[" + locatorPort + "]");
    props.setProperty("mcast-port", "0");
    props.setProperty("jmx-manager", "false");
    //props.setProperty("enable-network-partition-detection", "true");
//    props.setProperty("log-level", "fine");
    props.put("member-timeout", "2000");
    
    try {
      // Start new locator in vm0
      newLocator.invoke(new CacheSerializableRunnable("Start locator") {
        
        @Override
        public void run2() throws CacheException {
          // Disconnect from any existing DS.
          try {
            system.disconnect();
          } catch (Exception ex) {
            // Let it go.
          }
          File myLocatorLogFile = new File("locator-"+locatorPort+".log"); 
  
          try {
            Locator.startLocatorAndDS(locatorPort, myLocatorLogFile, props);
          } catch (IOException e) {
            fail("New locator startup failed on port: "+locatorPort, e);
          }
        }
      });
      
      // Add a new member to trigger VIEW_CHANGE.
      vm1.invoke(new CacheSerializableRunnable("Starting vm1") {
        @Override
        public void run2() {
          // Disconnect from any existing DS.
          disconnectFromDS();
          
          DistributedSystem.connect(props);
        }
      });

      AsyncInvocation slowDownCacheTimeTask = newLocator.invokeAsync(new CacheSerializableRunnable("Slow down locator cache time") {
        
        @Override
        public void run2() throws CacheException {
  
          InternalDistributedSystem system = (InternalDistributedSystem) InternalDistributedSystem
              .getAnyInstance();
          final DSClock clock = system.getClock();
  
          // Set lower time offset
          long currOffset = clock.getCacheTimeOffset();
          final long newOffset = currOffset - 10 /* ms */;
  
          clock.setCacheTimeOffset(null, newOffset, false);
          // Let GemfireTimeSync protocol kick in after VIEW_CHANGE
          waitForCriterion(new WaitCriterion() {
            @Override
            public boolean done() {
              return clock.getCacheTimeOffset() == newOffset;
            }
  
            @Override
            public String description() {
              return "Waiting for locator cache time to slowdown";
            }
          }, 30, 2, true);
        }
      });
      
      DistributedTestCase.join(slowDownCacheTimeTask, 100, getLogWriter());
      if (slowDownCacheTimeTask.exceptionOccurred()) {
        fail("Slow down locator cache time task failed with exception", slowDownCacheTimeTask.getException());
      }
    } finally {
      // Shutdown locator and clean vm0 for other tests.
      newLocator.invoke(new CacheSerializableRunnable("Shutdown locator") {
        
        @Override
        public void run2() throws CacheException {
          try {
            InternalDistributedSystem.getConnectedInstance().disconnect();
          } catch (Exception e) {
            fail("Stoping locator failed", e);
          }
        }
      });

      vm1.invoke(new CacheSerializableRunnable("Shutdown vm1") {
        
        @Override
        public void run2() throws CacheException {
          try {
            InternalDistributedSystem.getConnectedInstance().disconnect();
          } catch (Exception e) {
            fail("Stoping vm1 failed", e);
          }
        }
      });
    }
  }

  public class UnitTestHook implements TestHook {

    private Map<Address, GFTimeSyncHeader> respons;
    private long curTime;

    private int barrier = -1;

    @Override
    public void hook(int barr) {
      this.barrier = barr;
      if (barrier == GemFireTimeSync.TIME_RESPONSES) {
        pause(200);
      }
    }

    @Override
    public void setResponses(Map<Address, GFTimeSyncHeader> responses,
        long currentTime) {
      this.respons = responses;
      this.curTime = currentTime;
    }

    public Map<Address, GFTimeSyncHeader> getResponses() {
      return respons;
    }

    public long getCurTime() {
      return curTime;
    }

    public int getBarrier() {
      return barrier;
    }
  }

  public Object getIDAndTimeOffset(VM vm) {
    return vm.invoke(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        InternalDistributedSystem system = getSystem();
        JChannel jchannel = MembershipManagerHelper.getJChannel(system);

        final UnitTestHook gftsTestHook = new UnitTestHook();
        Protocol prot = jchannel.getProtocolStack().findProtocol("GemFireTimeSync");
        GemFireTimeSync gts = (GemFireTimeSync)prot;
        gts.setTestHook(gftsTestHook);
        //Let the syncMessages reach to all VMs for new offsets.
        waitForCriterion(new WaitCriterion() {
          
          @Override
          public boolean done() {
            return gftsTestHook.getBarrier() == GemFireTimeSync.OFFSET_RESPONSE;
          }
          
          @Override
          public String description() {
            return "Waiting for this node to get time offsets from co-ordinator";
          }
        }, 500, 50, false);
        
        
        long timeOffset = system.getClock().getCacheTimeOffset();
        gts.setTestHook(null);
        
        
        prot = jchannel.getProtocolStack().findProtocol("GMS");
        GMS gms = (GMS)prot;
        String address = gms.getLocalAddress();
        return new Object[]{address, timeOffset};
      }
    });
  }
  /*
   * All Berkley time service calculation related helper methods.
   * Same is done in GemFireTimeSync protocol.
   * 
   */

  private Map calculateOffsets(Map responses, long currentTime) {
    Map offsets = new HashMap();
    
    if (responses.size() > 1) {

      // now compute the average round-trip time and the average clock time,
      // throwing out values outside of the standard deviation for each
      
      long averageRTT = getMeanRTT(responses, 0, Long.MAX_VALUE);
      long rTTStddev = getRTTStdDev(responses, averageRTT);
      // now recompute the average throwing out ones that are way off
      long newAverageRTT = getMeanRTT(responses, averageRTT, rTTStddev);
      if (newAverageRTT > 0) {
        averageRTT = newAverageRTT;
      }
      
      long averageTime = getMeanClock(responses, 0, Long.MAX_VALUE);
      long stddev = getClockStdDev(responses, averageTime);
      long newAverageTime = getMeanClock(responses, averageTime, stddev);
      if (newAverageTime > 0) {
        averageTime = newAverageTime;
      }
      
      long averageTransmitTime = averageRTT / 2;
      long adjustedAverageTime = averageTime + averageTransmitTime;
      
      // TODO: should all members on the same machine get the same time offset?
      
      for (Iterator<Map.Entry<Address, GFTimeSyncHeader>> it = responses.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<Address, GFTimeSyncHeader> entry = it.next();
        IpAddress mbr = (IpAddress)entry.getKey();
        GFTimeSyncHeader response = entry.getValue();
        long responseTransmitTime = (response.timeReceived - currentTime) / 2;
        long offset = adjustedAverageTime - (response.time + responseTransmitTime);
        offsets.put(mbr.toString(), offset);
      }
    }
    return offsets;
  }

  private long getMeanRTT(Map<Address, GFTimeSyncHeader> values, long previousMean, long stddev) {
    long totalTime = 0;
    long numSamples = 0;
    long upperLimit = previousMean + stddev;
    for (GFTimeSyncHeader response: values.values()) {
      long rtt = response.timeReceived - response.time;
      if (rtt <= upperLimit) {
        numSamples++;
        totalTime += rtt;
      }
    }
    long averageTime = totalTime / numSamples;
    return averageTime;
  }
  
  private long getRTTStdDev(Map<Address, GFTimeSyncHeader> values, long average) {
    long sqDiffs = 0;
    for (GFTimeSyncHeader response: values.values()) {
      long diff = average - (response.timeReceived - response.time);
      sqDiffs += diff * diff;
    }
    return Math.round(Math.sqrt((double)sqDiffs));
  }

  /**
   * retrieves the average of the samples.  This can be used with (samples, 0, Long.MAX_VALUE) to get
   * the initial mean and then (samples, lastResult, stddev) to get those within the standard deviation.
   * @param values
   * @param previousMean
   * @param stddev
   * @return the mean
   */
  private long getMeanClock(Map<Address, GFTimeSyncHeader> values, long previousMean, long stddev) {
    long totalTime = 0;
    long numSamples = 0;
    long upperLimit = previousMean + stddev;
    long lowerLimit = previousMean - stddev;
    for (GFTimeSyncHeader response: values.values()) {
      if (lowerLimit <= response.time && response.time <= upperLimit) {
        numSamples++;
        totalTime += response.time;
      }
    }
    long averageTime = totalTime / numSamples;
    return averageTime;
  }
  
  private long getClockStdDev(Map<Address, GFTimeSyncHeader> values, long average) {
    long sqDiffs = 0;
    for (GFTimeSyncHeader response: values.values()) {
      long diff = average - response.time;
      sqDiffs += diff * diff;
    }
    return Math.round(Math.sqrt((double)sqDiffs));
  }
}
