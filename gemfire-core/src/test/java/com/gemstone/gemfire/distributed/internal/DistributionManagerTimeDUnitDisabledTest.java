/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.distributed.internal;

import java.util.Map;

import org.junit.Ignore;

import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync.GFTimeSyncHeader;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync.TestHook;
import com.gemstone.org.jgroups.stack.Protocol;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * The dunit test is testing time offset set at
 * {@link DistributionManager#cacheTimeDelta}
 * @author shobhit
 *
 */
@Ignore("Disabled for bug 52348")
public class DistributionManagerTimeDUnitDisabledTest extends DistributedTestCase {

  public final int SKEDNESS = 10;
  
  /**
   * @param name
   */
  public DistributionManagerTimeDUnitDisabledTest(String name) {
    super(name);
  }

  public void testDistributionManagerTimeSync() {
    disconnectAllFromDS();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    // Start distributed system in all VMs.
    
    long vmtime0 = (Long) vm0.invoke(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        InternalDistributedSystem system = getSystem();
        long timeOffset = system.getClock().getCacheTimeOffset();
        return timeOffset;
      }
    });
    
    long vmtime1 = (Long) vm1.invoke(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        
        InternalDistributedSystem system = getSystem();
        long timeOffset = system.getClock().getCacheTimeOffset();
        return timeOffset;
      }
    });
    
    long vmtime2 = (Long) vm2.invoke(new SerializableCallable() {
      
      @Override
      public Object call() throws Exception {
        
        InternalDistributedSystem system = getSystem();
        long timeOffset = system.getClock().getCacheTimeOffset();
        return timeOffset;
      }
    });

    getLogWriter().info("Offsets for VM0: " + vmtime0 + " VM1: " + vmtime1 + " and VM2: " +vmtime2);

    // verify if they are skewed by more than 1 milli second.
    int diff1 = (int) (vmtime0 - vmtime1);
    int diff2 = (int) (vmtime1 - vmtime2);
    int diff3 = (int) (vmtime2 - vmtime0);
    
    if ((diff1 > SKEDNESS || diff1 < -SKEDNESS) || (diff2 > SKEDNESS || diff2 < -SKEDNESS) || (diff3 > SKEDNESS || diff3 < -SKEDNESS)) {
      fail("Clocks are skewed by more than " + SKEDNESS + " ms");
    }
  }

  public void testDistributionManagerTimeSyncAfterJoinDone() {
    disconnectAllFromDS();
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    // Start distributed system in all VMs.
    
    vm0.invoke(new CacheSerializableRunnable("Starting vm0") {
      @Override
      public void run2() {
        getSystem();
      }
    });
    
    vm1.invoke(new CacheSerializableRunnable("Starting vm1") {
      @Override
      public void run2() {
        getSystem();
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Starting vm2") {
      @Override
      public void run2() {
        getSystem();
      }
    });
    
    long vmtime0 = (Long) getTimeOffset(vm0);    
    long vmtime1 = (Long) getTimeOffset(vm1);    
    long vmtime2 = (Long) getTimeOffset(vm2);
    
    getLogWriter().info("Offsets for VM0: " + vmtime0 + " VM1: " + vmtime1 + " and VM2: " +vmtime2);

    // verify if they are skewed by more than 1 milli second.
    int diff1 = (int) (vmtime0 - vmtime1);
    int diff2 = (int) (vmtime1 - vmtime2);
    int diff3 = (int) (vmtime2 - vmtime0);
    
    if ((diff1 > SKEDNESS || diff1 < -SKEDNESS) || (diff2 > SKEDNESS || diff2 < -SKEDNESS) || (diff3 > SKEDNESS || diff3 < -SKEDNESS)) {
      fail("Clocks are skewed by more than " + SKEDNESS + " ms");
    }
  }

  public Object getTimeOffset(VM vm) {
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
        
        return timeOffset;
      }
    });
  }

  public class UnitTestHook implements TestHook {

    private int barrier = -1;

    @Override
    public void hook(int barr) {
      this.barrier = barr;
    }

    @Override
    public void setResponses(Map<Address, GFTimeSyncHeader> responses,
        long currentTime) {
    }

    public Map<Address, GFTimeSyncHeader> getResponses() {
      return null;
    }

    public long getCurTime() {
      return 0;
    }

    public int getBarrier() {
      return barrier;
    }
  }
}
