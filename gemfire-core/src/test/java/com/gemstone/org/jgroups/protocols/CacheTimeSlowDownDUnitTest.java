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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DSClock;
import com.gemstone.gemfire.distributed.internal.DSClock.DSClockTestHook;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMember;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.protocols.GemFireTimeSync.GFTimeSyncHeader;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.ProtocolStack;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * @author shobhit
 *
 */
public class CacheTimeSlowDownDUnitTest extends DistributedTestCase {

  /**
   * 
   */
  private static final long serialVersionUID = 6040213401971040968L;

  /**
   * @param name
   */
  public CacheTimeSlowDownDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    if (!logPerTest) {
      // always disconnect from DS at the end of this test so it
      // doesn't leave a tampered one for the next test
      disconnectAllFromDS();
    }
  }

  public void testCacheClockSlowDownUsingTimeTask() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    disconnectAllFromDS();
    
    int port = getDUnitLocatorPort();
    
    // Check the new member's clock offset received from locator (co-ordinator)
    final long joinTimeOffset = (Long) vm0.invoke(new SerializableCallable("Check join time offset") {
      
      @Override
      public Object call() throws CacheException {
        InternalDistributedSystem system = getSystem();
        
        // Just to make sure the cache time is started and accepts dm timer task.
        Cache cache = CacheFactory.create(system);
        
        DistributionManager dm = (DistributionManager) system.getDistributionManager();
        DSClock clock = dm.getSystem().getClock();
        clock.setTestHook(new DistManagerTestHook());
        return clock.getCacheTimeOffset();
      }
    });
    
    // Send a custom GemfireTimeSync Message to reduce the offset and in turn slow down the cache time.
    VM locator = Host.getLocator();
    
    locator.invoke(new CacheSerializableRunnable("Send custom time sync message") {

      @Override
      public void run2() {

        InternalDistributedSystem system = (InternalDistributedSystem) InternalDistributedSystem.getAnyInstance();
        JGroupMembershipManager jgmm = MembershipManagerHelper.getMembershipManager(system);
        JChannel jchannel = MembershipManagerHelper.getJChannel(system);

        NetMember locMem = system.getDistributedMember().getNetMember();
        // Check if protocol statck has GemfireTimeSync protocol in it in
        // correct position.
        if (jchannel != null && jchannel.isConnected()) {
          ProtocolStack pstack = jchannel.getProtocolStack();
          GemFireTimeSync gts = (GemFireTimeSync) pstack.findProtocol("GemFireTimeSync");

          NetView nw = jgmm.getView();
          IpAddress memAdd = null;
          for (Object member : nw) {
            InternalDistributedMember iMem = (InternalDistributedMember) member;
            NetMember netMem = iMem.getNetMember();
            if (!netMem.equals(locMem)) {
              memAdd = ((JGroupMember)netMem).getAddress();
            }
          }

          Message offsetMessage = new Message();
          offsetMessage.setDest(memAdd);
          offsetMessage.isHighPriority = true;
          offsetMessage.putHeader("GemFireTimeSync", new GFTimeSyncHeader(0,
          /* GFTimeSyncHeader.OP_TIME_OFFSET */(byte) 2, (joinTimeOffset - 19)));
          gts.passDown(new Event(Event.MSG, offsetMessage));
          getLogWriter().info("Sent a GemfireTimeSync message to apply lower offset: " + (joinTimeOffset - 19) + "ms");
        }
      }
    });
    
    // Check if member received the message and started a timer task or not.
    long newTimeOffset = (Long) vm0.invoke(new SerializableCallable("Verify timer task and final offset") {
      
      @Override
      public Object call() throws CacheException {
        InternalDistributedSystem system = getSystem();
        DSClock clock = system.getClock();
        DSClockTestHook testHook = clock.getTestHook();
        assertNotNull(testHook);
        while(testHook.getInformation("TimerTaskCancelled") == null) {
          pause(100);
        }
        boolean isCancelled = (Boolean) testHook.getInformation("TimerTaskCancelled");
        List<Long> cacheTimes = (List) testHook.getInformation("CacheTime");
        List<Long> awaitedTimes = (List) testHook.getInformation("AwaitedTime");
        
        assertTrue(isCancelled);
        assertEquals(cacheTimes.size(), awaitedTimes.size());
        
        for (int i=0; i<cacheTimes.size()-1; i++) { //Last one wont satisfy condition.
          assertTrue((cacheTimes.get(i) - awaitedTimes.get(i)) >= 0);
        }
        
        clock.setTestHook(null);
        return clock.getCacheTimeOffset();
      }
    });
    
    assertEquals((joinTimeOffset - 19), newTimeOffset);
    
  }

  public class DistManagerTestHook implements DSClockTestHook {
    Map info = new HashMap();

    @Override
    public void suspendAtBreakPoint(int breakPoint) {
      switch (breakPoint) {
      case 1:
        if (info.get("CacheTime") == null) {
          info.put("CacheTime", new ArrayList());
        }
        if (info.get("AwaitedTime") == null) {
          info.put("AwaitedTime", new ArrayList());
        }
        break;
      case 2:        
        break;
      case 3:        
        break;
      default:
      }
    }

    @Override
    public void addInformation(Object key, Object value) {
      String sKey = (String)key;

      if ("CacheTime".equals(key)) {
        List cacheTimes = (List)info.get(key);
        cacheTimes.add(value);
      } else if ("AwaitedTime".equals(key)) {
        List awaitedTimes = (List)info.get(key);
        awaitedTimes.add(value);
      } else {
        info.put(key, value);
      }
    }

    @Override
    public Object getInformation(Object key) {
      return info.get(key);
    }
  }
}
