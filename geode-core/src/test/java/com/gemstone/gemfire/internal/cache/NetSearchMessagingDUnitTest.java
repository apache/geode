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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.cache.SearchLoadAndWriteProcessor.NetSearchRequestMessage;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * @author dsmith
 *
 */
public class NetSearchMessagingDUnitTest extends CacheTestCase {

  /**
   * @param name
   */
  public NetSearchMessagingDUnitTest(String name) {
    super(name);
  }
  
  public void testOneMessageWithReplicates() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    createReplicate(vm0);
    createReplicate(vm1);
    createNormal(vm2);
    createEmpty(vm3);
    
    //Test with a null value
    {
      long vm0Count = getReceivedMessages(vm0);
      long vm1Count = getReceivedMessages(vm1);
      long vm2Count = getReceivedMessages(vm2);
      long vm3Count = getReceivedMessages(vm3);

      assertEquals(null, get(vm3, "a"));

      //Make sure we only processed one message
      assertEquals(vm3Count + 1, getReceivedMessages(vm3));

      //Make sure the replicates only saw one message between them

      assertEquals(vm0Count + vm1Count + 1, getReceivedMessages(vm0) + getReceivedMessages(vm1));

      //Make sure the normal guy didn't see any messages
      assertEquals(vm2Count, getReceivedMessages(vm2));
    }
    
    //Test with a real value value
    {
      
      put(vm3, "a", "b");
      
      long vm0Count = getReceivedMessages(vm0);
      long vm1Count = getReceivedMessages(vm1);
      long vm2Count = getReceivedMessages(vm2);
      long vm3Count = getReceivedMessages(vm3);

      assertEquals("b", get(vm3, "a"));

      //Make sure we only processed one message
      assertEquals(vm3Count + 1, getReceivedMessages(vm3));

      //Make sure the replicates only saw one message between them

      assertEquals(vm0Count + vm1Count + 1, getReceivedMessages(vm0) + getReceivedMessages(vm1));

      //Make sure the normal guy didn't see any messages
      assertEquals(vm2Count, getReceivedMessages(vm2));
    }
    
  }
  
  public void testNetSearchNormals() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    
    createNormal(vm0);
    createNormal(vm1);
    createNormal(vm2);
    createEmpty(vm3);
    
    //Test with a null value
    {
      long vm0Count = getReceivedMessages(vm0);
      long vm1Count = getReceivedMessages(vm1);
      long vm2Count = getReceivedMessages(vm2);
      long vm3Count = getReceivedMessages(vm3);

      assertEquals(null, get(vm3, "a"));

      //Make sure we only processed one message
      waitForReceivedMessages(vm3, vm3Count + 3);

      //Make sure the normal guys each saw 1 query message.
      assertEquals(vm0Count + vm1Count + vm2Count + 3, getReceivedMessages(vm0) + getReceivedMessages(vm1) + getReceivedMessages(vm2));
    }
    
    //Test with a real value value
    {
      
      put(vm3, "a", "b");
      
      long vm0Count = getReceivedMessages(vm0);
      long vm1Count = getReceivedMessages(vm1);
      long vm2Count = getReceivedMessages(vm2);
      final long vm3Count = getReceivedMessages(vm3);

      assertEquals("b", get(vm3, "a"));

      //Make sure we only processed one message
      waitForReceivedMessages(vm2, vm2Count + 1);

      waitForReceivedMessages(vm3, vm3Count + 3);

      //Make sure the normal guys each saw 1 query message.
      assertEquals(vm0Count + vm1Count + vm2Count + 3, getReceivedMessages(vm0) + getReceivedMessages(vm1) + getReceivedMessages(vm2));
    }
    
  }
  
  /**
   * In bug #48186 a deadlock occurs when a netsearch pulls in a value from
   * the disk and causes a LRU eviction of another entry.  Here we merely
   * demonstrate that a netsearch that gets the value of an overflow entry
   * does not update the LRU status of that entry.
   */
  public void testNetSearchNoLRU() {
    Host host = Host.getHost(0);
    VM vm2 = host.getVM(2);
    VM vm1 = host.getVM(1);
    
    createOverflow(vm2, 5);
    createEmpty(vm1);
    
    
    
    //Test with a null value
    {
      put(vm2, "a", "1");
      put(vm2, "b", "2");
      put(vm2, "c", "3");
      put(vm2, "d", "4");
      put(vm2, "e", "5");
      // the cache in vm0 is now full and LRU will occur on this next put()
      put(vm2, "f", "6");
      
      SerializableCallable verifyEvicted = new SerializableCallable("verify eviction of 'a'") {
        public Object call() {
          Cache cache = getCache();
          LocalRegion region = (LocalRegion)cache.getRegion("region");
          RegionEntry re = region.getRegionEntry("a");
          Object o = re.getValueInVM(null);
          LogWriterUtils.getLogWriter().info("key a="+o);;
          return o == null || o == Token.NOT_AVAILABLE;
        }
      };
      
      boolean evicted = (Boolean)vm2.invoke(verifyEvicted);
      assertTrue("expected 'a' to be evicted", evicted);
      
      // now netsearch for 'a' from the other VM and verify again
      Object value = get(vm1, "a");
      assertEquals("expected to find '1' result from netSearch",  "1", value);

      evicted = (Boolean)vm2.invoke(verifyEvicted);
      assertTrue("expected 'a' to still be evicted", evicted);
      vm2.invoke(new SerializableRunnable("verify other entries are not evicted") {
        public void run() {
          Cache cache = getCache();
          LocalRegion region = (LocalRegion)cache.getRegion("region");
          String[] keys = new String[]{"b", "c", "d", "e", "f"};
          for (String key: keys) {
            RegionEntry re = region.getRegionEntry(key);
            Object o = re.getValueInVM(null);
            LogWriterUtils.getLogWriter().info("key " + key + "=" + o);
            assertTrue("expected key " + key + " to not be evicted",
                (o != null) && (o != Token.NOT_AVAILABLE));
          }
        }
      });
    }
  }

  /**
   * Make sure that even if we start out by net searching replicates,
   * we'll fall back to net searching normal members.
   */
  public void testNetSearchFailoverFromReplicate() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    //Install a listener to kill this member
    //when we get the netsearch request
    vm0.invoke(new SerializableRunnable("Install listener") {
      
      public void run() {
        DistributionMessageObserver ob = new DistributionMessageObserver() {
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof NetSearchRequestMessage) {
              disconnectFromDS();
            }
          }
        };
        DistributionMessageObserver.setInstance(ob);
      }
    });
    
    createReplicate(vm0);
    createNormal(vm1);
    createNormal(vm2);
    createEmpty(vm3);
    
    //Test with a real value value
    {
      put(vm3, "a", "b");
      
      long vm0Count = getReceivedMessages(vm0);
      long vm1Count = getReceivedMessages(vm1);
      long vm2Count = getReceivedMessages(vm2);
      long vm3Count = getReceivedMessages(vm3);

      assertEquals("b", get(vm3, "a"));

      //Make sure we were disconnected in vm0
      vm0.invoke(new SerializableRunnable("check disconnected") {
        
        public void run() {
          assertNull(GemFireCacheImpl.getInstance());
        }
      });
    }
    
  }
  
  public void testNetSearchFailoverFromOneReplicateToAnother() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    //Install a listener to kill this member
    //when we get the netsearch request
    vm0.invoke(new SerializableRunnable("Install listener") {
      
      public void run() {
        DistributionMessageObserver ob = new DistributionMessageObserver() {
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof NetSearchRequestMessage) {
              disconnectFromDS();
            }
          }
        };
        DistributionMessageObserver.setInstance(ob);
      }
    });
    
    createReplicate(vm0);
    createReplicate(vm1);
    createEmpty(vm3);
    
    //Test with a real value value
    {
      put(vm3, "a", "b");
      
      boolean disconnected = false;
      while(!disconnected) {
        assertEquals("b", get(vm3, "a"));

        //Make sure we were disconnected in vm0
        disconnected = (Boolean) vm0.invoke(new SerializableCallable("check disconnected") {

          public Object call() {
            return GemFireCacheImpl.getInstance() == null;
          }
        });
      }
    }
    
  }

  private Object put(VM vm, final String key, final String value) {
    return vm.invoke(new SerializableCallable() {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        LogWriterUtils.getLogWriter().info("putting key="+key+"="+value);
        Object result = region.put(key, value);
        LogWriterUtils.getLogWriter().info("done putting key="+key);
        return result;
      }
    });
  }

  private Object get(VM vm, final Object key) {
    return vm.invoke(new SerializableCallable("get " + key) {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        return region.get(key);
      }
    });
  }
  
  private void waitForReceivedMessages(final VM vm, final long expected) {
    Wait.waitForCriterion(new WaitCriterion() {
      
      @Override
      public boolean done() {
        return getReceivedMessages(vm) == expected;
      }
      
      @Override
      public String description() {
        return "Expected " + expected + " but got " + getReceivedMessages(vm);
      }
    }, 2000, 100, true);
  }
  
  private long getReceivedMessages(VM vm) {
    return ((Long) vm.invoke(new SerializableCallable() {
      
      public Object call() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        return cache.getDistributedSystem().getDMStats().getReceivedMessages();
      }
    })).intValue();
  }

  private void createEmpty(VM vm) {
    vm.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        RegionFactory rf = new RegionFactory();
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setConcurrencyChecksEnabled(false);
        rf.setDataPolicy(DataPolicy.EMPTY);
        rf.create("region");
      }
    });
    
  }

  private void createNormal(VM vm) {
    vm.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        RegionFactory rf = new RegionFactory();
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setConcurrencyChecksEnabled(false);
        rf.setDataPolicy(DataPolicy.NORMAL);
        rf.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        rf.create("region");
      }
    });
    
  }

  private void createOverflow(VM vm, final int count) {
    vm.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        rf.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(count, EvictionAction.OVERFLOW_TO_DISK));
        rf.create("region");
      }
    });
    
  }

  private void createReplicate(VM vm) {
    vm.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        RegionFactory rf = new RegionFactory();
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setConcurrencyChecksEnabled(false);
        rf.setDataPolicy(DataPolicy.REPLICATE);
        rf.create("region");
      }
    });
    
  }

}
