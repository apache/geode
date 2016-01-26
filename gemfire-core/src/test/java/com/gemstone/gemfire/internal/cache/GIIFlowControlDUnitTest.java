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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.ImageReplyMessage;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
public class GIIFlowControlDUnitTest extends CacheTestCase {
  
  protected static final String REGION_NAME = "region";
  private static final long MAX_WAIT = 10 * 1000;
  private static int origChunkSize = InitialImageOperation.CHUNK_SIZE_IN_BYTES;
  private static int origNumChunks = InitialImageOperation.CHUNK_PERMITS;
  protected static FlowControlObserver observer;
  /**
   * @param name
   */
  public GIIFlowControlDUnitTest(String name) {
    super(name);
  }

  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableRunnable("reset chunk size") {
      public void run() {
        InitialImageOperation.CHUNK_SIZE_IN_BYTES = origChunkSize;
        InitialImageOperation.CHUNK_PERMITS = origNumChunks;
      }
    });
    super.tearDown2();
  }
  
  public void testLotsOfChunks() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    invokeInEveryVM(new SerializableRunnable("reset chunk size") {
      public void run() {
        InitialImageOperation.CHUNK_SIZE_IN_BYTES = 10;
        InitialImageOperation.CHUNK_PERMITS = 2;
      }
    });
    
    createRegion(vm0);
    
    createData(vm0, 0, 50, "1234567890");
    
    createRegion(vm1);
   
    closeCache(vm0);
    
  }
  
  public void testFlowControlHappening() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    invokeInEveryVM(new SerializableRunnable("set chunk size") {
      public void run() {
        InitialImageOperation.CHUNK_SIZE_IN_BYTES = 10;
        InitialImageOperation.CHUNK_PERMITS = 2;
      }
    });
    
    vm1.invoke(new SerializableRunnable("Add flow control observer") {
      
      public void run() {
        observer = new FlowControlObserver();
        DistributionMessageObserver.setInstance(observer);
        getCache();
        observer.start();
        
      }
    });
    createRegion(vm0);
    
    createData(vm0, 0, 50, "1234567890");
    
    AsyncInvocation async1 = createRegionAsync(vm1);
    
    async1.join(100);
    assertTrue(async1.isAlive());
    
    vm1.invoke(new SerializableRunnable("Wait for chunks") {
      
      public void run() {
        waitForCriterion(new WaitCriterion(){

          public String description() {
            return "Waiting for messages to be at least 2: " + observer.messageCount.get();
          }

          public boolean done() {
            return observer.messageCount.get() >= 2;
          }
          
        }, MAX_WAIT, 100, true);
        
        //Make sure no more messages show up
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
        assertEquals(2, observer.messageCount.get());
        observer.allowMessages.countDown();
      }
    });
    
   
    
    async1.getResult(MAX_WAIT);
    
    vm1.invoke(new SerializableRunnable("Add flow control observer") {
      
      public void run() {
        assertTrue("Message count should be greater than 2 now", observer.messageCount.get() > 2);
      }
    });
    closeCache(vm0);
  }
  
  public void testKillSenderNoHang() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    invokeInEveryVM(new SerializableRunnable("set chunk size") {
      public void run() {
        InitialImageOperation.CHUNK_SIZE_IN_BYTES = 10;
        InitialImageOperation.CHUNK_PERMITS = 2;
      }
    });
    
    vm1.invoke(new SerializableRunnable("Add flow control observer") {
      
      public void run() {
        observer = new FlowControlObserver();
        DistributionMessageObserver.setInstance(observer);
        getCache();
        observer.start();
        
      }
    });
    
    createRegion(vm0);

    createData(vm0, 0, 50, "1234567890");

    AsyncInvocation async1 = createRegionAsync(vm1);

    vm1.invoke(new SerializableRunnable("Wait to flow control messages") {

      public void run() {
        waitForCriterion(new WaitCriterion(){

          public String description() {
            return "Waiting for messages to be at least 2: " + observer.messageCount.get();
          }

          public boolean done() {
            return observer.messageCount.get() >= 2;
          }

        }, MAX_WAIT, 100, true);

        //Make sure no more messages show up
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
        assertEquals(2, observer.messageCount.get());
      }
    });

    closeCache(vm0);

    vm1.invoke(new SerializableRunnable("release flow control") {

      public void run() {
        observer.allowMessages.countDown();
      }
    });

    //We should now finish.
    async1.getResult(MAX_WAIT);

  }
  
  // DISABLED due to high failure rate due, apparently, to problems
  // with the flow-control statistics.  See internal ticket #52221
  public void disabledtestCloseReceiverCacheNoHang() throws Throwable {
    doCloseTest(false);
  }
  
  // DISABLED due to high failure rate due, apparently, to problems
  // with the flow-control statistics.  See internal ticket #52221
  public void disabledtestDisconnectReceiverNoHang() throws Throwable {
    doCloseTest(true);
  }
  
  public void doCloseTest(boolean disconnect) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    invokeInEveryVM(new SerializableRunnable("set chunk size") {
      public void run() {
        InitialImageOperation.CHUNK_SIZE_IN_BYTES = 10;
        InitialImageOperation.CHUNK_PERMITS = 2;
      }
    });
    
    InitialImageOperation.CHUNK_SIZE_IN_BYTES = 10;
    InitialImageOperation.CHUNK_PERMITS = 2;
    
    vm1.invoke(new SerializableRunnable("Add flow control observer") {
      
      public void run() {
        observer = new FlowControlObserver();
        DistributionMessageObserver.setInstance(observer);
        getCache();
        observer.start();
        
      }
    });
    ExpectedException expectedEx = null;
    try {
      createRegion(vm0);

      createData(vm0, 0, 50, "1234567890");

      createRegionAsync(vm1);

      vm1.invoke(new SerializableRunnable("Wait to flow control messages") {

        public void run() {
          waitForCriterion(new WaitCriterion(){

            public String description() {
              return "Waiting for messages to be at least 2: " + observer.messageCount.get();
            }

            public boolean done() {
              return observer.messageCount.get() >= 2;
            }

          }, MAX_WAIT, 100, true);

          //Make sure no more messages show up
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            fail("interrupted", e);
          }
          assertEquals(2, observer.messageCount.get());
        }
      });

      try {
        vm0.invoke(new SerializableRunnable("check for in progress messages") {
          public void run() {
            final DMStats stats = getSystem().getDMStats();
            assertEquals(2, stats.getInitialImageMessagesInFlight());
          }
        });
      } catch (Exception e) {
        vm1.invoke(new SerializableRunnable("release flow control due to exception") {
          public void run() {
            observer.allowMessages.countDown();
          }
        });
        throw e;
      }

      expectedEx = addExpectedException(InterruptedException.class.getName(),
          vm1);
      if(disconnect) {
        disconnect(vm1);
      } else {
        closeCache(vm1);
      }
      
      vm1.invoke(new SerializableRunnable("release flow control") {

        public void run() {
          observer.allowMessages.countDown();
        }
      });

      vm0.invoke(new SerializableRunnable("check for in progress messages") {

        public void run() {
          final DMStats stats = getSystem().getDMStats();
          waitForCriterion(new WaitCriterion() {
            
            public boolean done() {
              return stats.getInitialImageMessagesInFlight() == 0;
            }
            
            public String description() {
              return "Timeout waiting for all initial image messages to be processed: "
                  + stats.getInitialImageMessagesInFlight();
            }
          }, MAX_WAIT, 100, true);
        }
      });


    } finally {
      if (expectedEx != null) {
        expectedEx.remove();
      }
    }
  }

  //TODO Test destroying either side of the DS during the flow control, no hangs.
  
  protected void closeCache(final VM vm) {
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };
    vm.invoke(closeCache);
  }
  
  protected void disconnect(final VM vm) {
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      public void run() {
        disconnectFromDS();
      }
    };
    vm.invoke(closeCache);
  }
  
  private void createRegion(VM vm) throws Throwable {
    SerializableRunnable createRegion = getCreateRegionRunnable();
    vm.invoke(createRegion);
  }

  private SerializableRunnable getCreateRegionRunnable() {
    SerializableRunnable createRegion = new SerializableRunnable("Create non persistent region") {
      public void run() {
        getCache();
        RegionFactory rf = new RegionFactory();
        rf.setDataPolicy(DataPolicy.REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    return createRegion;
  }
  
  private AsyncInvocation createRegionAsync(VM vm) throws Throwable {
    SerializableRunnable createRegion = getCreateRegionRunnable();
    return vm.invokeAsync(createRegion);
  }
  
  protected void createData(VM vm, final int startKey, final int endKey,
      final Object value) {
    SerializableRunnable createData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        
        for(int i =startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }
  
  protected void checkData(VM vm0, final int startKey, final int endKey,
      final String value) {
    SerializableRunnable checkData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        
        for(int i =startKey; i < endKey; i++) {
          assertEquals("On key " + i, value, region.get(i));
        }
      }
    };
    
    vm0.invoke(checkData);
  }

  private static class FlowControlObserver extends DistributionMessageObserver {
    CountDownLatch allowMessages = new CountDownLatch(1);
    AtomicInteger messageCount = new AtomicInteger();
    private volatile boolean started = false;

    @Override
    public void beforeProcessMessage(DistributionManager dm,
        DistributionMessage message) {
      if(started && message instanceof ImageReplyMessage) {
        messageCount.incrementAndGet();
        try {
          allowMessages.await();
        } catch (InterruptedException e) {
          fail("Interrupted", e);
        }
      }
    }

    public void start() {
      started = true;
    }
    
    
  }
}
