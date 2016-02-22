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
package com.gemstone.gemfire.cache.query.partitioned;

import java.io.File;
import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * @author rdubey
 * 
 */
public class PRBasicIndexCreationDeadlockDUnitTest extends
    PartitionedRegionDUnitTestCase

{
  /**
   * constructor
   * 
   * @param name
   */

  public PRBasicIndexCreationDeadlockDUnitTest(String name) {
    super(name);
  }

  // int totalNumBuckets = 131;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  final String name = "PartionedPortfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 1003;

  final int redundancy = 0;

  public static volatile boolean hook_vm1, hook_vm2;

  //Dummy test method to be removed when test is fixed
  public void testIndexCreationMessageDiskRecoveryDeadLock() {
  }

  public void DISABLE_testIndexCreationMessageDiskRecoveryDeadLock() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    Class valueConstraint = Portfolio.class;
    final String fileName1 = "PRPersistentIndexCreation_1.xml";
    final String fileName2 = "PRPersistentIndexCreation_2.xml";
    
    final File dir1 = new File("overflowData1");
    final File dir2 = new File("overflowData2");

    AsyncInvocation[] asyns = new AsyncInvocation[2];
    
    try {
      vm0.invoke(new CacheSerializableRunnable("Create disk store directories") {
        
        
        @Override
        public void run2() throws CacheException {
          boolean success = (dir1).mkdir();
          success = (dir2).mkdir();
        }
      });
   
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name, fileName1));
      vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name, fileName2));
  
      final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(100);
  
      // Putting the data into the PR's created
      vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                               0, 100));
  
      vm0.invoke(new CacheSerializableRunnable("Close VM0 cache") {
        
        @Override
        public void run2() throws CacheException {
          GemFireCacheImpl.getInstance().close();
        }
      });
    
      vm1.invoke(new CacheSerializableRunnable("Close VM1 cache") {
        
        @Override
        public void run2() throws CacheException {
          GemFireCacheImpl.getInstance().close();
        }
      });
  
      // Restart the caches with testHook.
      asyns[0] = vm0.invokeAsync(new CacheSerializableRunnable("Restart VM0 cache") {
        
        @Override
        public void run2() throws CacheException {
          GemFireCacheImpl.testCacheXml = PRQHelp.findFile(fileName1);
          IndexUtils.testHook = new IndexUtilTestHook();
          PRQHelp.getCache();
        }
      });
  
      //asyns[1] = 
        vm0.invoke(new CacheSerializableRunnable("Checking hook in VM0 cache") {
        
        @Override
        public void run2() throws CacheException {
          IndexUtilTestHook hook = (IndexUtilTestHook) IndexUtils.testHook;
          while (hook == null) {
            hook = (IndexUtilTestHook) IndexUtils.testHook;
            Wait.pause(20);
          }
          while (!hook.isHooked()) {
            Wait.pause(30);
          }
          //hook.setHooked(false);
          hook_vm1 = true;
          /*while (!hook_vm2) {
            pause(40);
          }
          hook.setHooked(false);*/
        }
      });
  
      asyns[1] = vm1.invokeAsync(new CacheSerializableRunnable("Restart VM1 cache") {
        
        @Override
        public void run2() throws CacheException {
          GemFireCacheImpl.testCacheXml = PRQHelp.findFile(fileName2);
          PRQHelp.getCache();
        }
      });

      Wait.pause(2000);
      
      vm0.invoke(new CacheSerializableRunnable("Checking hook in VM0 cache again") {
        
        @Override
        public void run2() throws CacheException {
          IndexUtilTestHook hook = (IndexUtilTestHook) IndexUtils.testHook;
          while (hook == null) {
            hook = (IndexUtilTestHook) IndexUtils.testHook;
            Wait.pause(20);
          }
          while (!hook.isHooked()) {
            Wait.pause(30);
          }
          hook.setHooked(false);
          hook_vm1 = false;
        }
      });  

      for (AsyncInvocation async: asyns) {
        ThreadUtils.join(async, 10000);
      }
    } finally {
      
      vm0.invoke(new CacheSerializableRunnable("Close VM0 cache") {
        
        @Override
        public void run2() throws CacheException {
          dir1.delete();
          dir2.delete();
          IndexUtilTestHook hook = (IndexUtilTestHook) IndexUtils.testHook;
          if (hook != null) {
            hook.setHooked(true);
            IndexUtils.testHook = null;
          }
        }
      });
    }
  }

  public class IndexUtilTestHook implements TestHook {

    private volatile boolean hooked = false;
    
    public void setHooked(boolean hooked) {
      this.hooked = hooked;
    }

    public boolean isHooked() {
      return hooked;
    }
    
    @Override
    public synchronized void hook(int spot) throws RuntimeException {
      GemFireCacheImpl.getInstance().getLogger().fine("IndexUtilTestHook is set");
      switch (spot) {
      case 0:
        hooked = true;
        while(hooked) {Wait.pause(300);}
        break;

      default:
        break;
      }
    }
  }
}
