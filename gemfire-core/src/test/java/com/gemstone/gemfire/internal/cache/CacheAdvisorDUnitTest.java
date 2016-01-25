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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the use of CacheDistributionAdvisor in createSubRegion
 *
 * @author Eric Zoerner
 *
 */
public class CacheAdvisorDUnitTest extends CacheTestCase {
  private transient VM[] vms;
  private transient InternalDistributedMember[] ids;
  
  /** Creates a new instance of CacheAdvisorDUnitTest */
  public CacheAdvisorDUnitTest(String name) {
    super(name);
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  protected InternalDistributedMember getDistributionManagerId() {
    Cache cache = getCache();
    DistributedSystem ds = cache.getDistributedSystem();
    return ((InternalDistributedSystem)ds).getDistributionManager().getId();
  }
  
  public void setUp() throws Exception {
    super.setUp();
    List vmList = new ArrayList();
    List idList = new ArrayList();
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vmList.add(vm);
        idList.add(vm.invoke(this, "getDistributionManagerId"));
      }
    }
    this.vms = (VM[])vmList.toArray(new VM[vmList.size()]);
    this.ids = (InternalDistributedMember[])idList.toArray(new InternalDistributedMember[idList.size()]);
  }
  
  public void testGenericAdvice() throws Exception {
    final RegionAttributes attrs = new AttributesFactory().create();
    assertTrue(attrs.getScope().isDistributedNoAck());
    assertTrue(attrs.getScope().isDistributed());
    final String rgnName = getUniqueName();
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(new CacheSerializableRunnable("CacheAdvisorDUnitTest.testGenericAdvice;createRegion") {
        public void run2() throws CacheException {
          createRegion(rgnName, attrs);
        }
      });
    }

    Set expected = new HashSet(Arrays.asList(ids));
    DistributedRegion rgn = (DistributedRegion)createRegion(rgnName, attrs);
    
    // root region
    DistributedRegion rootRgn = (DistributedRegion)getRootRegion();
    Set actual = rootRgn.getDistributionAdvisor().adviseGeneric();
    assertEquals("Unexpected advice for root region=" + rootRgn, expected, actual);
    
    // subregion
    actual = rgn.getDistributionAdvisor().adviseGeneric();
    assertEquals("Unexpected advice for subregion=" + rgn, expected, actual);
  }
      
  public void testNetWriteAdvice() throws Exception {
    final String rgnName = getUniqueName();
    Set expected = new HashSet();
    for (int i = 0; i < vms.length; i++) {
      VM vm = vms[i];
      InternalDistributedMember id = ids[i];
      if (i % 2 == 0) {
        expected.add(id);
      }
      final int index = i;
      vm.invoke(new CacheSerializableRunnable("CacheAdvisorDUnitTest.testNetWriteAdvice") {
        public void run2() throws CacheException {
          AttributesFactory fac = new AttributesFactory();
          if (index % 2 == 0) {
            fac.setCacheWriter(new CacheWriterAdapter());
          }
          createRegion(rgnName, fac.create());
        }
      });
    }
    
    RegionAttributes attrs = new AttributesFactory().create();
    DistributedRegion rgn = (DistributedRegion)createRegion(rgnName, attrs);
    assertEquals(expected, rgn.getCacheDistributionAdvisor().adviseNetWrite());    
  }
  
  public void testNetLoadAdvice() throws Exception {
    final String rgnName = getUniqueName();
    Set expected = new HashSet();
    for (int i = 0; i < vms.length; i++) {
      VM vm = vms[i];
      InternalDistributedMember id = ids[i];
      if (i % 2 == 1) {
        expected.add(id);
      }
      final int index = i;
      vm.invoke(new CacheSerializableRunnable("CacheAdvisorDUnitTest.testNetLoadAdvice") {
        public void run2() throws CacheException {
          AttributesFactory fac = new AttributesFactory();
          if (index % 2 == 1) {
            fac.setCacheLoader(new CacheLoader() {
              public Object load(LoaderHelper helper) throws CacheLoaderException {
                return null;
              }
              public void close() {
              }
            });
          }
          createRegion(rgnName, fac.create());
        }
      });
    }
    
    RegionAttributes attrs = new AttributesFactory().create();
    DistributedRegion rgn = (DistributedRegion)createRegion(rgnName, attrs);
    assertEquals(expected, rgn.getCacheDistributionAdvisor().adviseNetLoad());    
  }
  
  public void testNetLoadAdviceWithAttributesMutator() throws Exception {
    final String rgnName = getUniqueName();

    AttributesFactory fac = new AttributesFactory();
    fac.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes attrs = fac.create();
    DistributedRegion rgn = (DistributedRegion)createRegion(rgnName, attrs);
    
    invokeInEveryVM(new CacheSerializableRunnable("CachAdvisorTest.testNetLoadAdviceWithAttributesMutator;createRegion") {
      public void run2() throws CacheException {
        AttributesFactory f = new AttributesFactory();
        f.setScope(Scope.DISTRIBUTED_ACK);
        createRegion(rgnName, f.create());
      }
    });
        
    Set expected = new HashSet();
    for (int i = 1; i < vms.length; i += 2) {
      VM vm = vms[i];
      final int numVMsMinusOne = vms.length;
      InternalDistributedMember id = ids[i];
      expected.add(id);
//      final int index = i;
      vm.invoke(new CacheSerializableRunnable("CacheAdvisorDUnitTest.testNetLoadAdviceWithAttributesMutator;mutate") {
        public void run2() throws CacheException {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertEquals(numVMsMinusOne, ((DistributedRegion)rgn1).getDistributionAdvisor().adviseGeneric().size());
          AttributesMutator mut = rgn1.getAttributesMutator();
          mut.setCacheLoader(new CacheLoader() {
            public Object load(LoaderHelper helper) throws CacheLoaderException {
              return null;
            }
            public void close() {
            }
          });
         }
        });
    }
       
    assertEquals(expected,  rgn.getCacheDistributionAdvisor().adviseNetLoad());
  }

  /**
   * @param op needs to be one of the following:
   *   CACHE_CLOSE
   *   REGION_CLOSE
   *   REGION_LOCAL_DESTROY
   */
  private void basicTestClose(Operation op) throws Exception {
    final RegionAttributes attrs = new AttributesFactory().create();
    final String rgnName = getUniqueName();
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(new CacheSerializableRunnable("CacheAdvisorDUnitTest.basicTestClose; createRegion") {
        public void run2() throws CacheException {
          createRegion(rgnName, attrs);
        }
      });
    }
    
    DistributedRegion rgn = (DistributedRegion)createRegion(rgnName, attrs);
    Set expected = new HashSet(Arrays.asList(ids));
    assertEquals(expected, rgn.getDistributionAdvisor().adviseGeneric());
    final InternalDistributedMember myMemberId = getSystem().getDistributionManager().getId();
    
    // assert that other VMs advisors have test member id 
    invokeInEveryVM(new CacheSerializableRunnable("CacheAdvisorDUnitTest.basicTestClose;verify1") {
      public void run2() throws CacheException {
        DistributedRegion rgn1 = (DistributedRegion)getRootRegion();
        assertTrue(rgn1.getDistributionAdvisor().adviseGeneric().contains(myMemberId));
        rgn1 = (DistributedRegion)rgn1.getSubregion(rgnName);
        assertTrue(rgn1.getDistributionAdvisor().adviseGeneric().contains(myMemberId));
      }
    });
    if (op.equals(Operation.CACHE_CLOSE)) {
      closeCache();
    } else if (op.equals(Operation.REGION_CLOSE)) {
      getRootRegion().close();
    } else if (op.equals(Operation.REGION_LOCAL_DESTROY)) {
      getRootRegion().localDestroyRegion();
    } else {
      fail("expected op(" + op + ") to be CACHE_CLOSE, REGION_CLOSE, or REGION_LOCAL_DESTROY");
    }
    final InternalDistributedMember closedMemberId = getSystem().getDistributionManager().getId();
    invokeInEveryVM(new CacheSerializableRunnable("CacheAdvisorDUnitTest.basicTestClose;verify") {
      public void run2() throws CacheException {
        DistributedRegion rgn1 = (DistributedRegion)getRootRegion();
        assertTrue(!rgn1.getDistributionAdvisor().adviseGeneric().contains(closedMemberId));
        
        rgn1 = (DistributedRegion)rgn1.getSubregion(rgnName);
        assertTrue(!rgn1.getDistributionAdvisor().adviseGeneric().contains(closedMemberId));
      }
    });
  }
  
  /** coverage for bug 34255
   * @since 5.0
   */
 public void testRegionClose() throws Exception {
    basicTestClose(Operation.REGION_CLOSE);
  }

  /** coverage for bug 34255
   * @since 5.0
   */
  public void testRegionLocalDestroy() throws Exception {
    basicTestClose(Operation.REGION_LOCAL_DESTROY);
  }

  public void testCacheClose() throws Exception {
    basicTestClose(Operation.CACHE_CLOSE);
  }
}
