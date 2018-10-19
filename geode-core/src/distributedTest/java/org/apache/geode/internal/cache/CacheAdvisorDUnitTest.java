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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Tests the use of CacheDistributionAdvisor in createSubRegion
 *
 *
 */

public class CacheAdvisorDUnitTest extends JUnit4CacheTestCase {
  private transient VM[] vms;
  private transient InternalDistributedMember[] ids;

  /** Creates a new instance of CacheAdvisorDUnitTest */
  public CacheAdvisorDUnitTest() {
    super();
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  protected InternalDistributedMember getDistributionManagerId() {
    Cache cache = getCache();
    DistributedSystem ds = cache.getDistributedSystem();
    return ((InternalDistributedSystem) ds).getDistributionManager().getId();
  }

  @Override
  public final void postSetUp() throws Exception {
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
    this.vms = (VM[]) vmList.toArray(new VM[vmList.size()]);
    this.ids =
        (InternalDistributedMember[]) idList.toArray(new InternalDistributedMember[idList.size()]);
  }

  @Test
  public void testGenericAdvice() throws Exception {
    final RegionAttributes attrs = new AttributesFactory().create();
    assertTrue(attrs.getScope().isDistributedNoAck());
    assertTrue(attrs.getScope().isDistributed());
    final String rgnName = getUniqueName();
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(
          new CacheSerializableRunnable("CacheAdvisorDUnitTest.testGenericAdvice;createRegion") {
            public void run2() throws CacheException {
              final RegionAttributes attrs = new AttributesFactory().create();
              createRegion(rgnName, attrs);
            }
          });
    }

    Set expected = new HashSet(Arrays.asList(ids));
    DistributedRegion rgn = (DistributedRegion) createRegion(rgnName, attrs);

    // root region
    DistributedRegion rootRgn = (DistributedRegion) getRootRegion();
    Set actual = rootRgn.getDistributionAdvisor().adviseGeneric();
    assertEquals("Unexpected advice for root region=" + rootRgn, expected, actual);

    // subregion
    actual = rgn.getDistributionAdvisor().adviseGeneric();
    assertEquals("Unexpected advice for subregion=" + rgn, expected, actual);
  }

  @Test
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
    DistributedRegion rgn = (DistributedRegion) createRegion(rgnName, attrs);
    assertEquals(expected, rgn.getCacheDistributionAdvisor().adviseNetWrite());
  }

  @Test
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

              public void close() {}
            });
          }
          createRegion(rgnName, fac.create());
        }
      });
    }

    RegionAttributes attrs = new AttributesFactory().create();
    DistributedRegion rgn = (DistributedRegion) createRegion(rgnName, attrs);
    assertEquals(expected, rgn.getCacheDistributionAdvisor().adviseNetLoad());
  }

  @Test
  public void testNetLoadAdviceWithAttributesMutator() throws Exception {
    final String rgnName = getUniqueName();

    AttributesFactory fac = new AttributesFactory();
    fac.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes attrs = fac.create();
    DistributedRegion rgn = (DistributedRegion) createRegion(rgnName, attrs);

    Invoke.invokeInEveryVM(new CacheSerializableRunnable(
        "CachAdvisorTest.testNetLoadAdviceWithAttributesMutator;createRegion") {
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
      // final int index = i;
      vm.invoke(new CacheSerializableRunnable(
          "CacheAdvisorDUnitTest.testNetLoadAdviceWithAttributesMutator;mutate") {
        public void run2() throws CacheException {
          Region rgn1 = getRootRegion().getSubregion(rgnName);
          assertEquals(numVMsMinusOne,
              ((DistributedRegion) rgn1).getDistributionAdvisor().adviseGeneric().size());
          AttributesMutator mut = rgn1.getAttributesMutator();
          mut.setCacheLoader(new CacheLoader() {
            public Object load(LoaderHelper helper) throws CacheLoaderException {
              return null;
            }

            public void close() {}
          });
        }
      });
    }

    await()
        .untilAsserted(
            () -> assertEquals(expected, rgn.getCacheDistributionAdvisor().adviseNetLoad()));
  }

  /**
   * @param op needs to be one of the following: CACHE_CLOSE REGION_CLOSE REGION_LOCAL_DESTROY
   */
  private void basicTestClose(Operation op) throws Exception {
    final String rgnName = getUniqueName();
    for (int i = 0; i < vms.length; i++) {
      vms[i].invoke(
          new CacheSerializableRunnable("CacheAdvisorDUnitTest.basicTestClose; createRegion") {
            public void run2() throws CacheException {
              final RegionAttributes attrs = new AttributesFactory().create();
              createRegion(rgnName, attrs);
            }
          });
    }

    final RegionAttributes attrs = new AttributesFactory().create();
    DistributedRegion rgn = (DistributedRegion) createRegion(rgnName, attrs);
    Set expected = new HashSet(Arrays.asList(ids));
    assertEquals(expected, rgn.getDistributionAdvisor().adviseGeneric());
    final InternalDistributedMember myMemberId = getSystem().getDistributionManager().getId();

    // assert that other VMs advisors have test member id
    Invoke.invokeInEveryVM(
        new CacheSerializableRunnable("CacheAdvisorDUnitTest.basicTestClose;verify1") {
          public void run2() throws CacheException {
            DistributedRegion rgn1 = (DistributedRegion) getRootRegion();
            assertTrue(rgn1.getDistributionAdvisor().adviseGeneric().contains(myMemberId));
            rgn1 = (DistributedRegion) rgn1.getSubregion(rgnName);
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
    Invoke.invokeInEveryVM(
        new CacheSerializableRunnable("CacheAdvisorDUnitTest.basicTestClose;verify") {
          public void run2() throws CacheException {
            DistributedRegion rgn1 = (DistributedRegion) getRootRegion();
            assertTrue(!rgn1.getDistributionAdvisor().adviseGeneric().contains(closedMemberId));

            rgn1 = (DistributedRegion) rgn1.getSubregion(rgnName);
            assertTrue(!rgn1.getDistributionAdvisor().adviseGeneric().contains(closedMemberId));
          }
        });
  }

  /**
   * coverage for bug 34255
   *
   * @since GemFire 5.0
   */
  @Test
  public void testRegionClose() throws Exception {
    basicTestClose(Operation.REGION_CLOSE);
  }

  /**
   * coverage for bug 34255
   *
   * @since GemFire 5.0
   */
  @Test
  public void testRegionLocalDestroy() throws Exception {
    basicTestClose(Operation.REGION_LOCAL_DESTROY);
  }

  @Test
  public void testCacheClose() throws Exception {
    basicTestClose(Operation.CACHE_CLOSE);
  }
}
