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
package org.apache.geode.internal.cache.execute;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PRPerformanceTestDUnitTest extends CacheTestCase {

  private static final int TOTAL_NUM_BUCKETS = 13;
  private static final byte[] VALUE_ARRAY = new byte[1024];
  private static final List LIST_OF_KEYS = new ArrayList();

  private static Cache cache = null;

  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM vm3 = null;

  /**
   * This is a PartitionedRegion test for Custom Prtitioning . 4 VMs are used to create the PR with
   * and without(Only Accessor) the DataStore.
   */
  @Test
  public void testPartitionedRegionOperationsCustomPartitioning() throws Exception {
    Host host = Host.getHost(0);

    // create the VM(0 - 4)
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    final VM accessor = vm0;
    // final VM accessor = vm3;
    // create cache in all vms

    accessor.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());
    vm1.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());
    vm2.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());
    vm3.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());

    // Create PR;s in different VM's
    accessor.invoke(createPrRegionOnlyAccessorWithPartitionResolver);
    vm1.invoke(createPrRegionWithPartitionResolver);
    vm2.invoke(createPrRegionWithPartitionResolver);
    vm3.invoke(createPrRegionWithPartitionResolver);

    partitionedRegionTest("/PR1", 50);

    destroyTheRegion("/PR1");
  }

  private void partitionedRegionTest(final String prName, final int noOfEntries) {
    /*
     * Do put() operations through VM with PR having both Accessor and Datastore
     */
    vm0.invoke(new CacheSerializableRunnable("doPutCreateInvalidateOperations1") {
      @Override
      public void run2() throws CacheException {

        Calendar cal = Calendar.getInstance();
        final Region pr = cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        int size = 0;

        size = pr.size();
        assertEquals("Size doesnt return expected value", 0, size);
        assertEquals("isEmpty doesnt return proper state of the PartitionedRegion", true,
            pr.isEmpty());
        assertEquals(0, pr.keySet().size());
        int entries = noOfEntries;
        while (entries > 0) {
          for (int i = 0; i <= 11; i++) {
            int yr = (new Integer((int) (Math.random() * 2100))).intValue();
            int month = i;
            int date = (new Integer((int) (Math.random() * 30))).intValue();
            cal.set(yr, month, date);
            Object key = cal.getTime();
            LIST_OF_KEYS.add(key);
            assertNotNull(pr);
            // pr.put(key, Integer.toString(i));
            pr.put(key, VALUE_ARRAY);
            // assertIndexDetailsEquals(VALUE_ARRAY, pr.get(key));
          }
          entries--;
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("verifyKeysonVM0") {
      @Override
      public void run2() throws CacheException {

        // Calendar cal = Calendar.getInstance();
        final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        Iterator itr = LIST_OF_KEYS.iterator();
        while (itr.hasNext()) {
          assertTrue(searchForKey(pr, (Date) itr.next()));

        }
        // Intitial warm up phase ..Do a get of all the keys
        // Iterate over the key and try to get all the values repetitively
        itr = LIST_OF_KEYS.iterator();
        ArrayList vals = new ArrayList();
        while (itr.hasNext()) {
          Object val = pr.get(itr.next());
          assertNotNull(val);
          vals.add(val);
          // assertTrue(searchForKey(pr, (Date)itr.next()));
        }

        // Call the execute method for each key
        PerformanceTestFunction function = new PerformanceTestFunction();
        FunctionService.registerFunction(function);
        DefaultResultCollector drc = new DefaultResultCollector();
        // final Set allKeysSet = new HashSet();
        final Set singleKeySet = new HashSet();
        Execution dataSet = FunctionService.onRegion(pr);
        vals.clear();
        ArrayList list = new ArrayList();
        itr = LIST_OF_KEYS.iterator();

        while (itr.hasNext()) {
          singleKeySet.add(itr.next());
          dataSet = dataSet.withFilter(singleKeySet);
          try {
            ResultCollector rc = dataSet.execute(function.getId());
            list = (ArrayList) rc.getResult();
          } catch (Exception ex) {
            LogWriterUtils.getLogWriter().info("Exception Occurred :" + ex.getMessage());
            Assert.fail("Test failed", ex);
          }
          Object val = list.get(0);
          assertNotNull(val);
          vals.add(val);
          singleKeySet.clear();
          // assertTrue(searchForKey(pr, (Date)itr.next()));
        }
        assertEquals(vals.size(), LIST_OF_KEYS.size());
        // END: warmup

        // Now start the performance count
        itr = LIST_OF_KEYS.iterator();
        TimeKeeper t = new TimeKeeper();
        vals.clear();
        t.start();
        // ArrayList vals = new ArrayList();
        while (itr.hasNext()) {
          Object val = pr.get(itr.next());
          assertNotNull(val);
          vals.add(val);
          // assertTrue(searchForKey(pr, (Date)itr.next()));
        }

        t.stop();
        LogWriterUtils.getLogWriter().info("Time taken to iterate over " + vals.size()
            + " no. of keys: " + t.getTimeInMs() + " ms");

        // Call the execute method for each key and see if this takes more time

        vals.clear();
        t = new TimeKeeper();
        t.start();
        // ArrayList list = new ArrayList();
        itr = LIST_OF_KEYS.iterator();

        while (itr.hasNext()) {
          singleKeySet.add(itr.next());
          dataSet = dataSet.withFilter(singleKeySet);
          try {
            ResultCollector rc = dataSet.execute(function.getId());
            list = (ArrayList) rc.getResult();
          } catch (Exception expected) {
            // No data should cause exec to throw
          }
          Object val = list.get(0);
          assertNotNull(val);
          vals.add(val);
          singleKeySet.clear();
        }
        t.stop();
        assertEquals(vals.size(), LIST_OF_KEYS.size());
        LogWriterUtils.getLogWriter().info("Time taken to iterate over " + vals.size()
            + " no. of keys using FunctionExecution: " + t.getTimeInMs() + " ms");
      }
    });
  }

  private void destroyTheRegion(final String name) {
    vm0.invoke(new CacheSerializableRunnable("destroyRegionOp") {

      @Override
      public void run2() throws CacheException {
        Region pr = cache.getRegion(name);
        if (pr == null) {
          fail(name + " not created");
        }
        pr.destroyRegion();
      }
    });
  }

  public static void createCacheInVm() throws Exception {
    Properties props = new Properties();
    new PRPerformanceTestDUnitTest().createCache(props);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  private SerializableRunnable createPrRegionWithPartitionResolver =
      new CacheSerializableRunnable("createPrRegionWithDS") {

        @Override
        public void run2() throws CacheException {
          AttributesFactory attr = new AttributesFactory();
          PartitionResolver resolver = new MonthBasedPartitionResolver();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
          paf.setPartitionResolver(resolver);
          paf.setLocalMaxMemory(900);
          paf.setRedundantCopies(0);

          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          RegionAttributes regionAttribs = attr.create();
          cache.createRegion("PR1", regionAttribs);
          PerformanceTestFunction function = new PerformanceTestFunction();
          FunctionService.registerFunction(function);
        }
      };

  private SerializableRunnable createPrRegionOnlyAccessorWithPartitionResolver =
      new CacheSerializableRunnable("createPrRegionOnlyAccessor") {

        @Override
        public void run2() throws CacheException {
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          PartitionResolver resolver = new MonthBasedPartitionResolver();
          PartitionAttributes prAttr =
              paf.setLocalMaxMemory(0).setTotalNumBuckets(TOTAL_NUM_BUCKETS)
                  .setPartitionResolver(resolver).setRedundantCopies(0).create();

          attr.setPartitionAttributes(prAttr);
          RegionAttributes regionAttribs = attr.create();
          cache.createRegion("PR1", regionAttribs);

        }
      };

  /**
   * Search the entries PartitionedRegion for the key, to validate that indeed it doesn't exist
   */
  private static boolean searchForKey(PartitionedRegion par, Date key) {
    // Check to make super sure that the key exists
    boolean foundIt = false;
    final int numBucks = par.getTotalNumberOfBuckets();
    for (int b = 0; b < numBucks; b++) {
      if (par.getBucketKeys(b).contains(key)) {
        foundIt = true;
        break;
      }
    }
    if (!foundIt) {
      LogWriterUtils.getLogWriter().severe("Key " + key + " not found in any bucket");
    }
    return foundIt;
  }
}
