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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This is a dunit test for PartitionedRegion creation and Region API's for put and get
 * functionality in case of Custom Partitioning.
 */

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.*;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.cache.EntryOperationImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.internal.cache.PartitionedRegionDataStore.BucketVisitor;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.test.dunit.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Category(DistributedTest.class)
public class PRCustomPartitioningDUnitTest extends PartitionedRegionDUnitTestCase {

  public PRCustomPartitioningDUnitTest() {
    super();
  }

  protected static Cache cache = null;

  Properties props = new Properties();

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  static final int totalNumBuckets = 7;

  static ArrayList listOfKeys1 = new ArrayList();
  static ArrayList listOfKeys2 = new ArrayList();
  static ArrayList listOfKeys3 = new ArrayList();
  static ArrayList listOfKeys4 = new ArrayList();


  public static void createCacheInVm() throws Exception {
    Properties props = new Properties();
    // props.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "0");
    // props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new PRCustomPartitioningDUnitTest().createCache(props);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  /* SerializableRunnable object to create PR with a partition resolver */
  SerializableRunnable createPrRegionWithPartitionResolver =
      new CacheSerializableRunnable("createPrRegionWithDS") {

        public void run2() throws CacheException {
          AttributesFactory attr = new AttributesFactory();
          PartitionResolver resolver = MonthBasedPartitionResolver.getInstance();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setTotalNumBuckets(totalNumBuckets);
          paf.setPartitionResolver(resolver);
          paf.setRedundantCopies(0);

          PartitionAttributes prAttr = paf.create();
          attr.setPartitionAttributes(prAttr);
          RegionAttributes regionAttribs = attr.create();
          cache.createRegion("PR1", regionAttribs);
        }
      };

  /* SerializableRunnable object to create PR with a partition resolver */

  SerializableRunnable createPrRegionOnlyAccessorWithPartitionResolver =
      new CacheSerializableRunnable("createPrRegionOnlyAccessor") {

        public void run2() throws CacheException {
          AttributesFactory attr = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          PartitionResolver resolver = MonthBasedPartitionResolver.getInstance();
          PartitionAttributes prAttr = paf.setLocalMaxMemory(0).setTotalNumBuckets(totalNumBuckets)
              .setPartitionResolver(resolver).setRedundantCopies(0).create();

          attr.setPartitionAttributes(prAttr);
          RegionAttributes regionAttribs = attr.create();
          cache.createRegion("PR1", regionAttribs);

        }
      };

  /**
   * Search the entires PartitionedRegion for the key, to validate that indeed it doesn't exist
   * 
   * @returns true if it does exist
   * @param par
   * @param key
   */
  public static boolean searchForKey(PartitionedRegion par, Date key) {
    // Check to make super sure that the key exists
    boolean foundIt = false;
    final int numBucks = par.getTotalNumberOfBuckets();
    for (int b = 0; b < numBucks; b++) {
      if (par.getBucketKeys(b).contains(key)) {
        foundIt = true;
        LogWriterUtils.getLogWriter().info("Key " + key + " found in bucket " + b);
        break;
      }
    }
    if (!foundIt) {
      LogWriterUtils.getLogWriter().severe("Key " + key + " not found in any bucket");
    }
    return foundIt;
  }

  public void partitionedRegionTest(final String prName) {
    /*
     * Do put() operations through VM with PR having both Accessor and Datastore
     */
    vm0.invoke(new CacheSerializableRunnable("doPutCreateInvalidateOperations1") {
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

        for (int i = 0; i <= 11; i++) {
          int yr = (new Integer((int) (Math.random() * 2100))).intValue();
          int month = i;
          int date = (new Integer((int) (Math.random() * 30))).intValue();
          cal.set(yr, month, date);
          Object key = cal.getTime();
          listOfKeys1.add(key);
          assertNotNull(pr);
          pr.put(key, Integer.toString(i));
          assertEquals(Integer.toString(i), pr.get(key));

        }
        PartitionedRegion ppr = (PartitionedRegion) pr;
        try {
          ppr.dumpAllBuckets(false);
        } catch (ReplyException re) {
          Assert.fail("dumpAllBuckets", re);
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("doPutCreateInvalidateOperations2") {
      public void run2() throws CacheException {

        Calendar cal = Calendar.getInstance();
        final Region pr = cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }

        for (int i = 0; i <= 11; i++) {

          int yr = (new Integer((int) (Math.random() * 2200))).intValue();
          int month = i;
          int date = (new Integer((int) (Math.random() * 30))).intValue();

          cal.set(yr, month, date);
          Object key = cal.getTime();
          listOfKeys2.add(key);

          assertNotNull(pr);
          pr.put(key, Integer.toString(i));
          assertEquals(Integer.toString(i), pr.get(key));

        }
        PartitionedRegion ppr = (PartitionedRegion) pr;
        try {
          ppr.dumpAllBuckets(false);
        } catch (ReplyException re) {
          Assert.fail("dumpAllBuckets", re);
        }
      }
    });

    vm2.invoke(new CacheSerializableRunnable("doPutCreateInvalidateOperations2") {
      public void run2() throws CacheException {

        Calendar cal = Calendar.getInstance();
        final Region pr = cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }

        for (int i = 0; i <= 11; i++) {

          int yr = (new Integer((int) (Math.random() * 2300))).intValue();
          int month = i;
          int date = (new Integer((int) (Math.random() * 30))).intValue();

          cal.set(yr, month, date);
          Object key = cal.getTime();
          listOfKeys3.add(key);

          assertNotNull(pr);
          pr.put(key, Integer.toString(i));
          assertEquals(Integer.toString(i), pr.get(key));
        }
        PartitionedRegion ppr = (PartitionedRegion) pr;
        try {
          ppr.dumpAllBuckets(false);
        } catch (ReplyException re) {
          Assert.fail("dumpAllBuckets", re);
        }
      }
    });

    vm3.invoke(new CacheSerializableRunnable("doPutCreateInvalidateOperations3") {
      public void run2() throws CacheException {

        Calendar cal = Calendar.getInstance();
        final Region pr = cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }

        for (int i = 0; i <= 11; i++) {
          int yr = (new Integer((int) (Math.random() * 2400))).intValue();
          int month = i;
          int date = (new Integer((int) (Math.random() * 30))).intValue();

          cal.set(yr, month, date);
          Object key = cal.getTime();
          listOfKeys4.add(key);

          assertNotNull(pr);
          pr.put(key, Integer.toString(i));
          assertEquals(Integer.toString(i), pr.get(key));

        }
        PartitionedRegion ppr = (PartitionedRegion) pr;
        try {
          ppr.dumpAllBuckets(false);
        } catch (ReplyException re) {
          Assert.fail("dumpAllBuckets", re);
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("verifyKeysonVM0") {
      public void run2() throws CacheException {

        // Calendar cal = Calendar.getInstance();
        final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        Iterator itr = listOfKeys1.iterator();
        while (itr.hasNext()) {
          assertTrue(searchForKey(pr, (Date) itr.next()));
        }
        pr.getDataStore().visitBuckets(new BucketVisitor() {
          public void visit(Integer bucketId, Region r) {
            Set s = pr.getBucketKeys(bucketId.intValue());
            Iterator it = s.iterator();
            while (it.hasNext()) {
              EntryOperation eo = new EntryOperationImpl(pr, null, it.next(), null, null);
              PartitionResolver rr = pr.getPartitionResolver();
              Object o = rr.getRoutingObject(eo);
              Integer i = new Integer(o.hashCode() % totalNumBuckets);
              assertEquals(bucketId, i);
            } // getLogWriter().severe("Key " + key + " found in bucket " + b);
          }
        });
      }
    });

    vm1.invoke(new CacheSerializableRunnable("verifyKeysonVM1") {
      public void run2() throws CacheException {

        // Calendar cal = Calendar.getInstance();
        final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        Iterator itr = listOfKeys2.iterator();
        while (itr.hasNext()) {
          assertTrue(searchForKey(pr, (Date) itr.next()));
        }
        pr.getDataStore().visitBuckets(new BucketVisitor() {
          public void visit(Integer bucketId, Region r) {
            Set s = pr.getBucketKeys(bucketId.intValue());
            Iterator it = s.iterator();
            while (it.hasNext()) {
              EntryOperation eo = new EntryOperationImpl(pr, null, it.next(), null, null);
              PartitionResolver rr = pr.getPartitionResolver();
              Object o = rr.getRoutingObject(eo);
              Integer i = new Integer(o.hashCode() % totalNumBuckets);
              assertEquals(bucketId, i);
            } // getLogWriter().severe("Key " + key + " found in bucket " + b);
          }
        });
      }
    });

    vm2.invoke(new CacheSerializableRunnable("verifyKeysonVM2") {
      public void run2() throws CacheException {

        // Calendar cal = Calendar.getInstance();
        final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        Iterator itr = listOfKeys3.iterator();
        itr = listOfKeys3.iterator();
        while (itr.hasNext()) {
          assertTrue(searchForKey(pr, (Date) itr.next()));
        }
        pr.getDataStore().visitBuckets(new BucketVisitor() {
          public void visit(Integer bucketId, Region r) {
            Set s = pr.getBucketKeys(bucketId.intValue());
            Iterator it = s.iterator();
            while (it.hasNext()) {
              EntryOperation eo = new EntryOperationImpl(pr, null, it.next(), null, null);
              PartitionResolver rr = pr.getPartitionResolver();
              Object o = rr.getRoutingObject(eo);
              Integer i = new Integer(o.hashCode() % totalNumBuckets);
              // assertIndexDetailsEquals(bucketId, bucketId);
              assertEquals(bucketId, i);
            } // getLogWriter().severe("Key " + key + " found in bucket " + b);
          }
        });
      }
    });

    vm3.invoke(new CacheSerializableRunnable("verifyKeysonVM3") {
      public void run2() throws CacheException {

        // Calendar cal = Calendar.getInstance();
        final PartitionedRegion pr = (PartitionedRegion) cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        Iterator itr = listOfKeys4.iterator();
        itr = listOfKeys4.iterator();
        while (itr.hasNext()) {
          assertTrue(searchForKey(pr, (Date) itr.next()));
        }
        assertEquals(pr.getDataStore(), null);
      }
    });

  }

  /**
   * This is a PartitionedRegion test for Custom Partitioning . 4 VMs are used to create the PR with
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
    final VM accessor = vm3;
    // create cache in all vms

    vm0.invoke(() -> PRCustomPartitioningDUnitTest.createCacheInVm());
    vm1.invoke(() -> PRCustomPartitioningDUnitTest.createCacheInVm());
    vm2.invoke(() -> PRCustomPartitioningDUnitTest.createCacheInVm());
    accessor.invoke(() -> PRCustomPartitioningDUnitTest.createCacheInVm());



    // Create PR;s in different VM's
    vm0.invoke(createPrRegionWithPartitionResolver);
    vm1.invoke(createPrRegionWithPartitionResolver);
    vm2.invoke(createPrRegionWithPartitionResolver);
    accessor.invoke(createPrRegionOnlyAccessorWithPartitionResolver);

    partitionedRegionTest("/PR1");
    /*
     * destroy the Region.
     */
    destroyTheRegion("/PR1");
  }

  public void destroyTheRegion(final String name) {
    /*
     * destroy the Region.
     */
    vm0.invoke(new CacheSerializableRunnable("destroyRegionOp") {

      public void run2() throws CacheException {

        Region pr = cache.getRegion(name);
        if (pr == null) {
          fail(name + " not created");
        }
        pr.destroyRegion();
      }
    });
  }
}


/**
 * Example implementation of a Partition Resolver which uses part of the value for custom
 * partitioning.
 * 
 */
class MonthBasedPartitionResolver implements PartitionResolver, Declarable2 {

  private static MonthBasedPartitionResolver mbrResolver = null;
  final static String id = "MonthBasedPartitionResolverid1";
  private Properties properties;
  private String resolverName;


  public MonthBasedPartitionResolver() {}

  public static MonthBasedPartitionResolver getInstance() {
    if (mbrResolver == null) {
      mbrResolver = new MonthBasedPartitionResolver();
    }
    return mbrResolver;
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Serializable routingObj = (Serializable) opDetails.getKey();
    Calendar cal = Calendar.getInstance();
    cal.setTime((Date) routingObj);
    return new SerializableMonth(cal.get(Calendar.MONTH));
  }

  public void close() {
    // Close internal state when Region closes
  }

  public void init(Properties props) {
    this.properties = props;
  }

  // public Properties getProperties(){
  // return this.properties;
  // }

  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof MonthBasedPartitionResolver) {
      // MonthBasedPartitionResolver epc = (MonthBasedPartitionResolver) obj;
      return id.equals(MonthBasedPartitionResolver.id);
    } else {
      return false;
    }
  }

  public String getName() {
    return this.resolverName;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.properties;
  }
}


class SerializableMonth implements DataSerializable {
  private int month;

  public SerializableMonth(int month) {
    this.month = month;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.month = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.month);
  }

  public int hashCode() {
    if (this.month < 4)
      return 1;
    else if (this.month >= 4 && this.month < 8)
      return 2;
    else
      return 3;
  }
}

