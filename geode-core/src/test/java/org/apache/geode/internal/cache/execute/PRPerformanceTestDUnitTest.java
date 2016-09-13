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
package org.apache.geode.internal.cache.execute;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This is a dunit test for PartitionedRegion creation and Region API's
 * for put and get functionality in case of Custom Partitioning.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.DataSerializable;
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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

@Category(DistributedTest.class)
public class PRPerformanceTestDUnitTest extends
    PartitionedRegionDUnitTestCase {

  public PRPerformanceTestDUnitTest() {
    super();
  }

  protected static Cache cache = null;
  
  Properties props = new Properties();

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  static final int totalNumBuckets = 13;
  
  static ArrayList listOfKeys = new ArrayList();

  protected static final byte [] valueArray = new byte[1024];
  
  public static void createCacheInVm() throws Exception{
    Properties props = new Properties();
    new PRPerformanceTestDUnitTest().createCache(props);
  }
  
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }
  
  /* SerializableRunnable object to create PR with a partition resolver */
  SerializableRunnable createPrRegionWithPartitionResolver = new CacheSerializableRunnable(
      "createPrRegionWithDS") {

    public void run2() throws CacheException {      
      AttributesFactory attr = new AttributesFactory();
      PartitionResolver resolver = MonthBasedPartitionResolver.getInstance();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setTotalNumBuckets(totalNumBuckets);
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

  /* SerializableRunnable object to create PR with a partition resolver */

  SerializableRunnable createPrRegionOnlyAccessorWithPartitionResolver = new CacheSerializableRunnable(
      "createPrRegionOnlyAccessor") {

    public void run2() throws CacheException {      
      AttributesFactory attr = new AttributesFactory();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      PartitionResolver resolver = MonthBasedPartitionResolver.getInstance();
      PartitionAttributes prAttr = paf.setLocalMaxMemory(0).setTotalNumBuckets(
          totalNumBuckets).setPartitionResolver(resolver).setRedundantCopies(0)
          .create();

      attr.setPartitionAttributes(prAttr);
      RegionAttributes regionAttribs = attr.create();
      cache.createRegion("PR1", regionAttribs);
      
    }
  };
  /**
   * Search the entires PartitionedRegion for the key, to validate that indeed
   * it doesn't exist
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
        //getLogWriter().info("Key " + key + " found in bucket " + b);
        break;
      }
    }
    if (!foundIt) {
      LogWriterUtils.getLogWriter().severe("Key " + key + " not found in any bucket");      
    }
    return foundIt;
  }

  public void partitionedRegionTest(final String prName, final int  noOfEntries) {
    /*
     * Do put() operations through VM with PR having
     * both Accessor and Datastore
     */
    vm0
        .invoke(new CacheSerializableRunnable(
            "doPutCreateInvalidateOperations1") {
          public void run2() throws CacheException {
            
            Calendar cal = Calendar.getInstance();
            final Region pr = cache.getRegion(prName);
            if (pr == null) {
              fail(prName + " not created");
            }
            int size = 0;

            size = pr.size();
            assertEquals("Size doesnt return expected value", 0, size);
            assertEquals(
                "isEmpty doesnt return proper state of the PartitionedRegion",
                true, pr.isEmpty());
            assertEquals(0, pr.keySet().size());            
            int entries= noOfEntries;
            while(entries > 0) {
              for (int i = 0; i <= 11; i++) {
                int yr = (new Integer((int)(Math.random() * 2100))).intValue();
                int month = i;
                int date = (new Integer((int)(Math.random() * 30))).intValue();
                cal.set(yr, month, date);
                Object key = cal.getTime();
                listOfKeys.add(key);
                assertNotNull(pr);
                //pr.put(key, Integer.toString(i));
                pr.put(key, valueArray);
                //assertIndexDetailsEquals(valueArray, pr.get(key));
  
              }
              entries--;
           }
          }
        });



    vm0.invoke(new CacheSerializableRunnable(
        "verifyKeysonVM0") {
      public void run2() throws CacheException {

//        Calendar cal = Calendar.getInstance();
        final PartitionedRegion pr = (PartitionedRegion)cache.getRegion(prName);
        if (pr == null) {
          fail(prName + " not created");
        }
        Iterator itr = listOfKeys.iterator();
        while (itr.hasNext()) {
          assertTrue(searchForKey(pr, (Date)itr.next()));
          
        }
        // Intitial warm up phase ..Do a get of all the keys
        // Iterate over the key and try to get all the values repetitively
        itr = listOfKeys.iterator();
        ArrayList vals = new ArrayList();
        while (itr.hasNext()) {          
          Object val = pr.get(itr.next());
          assertNotNull(val);
          vals.add(val);         
          //assertTrue(searchForKey(pr, (Date)itr.next()));          
        }
                        
        // Call the execute method for each key 
        PerformanceTestFunction function = new PerformanceTestFunction();
        FunctionService.registerFunction(function);
        DefaultResultCollector drc = new DefaultResultCollector();
        //final Set allKeysSet = new HashSet();
        final Set singleKeySet = new HashSet();
        Execution dataSet = FunctionService.onRegion(pr);
        vals.clear();
        ArrayList list = new ArrayList();
        itr = listOfKeys.iterator();
        
        while (itr.hasNext()) {          
          singleKeySet.add(itr.next());
          dataSet = dataSet.withFilter(singleKeySet);
          try {
            ResultCollector rc = dataSet.execute(function.getId());
            list = (ArrayList)rc.getResult();
          }
          catch (Exception ex) {
            LogWriterUtils.getLogWriter().info("Exception Occured :" + ex.getMessage());
            Assert.fail("Test failed",ex);
          }
          Object val = list.get(0);
          assertNotNull(val);
          vals.add(val);
          singleKeySet.clear();
          //assertTrue(searchForKey(pr, (Date)itr.next()));          
        }        
        assertEquals(vals.size(),listOfKeys.size());                    
        // END: warmup
        
        // Now start the performance count
        itr = listOfKeys.iterator();
        TimeKeeper t = new TimeKeeper();
        vals.clear();
        t.start();
        // ArrayList vals = new ArrayList();
        while (itr.hasNext()) {          
          Object val = pr.get(itr.next());
          assertNotNull(val);
          vals.add(val);         
          //assertTrue(searchForKey(pr, (Date)itr.next()));          
        }
        
        t.stop();        
        LogWriterUtils.getLogWriter().info("Time taken to iterate over " + vals.size()+ " no. of keys: " + t.getTimeInMs() + " ms");
                
        // Call the execute method for each key and see if this takes more time

        vals.clear();
        t = new TimeKeeper();
        t.start();
        //ArrayList list = new ArrayList();
        itr = listOfKeys.iterator();
        
        while (itr.hasNext()) {          
          singleKeySet.add(itr.next());
          dataSet = dataSet.withFilter(singleKeySet);
          try {
            ResultCollector rc = dataSet.execute(function.getId());
            list = (ArrayList)rc.getResult();
          }
          catch (Exception expected) {
            // No data should cause exec to throw          
          }
          Object val = list.get(0);
          assertNotNull(val);
          vals.add(val);
          singleKeySet.clear();          
        }
        t.stop();
        assertEquals(vals.size(),listOfKeys.size());            
        LogWriterUtils.getLogWriter().info("Time taken to iterate over " + vals.size()+ " no. of keys using FunctionExecution: " + t.getTimeInMs() + " ms");
        
      }
    });

  }

  /**
   * This is a PartitionedRegion test for Custom Prtitioning . 4 VMs are used to create the PR with
   * and without(Only Accessor) the DataStore.
   */
  @Test
  public void testPartitionedRegionOperationsCustomPartitioning()
      throws Exception {
    Host host = Host.getHost(0);
    
    // create the VM(0 - 4)
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    final VM accessor = vm0;
    //final VM accessor = vm3;
    //create cache in all vms
    
    accessor.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());
    vm1.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());
    vm2.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());
    vm3.invoke(() -> PRPerformanceTestDUnitTest.createCacheInVm());

    

    // Create PR;s in different VM's
    accessor.invoke(createPrRegionOnlyAccessorWithPartitionResolver);               
    vm1.invoke(createPrRegionWithPartitionResolver);
    vm2.invoke(createPrRegionWithPartitionResolver);
    vm3.invoke(createPrRegionWithPartitionResolver);
    
    partitionedRegionTest("/PR1",50);
    /*
     * destroy the Region.
     */
    destroyTheRegion("/PR1");
    
    // Create PR;s in different VM's    
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

class SerializablePerfMonth implements DataSerializable{
  private int month;
  
  public SerializablePerfMonth(int month) {
    this.month = month;
  }
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.month = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.month);
  }
  
  public int hashCode() {
    /*if(this.month<4)
      return 1;
    else if(this.month >= 4 && this.month < 8)
      return 2;
    else 
      return 3;
      
      */
    return this.month;
  }
}
class TimeKeeper {
  private long startTime = -1;

  private long endTime = -1;

  public void start() {
    startTime = System.currentTimeMillis();
  }

  public void stop() {
    endTime = System.currentTimeMillis();
  }

  public long getTimeInMs() {
    if ((startTime == -1) || (endTime == -1)) {
      System.err.println("call start() and stop() before this method");
      return -1;
    }
    else if ((endTime == startTime)) {
      System.err
          .println("the start time and end time are the same...returning 1ms");
      return 1;
    }
    else
      return (endTime - startTime);
  }
}
