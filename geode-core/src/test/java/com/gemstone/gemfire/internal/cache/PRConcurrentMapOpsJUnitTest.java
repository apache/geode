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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Tests ConcurrentMap operations on a PartitionedRegion on a single node.
 *
 */

@Category(IntegrationTest.class)
public class PRConcurrentMapOpsJUnitTest {
  
  static DistributedSystem sys;
  
  static Cache cache;
  
  /**
   * Supports running test in two standalone VMs for debugging purposes.
   * Start one VM with the argument "far" to only host data.
   * Start the second VM with no arg or "near" to run the test in. 
   */
  public static void main(String[] args) throws Exception {
    if (args.length > 0 && "far".equals(args[0])) {
      new PRConcurrentMapOpsJUnitTest();
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      
      Properties globalProps = new Properties();
      globalProps.put(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY,
                      "100");
      
      PartitionAttributes pa = paf.setRedundantCopies(0)
      .setLocalMaxMemory(100).create();
      AttributesFactory af = new AttributesFactory();
      af.setPartitionAttributes(pa);
      RegionAttributes ra = af.create();
      
      PartitionedRegion pr = null;
      pr = (PartitionedRegion)cache.createRegion("PR4", ra);
      assert pr != null : "PR4 not created";
      System.out.println("\nData Node: Sleeping forever. Ctrl-C to stop.");
      Thread.sleep(Integer.MAX_VALUE);
      return;
    }
    
    PRConcurrentMapOpsJUnitTest t = new PRConcurrentMapOpsJUnitTest();
    t.testLocalConcurrentMapOps();
  }

  @Before
  public void setUp() {
    if (cache == null) {
      Properties dsProps = new Properties();
      dsProps.setProperty(MCAST_PORT, "0");
      
      //  Connect to a DS and create a Cache.
      sys = DistributedSystem.connect(dsProps);
      cache = CacheFactory.create(sys);
    }
  }
  
  @After
  public void tearDown() {
    sys.disconnect();
  }
  
  @Test
  public void testLocalConcurrentMapOps() throws Exception {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    
    Properties globalProps = new Properties();
    globalProps.put(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY,
                    "100");
    
    PartitionAttributes pa = paf.setRedundantCopies(0)
                                .setLocalMaxMemory(100).create();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    RegionAttributes ra = af.create();
    
    PartitionedRegion pr = null;
    pr = (PartitionedRegion)cache.createRegion("PR4", ra);
    assertNotNull("PR4 not created", pr);
    
    int start = 1;
    int end = 50;
    int end2 = 100;
    
    // successful putIfAbsent
    for (int i = start; i <= end; i++) {
      Object putResult = pr.putIfAbsent(Integer.toString(i),
                                        Integer.toString(i));
      assertNull("Expected null, but got " + putResult + "for key " + i,
                 putResult);
    }
    int size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    // unsuccessful putIfAbsent
    for (int i = start; i <= end; i++) {
      Object putResult = pr.putIfAbsent(Integer.toString(i),
                                        Integer.toString(i + 1));
      assertEquals("for i=" + i, Integer.toString(i), putResult);
      assertEquals("for i=" + i, Integer.toString(i), pr.get(Integer.toString(i)));
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    // successful replace(Object, Object)    
    for (int i = start; i <= end; i++) {
      Object replaceResult =  pr.replace(Integer.toString(i),
                                         Integer.toString(-i));
      assertEquals("for i=" + i, Integer.toString(i), replaceResult);
      assertEquals("for i=" + i, Integer.toString(-i), pr.get(Integer.toString(i)));
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    // unsuccessful replace(Object, Object)
    for (int i = end + 1; i <= end2; i++) {
      Object replaceResult =  pr.replace(Integer.toString(i),
                                         Integer.toString(-i));
      assertNull("Expected null, but got " + replaceResult + "for i=" + i,
                 replaceResult);
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    // successful replace(Object key, Object oldValue, Object newValue)
    for (int i = start; i <= end; i++) {
      boolean replaceResult =  pr.replace(Integer.toString(i),
                                          Integer.toString(-i),
                                          Integer.toString(i * 2));
      assertTrue("for i=" + i, replaceResult);
      assertEquals("for i=" + i, Integer.toString(i * 2), pr.get(Integer.toString(i)));
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    // unsuccessful replace(Object key, Object oldValue, Object newValue)
    for (int i = start; i <= end2; i++) {
      boolean replaceResult =  pr.replace(Integer.toString(i),
                                          Integer.toString(-i),
                                          Integer.toString(i * -4));
      assertFalse("for i=" + i, replaceResult);
      assertEquals("for i=" + i,
                   i <= end ? Integer.toString(i * 2) : null,
                   pr.get(Integer.toString(i)));
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    // test unsuccessful remove(key, value)
    for (int i = start; i <= end2; i++) {
      boolean removeResult =  pr.remove(Integer.toString(i),
                                        Integer.toString(-i));
      assertFalse("for i=" + i, removeResult);
      assertEquals("for i=" + i,
                   i <= end ? Integer.toString(i * 2) : null,
                   pr.get(Integer.toString(i)));
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", end, size);
    assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
                pr.isEmpty());
    
    
    // test successful remove(key, value)
    for (int i = start; i <= end; i++) {
      boolean removeResult =  pr.remove(Integer.toString(i),
                                        Integer.toString(i * 2));
      assertTrue("for i=" + i, removeResult);
      assertEquals("for i=" + i, null, pr.get(Integer.toString(i)));
    }
    size = pr.size();
    assertEquals("Size doesn't return expected value", 0, size);
    assertTrue("isEmpty doesnt return proper state of the PartitionedRegion", 
               pr.isEmpty());
  }
  
}
