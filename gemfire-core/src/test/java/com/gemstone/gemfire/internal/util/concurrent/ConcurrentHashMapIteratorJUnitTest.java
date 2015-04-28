/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.junit.IntegrationTest;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Category(IntegrationTest.class)
public class ConcurrentHashMapIteratorJUnitTest extends TestCase {

  public void test() throws InterruptedException {
    
    //Apparently, we need a distributed system to create
    //this CHM, because it's locks use DS properties.
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    DistributedSystem.connect(props);
    java.util.concurrent.ConcurrentHashMap baselineMap = new java.util.concurrent.ConcurrentHashMap();
    CustomEntryConcurrentHashMap testMap = new CustomEntryConcurrentHashMap();
    Map initialSet;
    

    createBaseline(baselineMap, testMap, 0, 100);
    assertEquals(baselineMap, testMap);
    initialSet = new HashMap(baselineMap);
    
//    putter = new Putter(baselineMap, testMap, 1000, 2000);
//    putter.run();
    
    
    RandomMutations randomer = new RandomMutations(baselineMap, testMap, 1001, 50000);
    randomer.start();
    
    
    for(int i = 0; i < 1000; i++) {
      checkForInitialSet(i, testMap, initialSet);
    }
    
    randomer.cancel();
    
    assertEquals(baselineMap, testMap);
  }

  private void checkForInitialSet(int i, ConcurrentMap testMap, Map initialSet) {
    HashSet found = new HashSet(testMap.values());
    if(!found.containsAll(initialSet.values())) {
      HashSet missed = new HashSet(initialSet.values());
      missed.removeAll(found);
      fail("On run " + i + " did not find these elements of the initial set using the iterator " + missed);
    }
  }
  
  public void createBaseline(ConcurrentMap baselineMap, ConcurrentMap testMap, 
      int start, int end) {
    for(int i = start; i < end; i++) {
      baselineMap.put(i, i);
      testMap.put(i, i);
    }
  }
  
  public static class RandomMutations extends Thread {
    private final ConcurrentMap baselineMap;
    private final ConcurrentMap testMap;
    private int start;
    private int end;
    private volatile boolean done;
    
    
    public RandomMutations(ConcurrentMap baselineMap, ConcurrentMap testMap, int start, int end) {
      this.baselineMap = baselineMap;
      this.testMap = testMap;
      this.start = start;
      this.end = end;
    }

    public void run() {
      Random random = new Random();
      while(!done) {
        int key = random.nextInt(end - start) + start;
        boolean put = random.nextBoolean();
        if(put) {
          baselineMap.put(key,key);
          testMap.put(key, key);
        } else {
          baselineMap.remove(key);
          testMap.remove(key);
        }
      }
    }
    
    public void cancel() throws InterruptedException {
      this.done = true;
      this.join();
    }
  }
     

}
