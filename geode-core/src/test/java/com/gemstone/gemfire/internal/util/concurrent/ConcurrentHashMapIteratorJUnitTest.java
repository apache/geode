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
package com.gemstone.gemfire.internal.util.concurrent;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Category(IntegrationTest.class)
public class ConcurrentHashMapIteratorJUnitTest extends TestCase {

  public void test() throws InterruptedException {
    
    //Apparently, we need a distributed system to create
    //this CHM, because it's locks use DS properties.
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
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
