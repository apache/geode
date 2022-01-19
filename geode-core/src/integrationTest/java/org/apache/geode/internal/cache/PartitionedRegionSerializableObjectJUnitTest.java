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

/**
 * Following test create a region and threads which perform get and put operations simutaneously on
 * that single region. Object used while putting in region is serializable.
 *
 */

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.ThreadUtils;

public class PartitionedRegionSerializableObjectJUnitTest {

  private final String regionName = "SerizableRegion";

  /** It is common region for all the threads */
  private Region root;

  /**
   * It is map to store thread name and list of objects which are created by that thread.
   */
  private static final Map thread2List = new HashMap();

  private static final int MAX_COUNT = 10;

  private static final int MAX_THREADS = 10;

  /**
   * This test creates a region and threads. This Region is common to all the threads which perform
   * get, put operations on that region. Object used during these operations are serializable. key
   * and value in the partition region are same.
   */
  @Test
  public void testOperationsWithSerializableObject() {
    int localMaxMemory = 50;
    Thread[] threadArr = new Thread[10];
    root = PartitionedRegionTestHelper.createPartitionedRegion(regionName,
        String.valueOf(localMaxMemory), 0);
    System.out.println("*******testOperationsWithSerializableObject started*********");
    for (int i = 0; i < MAX_THREADS; i++) {
      putThread putObj = new putThread("PRSerializableObjectJUnitTest" + i);
      threadArr[i] = putObj;
    }

    for (int i = 0; i < MAX_THREADS; i++) {
      threadArr[i].start();
      ThreadUtils.join(threadArr[i], 30 * 1000);
    }

    for (int i = 0; i < MAX_THREADS; i++) {
      new getThread("PRSerializableObjectJUnitTest" + i);
    }

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      root.getCache().getLogger().warning("Got Interrupted Exception in sleep.", e);
      fail("interrupted");
    }
    System.out
        .println("********testOperationsWithSerializableObject complited successfully********");
  }

  /**
   * This class creates thread that take list of the objects from thread2List Map and performs get
   * operation with these objects as key and verifying result with itself because key and values are
   * same in the partition Region
   */
  private class getThread extends Thread {

    getThread(String threadName) {
      super(threadName);
    }

    @Override
    public void run() {
      Region pr = PartitionedRegionTestHelper.getExistingRegion(SEPARATOR + regionName);
      assertNotNull(pr);
      List list = new ArrayList();
      list = (ArrayList) thread2List.get(getName());

      for (final Object listObj : list) {
        try {
          Object retObj = pr.get(listObj);
          assertNotNull(retObj);
          if (!listObj.equals(retObj)) {
            fail(
                "PRSerializableObjectJUniTest:getThread() Object from the region is not equal to Object from the list");
          }
        } catch (Exception ex) {
          fail("PRSerializableObjectJUniTest:getThread() failed ");
        }
      }
    }
  }

  /**
   * This class create threads that put the serializable objects in the partion region and also add
   * the the list of the serializable objects to the thread2List map
   */
  private class putThread extends Thread {

    putThread(String threadName) {
      super(threadName);
    }

    @Override
    public void run() {
      Region pr = PartitionedRegionTestHelper.getExistingRegion(SEPARATOR + regionName);
      assertNotNull(pr);
      int key = 0;
      Object obj = null;
      List list = new ArrayList();
      for (key = 0; key < MAX_COUNT; key++) {
        try {
          obj = PartitionedRegionTestHelper.createPRSerializableObject(getName() + key, key);
          pr.put(obj, obj);
          list.add(obj);
        } catch (Exception ex) {
          fail(
              "PRSerializableObjectJUnitTest:putThread Got an incorrect exception for localMaxMemory=0 at count = "
                  + key + ". Exception stack = " + ex);
        }
      }
      thread2List.put(getName(), list);
    }
  }
}
