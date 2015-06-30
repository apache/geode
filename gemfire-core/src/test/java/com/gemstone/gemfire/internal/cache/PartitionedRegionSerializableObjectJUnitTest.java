/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Following test create a region and threads which perform get and put
 * operations simutaneously on that single region. Object used while putting in
 * region is serializable.
 * 
 * @author gthombar
 */

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import dunit.DistributedTestCase;
import junit.framework.TestCase;

@Category(IntegrationTest.class)
public class PartitionedRegionSerializableObjectJUnitTest
{
  String regionName = "SerizableRegion";

  /** It is common region for all the threads */
  Region root;

  /**
   * It is map to store thread name and list of objects which are created by
   * that thread.
   */
  static Map thread2List = new HashMap();

  static int MAX_COUNT = 10;

  static int MAX_THREADS = 10;

  /**
   * This test creates a region and threads. This Region is common to all the
   * threads which perform get, put operations on that region. Object used
   * during these operations are serializable. key and value in the partition
   * region are same.
   * 
   */
  @Test
  public void testOperationsWithSerializableObject()
  {

    int localMaxMemory = 50;
    Thread threadArr[] = new Thread[10];
    root = PartitionedRegionTestHelper.createPartitionedRegion(regionName,
        String.valueOf(localMaxMemory), 0);
    System.out
        .println("*******testOperationsWithSerializableObject started*********");
    for (int i = 0; i < MAX_THREADS; i++) {
      putThread putObj = new putThread("PRSerializableObjectJUnitTest" + i);
      threadArr[i] = putObj;
    }

    for (int i = 0; i < MAX_THREADS; i++) {
      threadArr[i].start();
      DistributedTestCase.join(threadArr[i], 30 * 1000, null);
    }

    for (int i = 0; i < MAX_THREADS; i++) {
      new getThread("PRSerializableObjectJUnitTest" + i);
    }

    try {
      Thread.sleep(100);
    }
    catch (InterruptedException e) {
      root.getCache().getLogger().warning("Got Interrupted Exception in sleep.",e);
      fail("interrupted");
    }
    System.out
        .println("********testOperationsWithSerializableObject complited successfully********");
  }

  /**
   * This class creates thread that take list of the objects from thread2List
   * Map and performs get operation with these objects as key and verifying
   * result with itself because key and values are same in the partition Region
   * 
   */
  public class getThread extends Thread
  {
    getThread(String threadName) {
      super(threadName);
    }

    public void run()
    {
      Region pr = PartitionedRegionTestHelper
          .getExistingRegion(Region.SEPARATOR + regionName);
      assertNotNull(pr);
      List list = new ArrayList();
      list = (ArrayList)thread2List.get(this.getName());

      Iterator itr = list.iterator();

      while (itr.hasNext()) {
        Object listObj = itr.next();
        try {
          Object retObj = pr.get(listObj);
          assertNotNull(retObj);
          if (!listObj.equals(retObj)) {
            fail("PRSerializableObjectJUniTest:getThread() Object from the region is not equal to Object from the list");
          }
        }
        catch (Exception ex) {
          fail("PRSerializableObjectJUniTest:getThread() failed ");
        }
      }
    }
  }

  /**
   * This class create threads that put the serializable objects in the partion
   * region and also add the the list of the serializable objects to the
   * thread2List map
   * 
   */

  public class putThread extends Thread
  {
    putThread(String threadName) {
      super(threadName);
    }

    public void run()
    {
      Region pr = PartitionedRegionTestHelper
          .getExistingRegion(Region.SEPARATOR + regionName);
      assertNotNull(pr);
      int key = 0;
      Object obj = null;
      List list = new ArrayList();
      for (key = 0; key < MAX_COUNT; key++) {
        try {
          obj = PartitionedRegionTestHelper.createPRSerializableObject(this
              .getName()
              + key, key);
          pr.put(obj, obj);
          list.add(obj);
        }
        catch (Exception ex) {
          fail("PRSerializableObjectJUnitTest:putThread Got an incorrect exception for localMaxMemory=0 at count = "
              + key + ". Exception stack = " + ex);
        }
      }
      thread2List.put(this.getName(), list);
    }
  }
}
