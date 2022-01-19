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
/*
 * ClearGlobalDUnitTest.java
 *
 * Created on September 13, 2005, 1:47 PM
 */

package org.apache.geode.internal.cache;

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * For the Global region subsequent puts should be blocked until the clear operation is completely
 * done
 */

public class ClearGlobalDUnitTest extends JUnit4DistributedTestCase {

  static VM server1 = null;

  static Cache cache;

  static Properties props = new Properties();

  static DistributedSystem ds = null;

  static Region region;

  static boolean testFailed = true;

  static StringBuilder exceptionMsg = new StringBuilder();

  static boolean testComplete = false;

  static Object lock = new Object();

  private static CacheObserver origObserver;

  /** name of the test region */
  private static final String REGION_NAME = "ClearGlobalDUnitTest_Region";

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server1.invoke(ClearGlobalDUnitTest::createCacheServer1);
    createCacheServer2();
    LogWriterUtils.getLogWriter().fine("Cache created in successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    server1.invoke(ClearGlobalDUnitTest::closeCache);
    resetClearCallBack();
    closeCache();
  }

  public static void resetClearCallBack() {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(origObserver);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Test
  public void testClearGlobalMultiVM() throws Exception {
    Object[] objArr = new Object[1];
    for (int i = 1; i < 4; i++) {
      objArr[0] = "" + i;
      server1.invoke(ClearGlobalDUnitTest.class, "putMethod", objArr);
    }
    server1.invoke(ClearGlobalDUnitTest::clearMethod);
    checkTestResults();

  }// end of test case

  public static void createCacheServer1() throws Exception {
    ds = (new ClearGlobalDUnitTest()).getSystem(props);
    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    RegionAttributes attr = factory.create();
    region = cache.createRegion(REGION_NAME, attr);

  } // end of create cache for VM0

  public static void createCacheServer2() throws Exception {
    ds = (new ClearGlobalDUnitTest()).getSystem(props);
    CacheObserverImpl observer = new CacheObserverImpl();
    origObserver = CacheObserverHolder.setInstance(observer);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    RegionAttributes attr = factory.create();
    region = cache.createRegion(REGION_NAME, attr);
    cache.setLockTimeout(3);

  } // end of create cache for VM1

  public static void putMethod(Object ob) throws Exception {
    if (ob != null) {
      String str = "first";
      region.put(ob, str);
    }
  }

  public static void clearMethod() throws Exception {
    region.clear();
  }

  public static void checkTestResults() throws Exception {
    synchronized (lock) {
      while (!testComplete) {
        try {
          lock.wait(5000);
        } catch (InterruptedException ex) {
          fail("interrupted");
        }
      }
    }

    if (testFailed) {
      throw new Exception("Test Failed: " + exceptionMsg);
    } else {
      LogWriterUtils.getLogWriter().info("Test Passed Successfully ");
    }
  }

  public static class CacheObserverImpl extends CacheObserverAdapter {
    @Override
    public void afterRegionClear(RegionEvent event) {
      Thread th = new PutThread();
      th.start();
      ThreadUtils.join(th, 5 * 60 * 1000);
      synchronized (lock) {
        testComplete = true;
        lock.notify();
      }

    }
  }// end of CacheObserverImpl

  static class PutThread extends Thread {
    @Override
    public void run() {
      try {
        region.put("ClearGlobal", "PutThread");
        exceptionMsg.append(" Put operation should wait for clear operation to complete ");
      } catch (TimeoutException ex) {
        // pass
        testFailed = false;
        LogWriterUtils.getLogWriter().info("Expected TimeoutException in thread ");
      } catch (Exception ex) {
        exceptionMsg.append(" Exception occurred while region.put(key,value)");
      }
    }
  }// end of PutThread
}// End of class
