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
 * ClearDAckDUnitTest.java
 *
 * Created on September 6, 2005, 2:57 PM
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class ClearDAckDUnitTest extends JUnit4DistributedTestCase { // TODO: reformat

  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static Region paperWork;
  static CacheTransactionManager cacheTxnMgr;
  static boolean IsAfterClear = false;
  static boolean flag = false;
  DistributedMember vm0ID, vm1ID;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0ID = vm0.invoke(ClearDAckDUnitTest::createCacheVM0);
    vm1ID = vm1.invoke(ClearDAckDUnitTest::createCacheVM1);
    LogWriterUtils.getLogWriter().info("Cache created in successfully");
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    vm0.invoke(ClearDAckDUnitTest::closeCache);
    vm1.invoke(ClearDAckDUnitTest::resetClearCallBack);
    vm1.invoke(ClearDAckDUnitTest::closeCache);
    vm2.invoke(ClearDAckDUnitTest::closeCache);
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        cache = null;
      }
    });
  }

  public static long getRegionVersion(DistributedMember memberID) {
    return ((LocalRegion) region).getVersionVector().getVersionForMember((VersionSource) memberID);
  }

  private static CacheObserver origObserver;

  public static void resetClearCallBack() {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(origObserver);
  }

  public static DistributedMember createCacheVM0() {
    try {
      // props.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "1234");
      // ds = DistributedSystem.connect(props);
      LogWriterUtils.getLogWriter().info("I am vm0");
      ds = (new ClearDAckDUnitTest()).getSystem(props);
      cache = CacheFactory.create(ds);

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      // [bruce]introduces bad race condition if mcast is used, so
      // the next line is disabled
      // factory.setEarlyAck(true);
      // DistributedSystem.setThreadsSocketPolicy(false);
      RegionAttributes attr = factory.create();

      region = cache.createRegion("map", attr);
      LogWriterUtils.getLogWriter().info("vm0 map region: " + region);
      paperWork = cache.createRegion("paperWork", attr);
      return cache.getDistributedSystem().getDistributedMember();
    } catch (CacheException ex) {
      throw new RuntimeException("createCacheVM0 exception", ex);
    }
  } // end of create cache for VM0

  public static DistributedMember createCacheVM1() {
    try {
      // props.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "1234");
      // ds = DistributedSystem.connect(props);
      LogWriterUtils.getLogWriter().info("I am vm1");
      ds = (new ClearDAckDUnitTest()).getSystem(props);
      // DistributedSystem.setThreadsSocketPolicy(false);
      CacheObserverImpl observer = new CacheObserverImpl();
      origObserver = CacheObserverHolder.setInstance(observer);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      cache = CacheFactory.create(ds);

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);

      RegionAttributes attr = factory.create();

      region = cache.createRegion("map", attr);
      LogWriterUtils.getLogWriter().info("vm1 map region: " + region);
      paperWork = cache.createRegion("paperWork", attr);
      return cache.getDistributedSystem().getDistributedMember();

    } catch (CacheException ex) {
      throw new RuntimeException("createCacheVM1 exception", ex);
    }
  }

  public static void createCacheVM2AndLocalClear() {
    try {
      // props.setProperty(DistributionConfig.SystemConfigurationProperties.MCAST_PORT, "1234");
      // ds = DistributedSystem.connect(props);
      LogWriterUtils.getLogWriter().info("I am vm2");
      ds = (new ClearDAckDUnitTest()).getSystem(props);
      // DistributedSystem.setThreadsSocketPolicy(false);
      CacheObserverImpl observer = new CacheObserverImpl();
      origObserver = CacheObserverHolder.setInstance(observer);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      cache = CacheFactory.create(ds);

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.NORMAL);

      RegionAttributes attr = factory.create();

      region = cache.createRegion("map", attr);
      LogWriterUtils.getLogWriter().info("vm2 map region: " + region);
      paperWork = cache.createRegion("paperWork", attr);

      region.put("vm2Key", "vm2Value");
      region.localClear();

    } catch (CacheException ex) {
      throw new RuntimeException("createCacheVM1 exception", ex);
    }
  }

  public static void closeCache() {
    try {
      cache.close();
      ds.disconnect();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  // test methods


  @Test
  public void testClearMultiVM() {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    Object[] objArr = new Object[1];
    for (int i = 1; i < 4; i++) {
      objArr[0] = "" + i;
      vm0.invoke(ClearDAckDUnitTest.class, "putMethod", objArr);

    }
    LogWriterUtils.getLogWriter().info("Did all puts successfully");

    long regionVersion = vm1.invoke(() -> ClearDAckDUnitTest.getRegionVersion(vm0ID));

    vm0.invoke(ClearDAckDUnitTest::clearMethod);

    boolean flag = vm1.invoke(ClearDAckDUnitTest::getVM1Flag);
    LogWriterUtils.getLogWriter().fine("Flag in VM1=" + flag);

    assertTrue(flag);

    long newRegionVersion = vm1.invoke(() -> ClearDAckDUnitTest.getRegionVersion(vm0ID));
    assertEquals("expected clear() to increment region version by 1 for " + vm0ID,
        regionVersion + 1, newRegionVersion);

    // test that localClear does not distribute
    VM vm2 = host.getVM(2);
    vm2.invoke(ClearDAckDUnitTest::createCacheVM2AndLocalClear);

    flag = vm1.invoke(ClearDAckDUnitTest::getVM1Flag);
    LogWriterUtils.getLogWriter().fine("Flag in VM1=" + flag);
    assertFalse(flag);

  }// end of test case

  public static Object putMethod(Object ob) {
    Object obj = null;
    // try{
    if (ob != null) {
      String str = "first";
      obj = region.put(ob, str);
    }
    // }catch(Exception ex){
    // ex.printStackTrace();
    // fail("Failed while region.put");
    // }
    return obj;
  }


  // public static boolean clearMethod(){
  public static void clearMethod() {
    try {

      long start = System.currentTimeMillis();
      region.clear();
      long end = System.currentTimeMillis();

      long diff = end - start;
      LogWriterUtils.getLogWriter().info(
          "Clear Thread proceeded before receiving the ack message in (milli seconds): " + diff);

    } catch (Exception e) {
      e.printStackTrace();
    }

  }


  public static class CacheObserverImpl extends CacheObserverAdapter {

    @Override
    public void afterRegionClear(RegionEvent event) {
      IsAfterClear = true;
    }

  }

  public static boolean getVM1Flag() {
    boolean result = IsAfterClear;
    IsAfterClear = false;
    return result;
  }



}// end of test class
