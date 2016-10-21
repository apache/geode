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
package org.apache.geode.internal.cache.tier;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.tier.sockets.DeltaEOFException;
import org.apache.geode.internal.cache.tier.sockets.FaultyDelta;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Test delta propagation for faulty delta implementation
 * 
 * @since GemFire 6.1
 */
@Category(DistributedTest.class)
public class Bug40396DUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache;
  private static final String REGION_NAME = "Bug40396DUnitTest_region";

  private static final String END_OF_FILE_EX = "eofe";
  private static final String ARRAY_INDEX_OUT_BOUND_EX = "aiob";

  private static int counter;

  private VM server;
  private VM server2;

  private static final int PUT_COUNT = 10;

  public Bug40396DUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    server2 = host.getVM(2);
  }

  /*
   * create server cache
   */
  public static Integer createServerCache() throws Exception {
    new Bug40396DUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.getAttributesMutator().setCloningEnabled(false);
    CacheServer server = cache.addCacheServer();
    addExceptions();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }

  public static void addExceptions() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger()
          .info("<ExpectedException action=add>" + "java.io.EOFException" + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>"
          + "java.lang.ArrayIndexOutOfBoundsException" + "</ExpectedException>");
    }
  }

  public static void removeExceptions() {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info(
          "<ExpectedException action=remove>" + "java.io.EOFException" + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>"
          + "java.lang.ArrayIndexOutOfBoundsException" + "</ExpectedException>");
    }
  }

  /*
   * create cache with properties
   */
  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static Exception putDelta(String regName, String type) {
    Region reg = cache.getRegion(Region.SEPARATOR + regName);
    try {
      if (type.equals(END_OF_FILE_EX)) {
        DeltaEOFException obj = new DeltaEOFException();
        for (int i = 0; i < PUT_COUNT; i++) {
          obj.setIntVal(i);
          obj.setBigObj(new byte[] {(byte) (i + 3), (byte) (i + 3)});
          reg.put("key", obj);
        }
      } else if (type.equals(ARRAY_INDEX_OUT_BOUND_EX)) {
        FaultyDelta obj = new FaultyDelta();
        for (int i = 0; i < PUT_COUNT; i++) {
          obj.setIntVal(i);
          obj.setBigObj(new byte[] {(byte) (i + 3), (byte) (i + 3)});
          reg.put("key", obj);
        }
      }
    } catch (Exception ex) {
      return ex;
    }
    // this make tests fail
    return new Exception();
  }

  /**
   * This test does the following 1)send faulty implementation (Reading more in fromDelta then what
   * sent by toDelta) of delta raises EOF exception<br>
   */
  @Test
  public void testForFaultyDeltaImplementationForEOFEX() {
    boolean matched = false;
    ((Integer) server.invoke(() -> Bug40396DUnitTest.createServerCache())).intValue();
    ((Integer) server2.invoke(() -> Bug40396DUnitTest.createServerCache())).intValue();
    Exception xp =
        (Exception) server.invoke(() -> Bug40396DUnitTest.putDelta(REGION_NAME, END_OF_FILE_EX));
    StackTraceElement[] st = xp.getCause().getStackTrace();
    matched = getMatched(st);

    assertTrue("pattern not found", matched);
  }

  private boolean getMatched(StackTraceElement[] ste) {
    boolean mched = false;
    for (int i = 0; i < ste.length; i++) {
      if (mched)
        break;
      if (ste[i].toString().indexOf("fromDelta") != -1)
        mched = true;
    }
    return mched;
  }

  /**
   * This test does the following 1)send faulty implementation when reading incorrect order from
   * toDelta, raises delta raises array index out of bound exception<br>
   */
  @Test
  public void testForFaultyDeltaImplementationForAIOBEX() {
    boolean matched = false;
    ((Integer) server.invoke(() -> Bug40396DUnitTest.createServerCache())).intValue();
    ((Integer) server2.invoke(() -> Bug40396DUnitTest.createServerCache())).intValue();
    Exception xp = (Exception) server
        .invoke(() -> Bug40396DUnitTest.putDelta(REGION_NAME, ARRAY_INDEX_OUT_BOUND_EX));

    StackTraceElement[] st = xp.getStackTrace();
    matched = getMatched(st);

    assertTrue("pattern not found", matched);
  }

  @Override
  public final void preTearDown() throws Exception {
    // then close the servers
    server.invoke(() -> Bug40396DUnitTest.removeExceptions());
    server.invoke(() -> Bug40396DUnitTest.closeCache());
    server2.invoke(() -> Bug40396DUnitTest.closeCache());
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        cache = null;
      }
    });
  }
}
