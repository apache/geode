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
package com.gemstone.gemfire.internal.cache.tier;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.tier.sockets.DeltaEOFException;
import com.gemstone.gemfire.internal.cache.tier.sockets.FaultyDelta;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
/**
 * Test delta propagation for faulty delta implementation
 * @author aingle
 * @since 6.1
 */
public class Bug40396DUnitTest extends DistributedTestCase {
  
  private static Cache cache;
  private static final String REGION_NAME="Bug40396DUnitTest_region";
  
  private static final String END_OF_FILE_EX = "eofe";
  private static final String ARRAY_INDEX_OUT_BOUND_EX = "aiob";
  
  private static int counter;
  
  private VM server;
  private VM server2;

  private static final int PUT_COUNT = 10;
  
  public Bug40396DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    server2 = host.getVM(2);
  }
  
  /*
   * create server cache
   */
  public static Integer createServerCache() throws Exception
  {
    new Bug40396DUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    region.getAttributesMutator().setCloningEnabled(false);
    CacheServer server = cache.addCacheServer();
    addExceptions();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }
  
  public static void addExceptions() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info(
          "<ExpectedException action=add>" + "java.io.EOFException"
              + "</ExpectedException>");
      cache.getLogger().info(
          "<ExpectedException action=add>" + "java.lang.ArrayIndexOutOfBoundsException"
              + "</ExpectedException>");
    }
  }

  public static void removeExceptions() {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info(
          "<ExpectedException action=remove>" + "java.io.EOFException"
              + "</ExpectedException>");
      cache.getLogger().info(
          "<ExpectedException action=remove>"
              + "java.lang.ArrayIndexOutOfBoundsException" + "</ExpectedException>");
    }
  }
  
  /*
   * create cache with properties
   */
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void closeCache()
  {
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
          obj.setBigObj(new byte[] { (byte)(i + 3), (byte)(i + 3) });
          reg.put("key", obj);
        }
      }
      else if (type.equals(ARRAY_INDEX_OUT_BOUND_EX)) {
        FaultyDelta obj = new FaultyDelta();
        for (int i = 0; i < PUT_COUNT; i++) {
          obj.setIntVal(i);
          obj.setBigObj(new byte[] { (byte)(i + 3), (byte)(i + 3) });
          reg.put("key", obj);
        }
      }
    }
    catch (Exception ex) {
      return ex;
    }
    // this make tests fail
    return new Exception();
  }
    
  /**
   * This test does the following 1)send faulty implementation (Reading more in
   * fromDelta then what sent by toDelta) of delta raises EOF exception<br>
   */
  public void testForFaultyDeltaImplementationForEOFEX() {
    boolean matched = false;
    ((Integer)server.invoke(Bug40396DUnitTest.class, "createServerCache")).intValue();
    ((Integer)server2.invoke(Bug40396DUnitTest.class, "createServerCache")).intValue();
    Exception xp = (Exception)server.invoke(Bug40396DUnitTest.class,
        "putDelta", new Object[] { REGION_NAME, END_OF_FILE_EX });
    StackTraceElement[] st = xp.getCause().getStackTrace();
    matched = getMatched(st);

    assertTrue("pattern not found", matched);
  }
  
  private boolean getMatched(StackTraceElement[] ste){
    boolean mched = false;
    for(int i =0 ; i< ste.length; i++){
      if(mched)
        break;
      if (ste[i].toString().indexOf("fromDelta")!= -1)
        mched = true;
    }
    return mched;
  }
  
  /**
   * This test does the following 1)send faulty implementation when reading
   * incorrect order from toDelta, raises delta raises array index out of bound
   * exception<br>
   */
  public void testForFaultyDeltaImplementationForAIOBEX() {
    boolean matched = false;
    ((Integer)server.invoke(Bug40396DUnitTest.class, "createServerCache")).intValue();
    ((Integer)server2.invoke(Bug40396DUnitTest.class, "createServerCache")).intValue();
    Exception xp = (Exception) server.invoke(Bug40396DUnitTest.class, "putDelta", new Object[] {
          REGION_NAME, ARRAY_INDEX_OUT_BOUND_EX });
    
    StackTraceElement[] st = xp.getStackTrace();
    matched = getMatched(st);
    
    assertTrue("pattern not found", matched);
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    // then close the servers
    server.invoke(Bug40396DUnitTest.class, "removeExceptions");
    server.invoke(Bug40396DUnitTest.class, "closeCache");
    server2.invoke(Bug40396DUnitTest.class, "closeCache");
    cache = null;
    invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }
}
