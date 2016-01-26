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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * TODO This doesn't really test the optimised RI behaviour but only that RI
 * works. But there must be other tests doing the same.
 * 
 * @author ashetkar
 * 
 */
@SuppressWarnings("serial")
public class Bug43684DUnitTest extends DistributedTestCase {

  private static final String REGION_NAME = "Bug43684DUnitTest";

  private static GemFireCacheImpl cache;

  private static Host host;

  private static VM server1;

  private static VM server2;

  private static VM server3;

  private static VM client1;

  private static int numBuckets = 11;

  public Bug43684DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    client1 = host.getVM(3);
    addExpectedException("Connection refused: connect");
  }

  public void tearDown2() throws Exception {
    closeCache();
    client1.invoke(Bug43684DUnitTest.class, "closeCache");
    server1.invoke(Bug43684DUnitTest.class, "closeCache");
    server2.invoke(Bug43684DUnitTest.class, "closeCache");
    server3.invoke(Bug43684DUnitTest.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
    DistributedTestCase.disconnectFromDS();
  }

  public void testRIWithSingleKeyOnRR()  throws Exception {
    doRegisterInterest("KEY_1", null, numBuckets, true, false);
  }

  public void testRIWithAllKeysOnRR()  throws Exception {
    doRegisterInterest(null, null, numBuckets, true, false);
  }

  public void testRIWithKeyListOnRR()  throws Exception {
    ArrayList<String> riKeys = new ArrayList<String>();
    riKeys.add("KEY_0");
    riKeys.add("KEY_1");
    riKeys.add("KEY_2");
    riKeys.add("KEY_5");
    riKeys.add("KEY_6");
    riKeys.add("KEY_7");
    riKeys.add("KEY_9");
    
    doRegisterInterest(riKeys, null, numBuckets, true, false);
  }

  public void testRIWithRegularExpressionOnRR()  throws Exception{
    doRegisterInterest(null, "^[X][_].*", numBuckets, true, false);
  }

  public void testRIWithSingleKeyOnPR()  throws Exception {
    doRegisterInterest("KEY_1", null);
  }

  public void testRIWithAllKeysOnPR()  throws Exception {
    doRegisterInterest(null, null);
  }

  public void testRIWithKeyListOnPR()  throws Exception {
    ArrayList<String> riKeys = new ArrayList<String>();
    riKeys.add("KEY_0");
    riKeys.add("KEY_1");
    riKeys.add("KEY_2");
    riKeys.add("KEY_5");
    riKeys.add("KEY_6");
    riKeys.add("KEY_7");
    riKeys.add("KEY_9");
    
    doRegisterInterest(riKeys, null);
  }

  public void testRIWithRegularExpressionOnPR()  throws Exception{
    doRegisterInterest(null, "^[X][_].*");
  }

  public void testRIWithMoreEntriesOnPR()  throws Exception{
    doRegisterInterest(null, null, 5147, false, false);
  }

  public void testRIWithSingleKeyOnEmptyPrimaryOnPR()  throws Exception {
    doRegisterInterest("KEY_1", null, numBuckets, false, true);
  }

  public void testRIWithAllKeysOnEmptyPrimaryOnPR()  throws Exception {
    doRegisterInterest(null, null, numBuckets, false, true);
  }

  public void testRIWithKeyListOnEmptyPrimaryOnPR()  throws Exception {
    ArrayList<String> riKeys = new ArrayList<String>();
    riKeys.add("KEY_0");
    riKeys.add("KEY_1");
    riKeys.add("KEY_2");
    riKeys.add("KEY_5");
    riKeys.add("KEY_6");
    riKeys.add("KEY_7");
    riKeys.add("KEY_9");
    
    doRegisterInterest(riKeys, null, numBuckets, false, true);
  }

  public void testRIWithRegularExpressionOnEmptyPrimaryOnPR()  throws Exception{
    doRegisterInterest(null, "^[X][_].*", numBuckets, false, true);
  }

  public void testNativeClientIssueOnPR()  throws Exception{
    ArrayList<String> riKeys = new ArrayList<String>();
    riKeys.add("OPKEY_0");
    riKeys.add("OPKEY_1");
    riKeys.add("OPKEY_2");
    riKeys.add("OPKEY_3");
    riKeys.add("OPKEY_4");
    riKeys.add("OPKEY_5");
    riKeys.add("OPKEY_6");
    riKeys.add("OPKEY_7");
    riKeys.add("OPKEY_8");
    riKeys.add("OPKEY_9");
    riKeys.add("OPKEY_10");
    riKeys.add("OPKEY_11");
    riKeys.add("OPKEY_12");
    riKeys.add("OPKEY_13");
    doRegisterInterest2(riKeys, false, false);
  }

  private void doRegisterInterest(Object keys, String regEx) throws Exception {
    doRegisterInterest(keys, regEx, numBuckets, false, false);
  }

  @SuppressWarnings("rawtypes")
  private void doRegisterInterest(Object keys, String regEx, Integer numOfPuts, Boolean isReplicated, Boolean isPrimaryEmpty) throws Exception {
    int port1 = (Integer)server1.invoke(Bug43684DUnitTest.class, "createServerCache", new Object[]{isReplicated, isPrimaryEmpty});
    server2.invoke(Bug43684DUnitTest.class, "createServerCache", new Object[]{isReplicated, false});
    server3.invoke(Bug43684DUnitTest.class, "createServerCache", new Object[]{isReplicated, false});

    int regexNum = 20;
    server1.invoke(Bug43684DUnitTest.class, "doPuts", new Object[]{numOfPuts, regEx, regexNum});

    client1.invoke(Bug43684DUnitTest.class, "createClientCache", new Object[] {host, port1});
    client1.invoke(Bug43684DUnitTest.class, "registerInterest", new Object[] {keys, regEx});

    server1.invoke(Bug43684DUnitTest.class, "closeCache");
    int size = keys != null ? (keys instanceof List ? ((List)keys).size() : 1) : regEx == null ? numOfPuts : regexNum;
    client1.invoke(Bug43684DUnitTest.class, "verifyResponse", new Object[]{size});
  }

  @SuppressWarnings("rawtypes")
  private void doRegisterInterest2(Object keys, Boolean isReplicated, Boolean isPrimaryEmpty) throws Exception {
    int port1 = (Integer)server1.invoke(Bug43684DUnitTest.class, "createServerCache", new Object[]{isReplicated, isPrimaryEmpty});
    server2.invoke(Bug43684DUnitTest.class, "createServerCache", new Object[]{isReplicated, false});
    server3.invoke(Bug43684DUnitTest.class, "createServerCache", new Object[]{isReplicated, false});

    client1.invoke(Bug43684DUnitTest.class, "createClientCache", new Object[] {host, port1});
    createClientCache(host, port1);
    doOps();

    client1.invoke(Bug43684DUnitTest.class, "registerInterest", new Object[] {keys, null});
    client1.invoke(Bug43684DUnitTest.class, "verifyResponse2");
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(false, false);
  }

  @SuppressWarnings("rawtypes")
  public static Integer createServerCache(Boolean isReplicated, Boolean isPrimaryEmpty) throws Exception {
    DistributedTestCase.disconnectFromDS();
    Properties props = new Properties();
    props.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
//    props.setProperty("log-file", "server_" + OSProcess.getId() + ".log");
//    props.setProperty("log-level", "fine");
    props.setProperty("statistic-archive-file", "server_" + OSProcess.getId()
        + ".gfs");
    props.setProperty("statistic-sampling-enabled", "true");
    CacheFactory cf = new CacheFactory(props);
    cache = (GemFireCacheImpl)cf.create();

    RegionFactory rf;
    if (isReplicated) {
      RegionShortcut rs = isPrimaryEmpty ? RegionShortcut.REPLICATE_PROXY : RegionShortcut.REPLICATE;
      rf = cache.createRegionFactory(rs);
    } else {
      RegionShortcut rs = isPrimaryEmpty ? RegionShortcut.PARTITION_PROXY : RegionShortcut.PARTITION;
      rf = cache.createRegionFactory(rs);
      rf.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(numBuckets).create());
    }
    rf.create(REGION_NAME);
    CacheServerImpl server = (CacheServerImpl)cache.addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void createClientCache(Host host, Integer port) {
    DistributedTestCase.disconnectFromDS();
    Properties props = new Properties();
//    props.setProperty("log-file", "client_" + OSProcess.getId() + ".log");
//    props.setProperty("log-level", "fine");
    props.setProperty("statistic-archive-file", "client_" + OSProcess.getId()
        + ".gfs");
    props.setProperty("statistic-sampling-enabled", "true");
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.addPoolServer(host.getHostName(), port).setPoolSubscriptionEnabled(true);
    cache = (GemFireCacheImpl)ccf.create();
    ClientRegionFactory crf = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    crf.create(REGION_NAME);
  }
     
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void registerInterest(Object keys, String regEx) {
    Region region = cache.getRegion(REGION_NAME);
    if (keys == null && regEx == null) {
      region.registerInterest("ALL_KEYS");
    } else if (keys != null) {
      region.registerInterest(keys);
      region.registerInterest("UNKNOWN_KEY");
    } else if (regEx != null) {
      region.registerInterestRegex(regEx);
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void doPuts(Integer num, String regex, int regexNum) throws Exception {
    Region r = cache.getRegion(REGION_NAME);
    for (int i = 0; i < num; i++) {
      r.create("KEY_" + i, "VALUE__" + i);
    }
    if (regex != null) {
      for (int i = 0; i < regexNum; i++) {
        r.create("X_KEY_" + i, "X_VALUE__" + i);
      }
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void doOps() throws Exception {
    Region r = cache.getRegion(REGION_NAME);
    for (int i = 0; i < 14; i++) {
      r.create("OPKEY_" + i, "OPVALUE__" + i);
    }
    
    for (int i = 7; i < 14; i++) {
      r.destroy("OPKEY_" + i);
    }
  }

  public static void verifyResponse2() throws Exception {
    LocalRegion r = (LocalRegion)cache.getRegion(REGION_NAME);
    for (int i = 0; i < 7; i++) {
      assertTrue(r.containsKey("OPKEY_" + i));
    }
    for (int i = 7; i < 14; i++) {
      assertFalse(r.containsKey("OPKEY_" + i));
      assertTrue(r.containsTombstone("OPKEY_" + i));
    }
  }

  public static void verifyResponse(Integer size) throws Exception {
    LocalRegion r = (LocalRegion)cache.getRegion(REGION_NAME);
    assertEquals(size.intValue(), r.size());
  }

  public static void getLocally() throws Exception {
    LocalRegion r = (LocalRegion)cache.getRegion(REGION_NAME);
    for (int i = 0; i < numBuckets; i++) {
      RegionEntry e = r.getRegionEntry("KEY_" + i);
      cache.getLoggerI18n().info(LocalizedStrings.DEBUG, e._getValue());
    }
  }
}
