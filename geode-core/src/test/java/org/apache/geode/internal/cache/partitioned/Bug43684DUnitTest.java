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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * TODO This doesn't really test the optimised RI behaviour but only that RI
 * works. But there must be other tests doing the same.
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class Bug43684DUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = Bug43684DUnitTest.class.getSimpleName();

  private static GemFireCacheImpl cache;

  private Host host;

  private static VM server1;

  private static VM server2;

  private static VM server3;

  private static VM client1;

  private static int numBuckets = 11;

  @Override
  public final void postSetUp() throws Exception {
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    client1 = host.getVM(3);
    IgnoredException.addIgnoredException("Connection refused: connect");
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    client1.invoke(() -> Bug43684DUnitTest.closeCache());
    server1.invoke(() -> Bug43684DUnitTest.closeCache());
    server2.invoke(() -> Bug43684DUnitTest.closeCache());
    server3.invoke(() -> Bug43684DUnitTest.closeCache());
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
    disconnectFromDS();
  }

  @Test
  public void testRIWithSingleKeyOnRR()  throws Exception {
    doRegisterInterest("KEY_1", null, numBuckets, true, false);
  }

  @Test
  public void testRIWithAllKeysOnRR()  throws Exception {
    doRegisterInterest(null, null, numBuckets, true, false);
  }

  @Test
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

  @Test
  public void testRIWithRegularExpressionOnRR()  throws Exception{
    doRegisterInterest(null, "^[X][_].*", numBuckets, true, false);
  }

  @Test
  public void testRIWithSingleKeyOnPR()  throws Exception {
    doRegisterInterest("KEY_1", null);
  }

  @Test
  public void testRIWithAllKeysOnPR()  throws Exception {
    doRegisterInterest(null, null);
  }

  @Test
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

  @Test
  public void testRIWithRegularExpressionOnPR()  throws Exception{
    doRegisterInterest(null, "^[X][_].*");
  }

  @Test
  public void testRIWithMoreEntriesOnPR()  throws Exception{
    doRegisterInterest(null, null, 5147, false, false);
  }

  @Test
  public void testRIWithSingleKeyOnEmptyPrimaryOnPR()  throws Exception {
    doRegisterInterest("KEY_1", null, numBuckets, false, true);
  }

  @Test
  public void testRIWithAllKeysOnEmptyPrimaryOnPR()  throws Exception {
    doRegisterInterest(null, null, numBuckets, false, true);
  }

  @Test
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

  @Test
  public void testRIWithRegularExpressionOnEmptyPrimaryOnPR()  throws Exception{
    doRegisterInterest(null, "^[X][_].*", numBuckets, false, true);
  }

  @Test
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
    int port1 = (Integer)server1.invoke(() -> Bug43684DUnitTest.createServerCache(isReplicated, isPrimaryEmpty));
    server2.invoke(() -> Bug43684DUnitTest.createServerCache(isReplicated, false));
    server3.invoke(() -> Bug43684DUnitTest.createServerCache(isReplicated, false));

    int regexNum = 20;
    server1.invoke(() -> Bug43684DUnitTest.doPuts(numOfPuts, regEx, regexNum));

    client1.invoke(() -> Bug43684DUnitTest.createClientCache(host, port1));
    client1.invoke(() -> Bug43684DUnitTest.registerInterest(keys, regEx));

    server1.invoke(() -> Bug43684DUnitTest.closeCache());
    int size = keys != null ? (keys instanceof List ? ((List)keys).size() : 1) : regEx == null ? numOfPuts : regexNum;
    client1.invoke(() -> Bug43684DUnitTest.verifyResponse(size));
  }

  @SuppressWarnings("rawtypes")
  private void doRegisterInterest2(Object keys, Boolean isReplicated, Boolean isPrimaryEmpty) throws Exception {
    int port1 = (Integer)server1.invoke(() -> Bug43684DUnitTest.createServerCache(isReplicated, isPrimaryEmpty));
    server2.invoke(() -> Bug43684DUnitTest.createServerCache(isReplicated, false));
    server3.invoke(() -> Bug43684DUnitTest.createServerCache(isReplicated, false));

    client1.invoke(() -> Bug43684DUnitTest.createClientCache(host, port1));
    createClientCache(host, port1);
    doOps();

    client1.invoke(() -> Bug43684DUnitTest.registerInterest(keys, null));
    client1.invoke(() -> Bug43684DUnitTest.verifyResponse2());
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(false, false);
  }

  @SuppressWarnings("rawtypes")
  public static Integer createServerCache(Boolean isReplicated, Boolean isPrimaryEmpty) throws Exception {
    disconnectFromDS();
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "server_" + OSProcess.getId()
        + ".gfs");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
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
    disconnectFromDS();
    Properties props = new Properties();
    props.setProperty(STATISTIC_ARCHIVE_FILE, "client_" + OSProcess.getId()
        + ".gfs");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
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
