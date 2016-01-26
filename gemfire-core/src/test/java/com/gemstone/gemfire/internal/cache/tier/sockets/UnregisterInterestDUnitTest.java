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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.ArrayList;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author ashetkar
 */
public class UnregisterInterestDUnitTest extends DistributedTestCase {

  private VM server0 = null;
  private VM client1 = null;
  private VM client2 = null;

  private static GemFireCache cache = null;

  private static final String regionname = "UnregisterInterestDUnitTest_region";
  private static final int all_keys = 0;
  private static final int list = 1;
  private static final int regex = 2;
  private static final int filter = 3;
  private static final boolean receiveValuesConstant = true;

  /**
   * @param name
   */
  public UnregisterInterestDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server0 = host.getVM(0);
    client1 = host.getVM(1);
    client2 = host.getVM(2);
    
    int port = (Integer)server0.invoke(UnregisterInterestDUnitTest.class, "createCacheAndStartServer");
    client1.invoke(UnregisterInterestDUnitTest.class, "createClientCache", new Object[]{client1.getHost(), port});
    client2.invoke(UnregisterInterestDUnitTest.class, "createClientCache", new Object[]{client2.getHost(), port});
  }

  public void tearDown2() throws Exception {
    closeCache();
    server0.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    client1.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    client2.invoke(UnregisterInterestDUnitTest.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

  /**
   * The test starts two clients who register interest in "ALL_KEYS" but close
   * without unregistering the same. The test then verifies that the server
   * cleans up the register interest artifacts of these clients properly.
   * 
   * See bug #47619
   * 
   * @throws Exception
   */
  public void testUnregisterInterestAllKeys() throws Exception {
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {all_keys, 0, 0});
    client1.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {all_keys, receiveValuesConstant, null});
    client2.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {all_keys, !receiveValuesConstant, null});
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {all_keys, 1, 1});
    client1.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    client2.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {all_keys, 0, 0});
  }

  /**
   * The test starts two clients who register interest in a list keys but close
   * without unregistering the same. The test then verifies that the server
   * cleans up the register interest artifacts of these clients properly.
   * 
   * See bug #47619
   * 
   * @throws Exception
   */
  public void testUnregisterInterestKeys() throws Exception {
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {list, 0, 0});
    client1.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {list, receiveValuesConstant, new String[]{"key_1", "key_2", "key_3", "key_4", "key_5"}});
    client2.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {list, !receiveValuesConstant, new String[]{"key_1", "key_2", "key_3", "key_4", "key_5"}});
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {list, 1, 1});
    client1.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    client2.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {list, 0, 0});
  }

  /**
   * The test starts two clients who register interest with regular expression
   * "[a-z]*[0-9]" but close without unregistering the same. The test then
   * verifies that the server cleans up the register interest artifacts of these
   * clients properly.
   * 
   * See bug #47619
   * 
   * @throws Exception
   */
  public void testUnregisterInterestPatterns() throws Exception {
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {regex, 0, 0});
    client1.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {regex, receiveValuesConstant, new String[] {"[a-z]*[0-9]"}});
    client2.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {regex, !receiveValuesConstant, new String[] {"[a-z]*[0-9]"}});
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {regex, 1, 1});
    client1.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    client2.invoke(UnregisterInterestDUnitTest.class, "closeCache");
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {regex, 0, 0});
  }

  /**
   * The test starts two clients who register interest in a common list of keys,
   * with the option of receiving updates as invalidates. One of the clients
   * later unregisters its interests in the list of keys. Server then does some
   * puts on the keys. The test then verifies that the server does send these
   * updates to the second client while first client sees no events.
   * 
   * See bug #47717
   * 
   * @throws Exception
   */
  public void testUnregisterInterestKeysInvForOneClientDoesNotAffectOtherClient() throws Exception {
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {list, 0, 0});
    client1.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {list, !receiveValuesConstant, new String[] {"key_1", "key_2", "key_3", "key_4", "key_5"}});
    client2.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {list, !receiveValuesConstant, new String[] {"key_1", "key_2", "key_3", "key_4", "key_5"}});
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {list, 0, 2});
    client1.invoke(UnregisterInterestDUnitTest.class, "unregisterInterest", new Object[] {new String[] {"key_1", "key_2", "key_3"}});
    server0.invoke(UnregisterInterestDUnitTest.class, "updateKeys",  new Object[] {new String[] {"key_1", "key_2", "key_3", "key_4", "key_5"}});
    client1.invoke(UnregisterInterestDUnitTest.class, "timedWaitForInvalidates", new Object[] {2});
    client2.invoke(UnregisterInterestDUnitTest.class, "timedWaitForInvalidates", new Object[] {5});
  }

  /**
   * The test starts two clients who register interest with the same regular
   * expression, with the option of receiving updates as invalidates. One of the
   * clients later unregisters its interest with the regular expression. Server
   * then does some puts on the keys. The test then verifies that the server
   * does send these updates to the second client while the first client sees no
   * events.
   * 
   * See bug #47717
   * 
   * @throws Exception
   */
  public void testUnregisterInterestRegexInvForOneClientDoesNotAffectOtherClient() throws Exception {
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {regex, 0, 0});
    client1.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {regex, !receiveValuesConstant, new String[] {"[a-z]*[0-9]"}});
    client2.invoke(UnregisterInterestDUnitTest.class, "registerInterest", new Object[] {regex, !receiveValuesConstant, new String[] {"[a-z]*[0-9]"}});
    server0.invoke(UnregisterInterestDUnitTest.class, "checkRIArtifacts", new Object[] {regex, 0, 2});
    client1.invoke(UnregisterInterestDUnitTest.class, "unregisterInterestRegex", new Object[] {new String[] {"[a-z]*[0-9]"}});
    server0.invoke(UnregisterInterestDUnitTest.class, "updateKeys",  new Object[] {new String[] {"key1", "key2", "key3", "key4", "key5"}});
    client1.invoke(UnregisterInterestDUnitTest.class, "timedWaitForInvalidates", new Object[] {0});
    client2.invoke(UnregisterInterestDUnitTest.class, "timedWaitForInvalidates", new Object[] {5});
  }

  public void _testUnregisterInterestFilters() throws Exception {
    
  }

  public static void checkRIArtifacts(Integer interestType, Integer value, Integer valueInv) {
    switch (interestType) {
    case all_keys:
      checkAllKeys(value, valueInv);
      break;
    case list:
      checkKeyList(value, valueInv);
      break;
    case regex:
      checkPatterns(value, valueInv);
      break;
    case filter:
      checkFilters(value, valueInv);
      break;
    default:
      fail("Invalid interest type: " + interestType, new IllegalArgumentException("Invalid interest type: " + interestType));
    }
  }

  public static void checkAllKeys(Integer value, Integer valueInv) {
    FilterProfile fp = ((GemFireCacheImpl)cache).getFilterProfile(regionname);
    assertEquals(value.intValue(), fp.getAllKeyClientsSize());
    assertEquals(valueInv.intValue(), fp.getAllKeyClientsInvSize());
  }

  public static void checkKeyList(Integer value, Integer valueInv) {
    FilterProfile fp = ((GemFireCacheImpl)cache).getFilterProfile(regionname);
    assertEquals(value.intValue(), fp.getKeysOfInterestSize());
    assertEquals(valueInv.intValue(), fp.getKeysOfInterestInvSize());
  }

  public static void checkPatterns(Integer value, Integer valueInv) {
    FilterProfile fp = ((GemFireCacheImpl)cache).getFilterProfile(regionname);
    assertEquals(value.intValue(), fp.getPatternsOfInterestSize());
    assertEquals(valueInv.intValue(), fp.getPatternsOfInterestInvSize());
  }

  public static void checkFilters(Integer value, Integer valueInv) {
  }

  public static void registerInterest(Integer interestType, Boolean receiveValues, String[] values) {
    Region region = cache.getRegion(regionname);
    switch (interestType) {
    case all_keys:
      region.registerInterest("ALL_KEYS", false, receiveValues);
      break;
    case list:
      ArrayList<String> keys = new ArrayList<String>();
      for (Object key : values) {
        keys.add((String)key);
      }
      region.registerInterest(keys, false, receiveValues);
      break;
    case regex:
      region.registerInterestRegex((String)values[0], false, receiveValues);
      break;
    case filter:
      break;
    default:
      fail("Invalid interest type: " + interestType, new IllegalArgumentException("Invalid interest type: " + interestType));
    }
  }

  public static void unregisterInterest(String[] keys) {
    Region region = cache.getRegion(regionname);
    
    for (String key : keys) {
      region.unregisterInterest(key);
    }
  }

  public static void unregisterInterestRegex(String[] patterns) {
    Region region = cache.getRegion(regionname);
    
    for (String pattern : patterns) {
      region.unregisterInterestRegex(pattern);
    }
  }

  public static void updateKeys(String[] keys) {
    Region region = cache.getRegion(regionname);
    
    for (String key : keys) {
      region.put(key, key + "_VALUE");
    }
  }

  public static void timedWaitForInvalidates(Integer invalidates) {
    final int inv = invalidates;
    final PoolImpl pool = (PoolImpl) PoolManager.find(cache.getRegion(regionname));

    WaitCriterion wc = new WaitCriterion() {      
      @Override
      public boolean done() {
        // Client region listeners are not invoked for invalidates that create entries.
        return pool.getInvalidateCount() == inv;
      }
      
      @Override
      public String description() {
        return "Expected to receive " + inv + " invalidates but received " + pool.getInvalidateCount();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10000, 100, true);
  }

  public static Integer createCacheAndStartServer() throws Exception {
    DistributedSystem ds = new UnregisterInterestDUnitTest("UnregisterInterestDUnitTest").getSystem();
    ds.disconnect();
    Properties props = new Properties();
    props.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
    CacheFactory cf = new CacheFactory(props);
    cache = cf.create();
    RegionFactory rf = ((GemFireCacheImpl)cache).createRegionFactory(RegionShortcut.REPLICATE);
    rf.create(regionname);
    CacheServer server = ((GemFireCacheImpl)cache).addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  public static void createClientCache(Host host, Integer port) throws Exception {
    DistributedSystem ds = new UnregisterInterestDUnitTest("UnregisterInterestDUnitTest").getSystem();
    ds.disconnect();

    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    ClientCacheFactory ccf = new ClientCacheFactory(props);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.addPoolServer(host.getHostName(), port);
    cache = ccf.create();
    ClientRegionFactory crf = ((GemFireCacheImpl)cache).createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
    crf.create(regionname);
  }
}
