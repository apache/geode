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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.cache.client.*;

/**
 * DUnit Test for use-cases of various {@link InterestResultPolicy} types.
 *
 * @author Dinesh Patel
 *
 */
public class InterestResultPolicyDUnitTest extends DistributedTestCase
{
  /** test VM */
  VM vm0, vm1 = null;

  /** total entries pre-populated on server */
  final static int PREPOPULATED_ENTRIES = 10;

  /** the cache instance for the test */
  private static Cache cache = null;
  
  private static int PORT   ; 

  private static final String REGION_NAME = "InterestResultPolicyDUnitTest_region" ;

  private IgnoredException expectedEx;

  /**
   * Creates a test instance with the given name
   *
   * @param name -
   *          name of test instance
   */
  public InterestResultPolicyDUnitTest(String name) {
    super(name);
  }

  /**
   * Creates the server cache and populates it with some entries
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    Wait.pause(5000);
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    PORT =  ((Integer)vm0.invoke(InterestResultPolicyDUnitTest.class, "createServerCache" )).intValue();
    vm0.invoke(InterestResultPolicyDUnitTest.class, "populateServerCache");
  }

  /**
   * Closes the cache on server and client
   */
  @Override
  protected final void preTearDown() throws Exception {
    // might get ServerConnectivityExceptions during shutdown
    this.expectedEx = IgnoredException.addIgnoredException(ServerConnectivityException.class
        .getName());
    // close server
    vm0.invoke(InterestResultPolicyDUnitTest.class, "closeCache");
    // close client
    vm1.invoke(InterestResultPolicyDUnitTest.class, "closeCache");
  }

  @Override
  protected void postTearDown() throws Exception {
    if (this.expectedEx != null) {
      this.expectedEx.remove();
    }
  }

  /**
   * This test does the following<br>
   * 1)Create client cache and region<br>
   * 2)Call registerInterest with all keys pre-populated on server and
   * policy:NONE<br>
   * 3)At the end of registerInterest call, verify that no entries are created
   * in the client cache<br>
   */
  public void testPolicyNone()
  {
    LogWriter logger = getSystem().getLogWriter();
    logger.fine("testPolicyNone BEGIN");
    Object[] objArr = new Object[2];
    objArr[0] = InterestResultPolicy.NONE;
    objArr[1] = new Integer(PREPOPULATED_ENTRIES);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT)});
    vm1.invoke(InterestResultPolicyDUnitTest.class, "registerInterest", objArr);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "verifyResult", objArr);
    logger.fine("testPolicyNone END");
  }

  /**
   * This test does the following<br>
   * 1)Create client cache and region<br>
   * 2)Call registerInterest with all keys pre-populated on server and
   * policy:KEYS<br>
   * 3)At the end of registerInterest call, verify that entries are created in
   * the client cache with value null<br>
   */
  public void testPolicyKeys()
  {
    LogWriter logger = getSystem().getLogWriter();
    logger.fine("testPolicyKeys BEGIN");
    Object[] objArr = new Object[2];
    objArr[0] = InterestResultPolicy.KEYS;
    objArr[1] = new Integer(PREPOPULATED_ENTRIES);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT)});;
    vm1.invoke(InterestResultPolicyDUnitTest.class, "registerInterest", objArr);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "verifyResult", objArr);
    logger.fine("testPolicyKeys END");
  }

  /**
   * This test does the following<br>
   * 1)Create client cache and region<br>
   * 2)Call registerInterest with all keys pre-populated on server and
   * policy:KEYS_VALUES<br>
   * 3)At the end of registerInterest call, verify that all entries are created
   * in the client cache with values<br>
   */
  public void testPolicyKeysValues()
  {
    LogWriter logger = getSystem().getLogWriter();
    logger.fine("testPolicyKeyValues BEGIN");
    Object[] objArr = new Object[2];
    objArr[0] = InterestResultPolicy.KEYS_VALUES;
    objArr[1] = new Integer(PREPOPULATED_ENTRIES);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT)});
    vm1.invoke(InterestResultPolicyDUnitTest.class, "registerInterest", objArr);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "verifyResult", objArr);
    logger.fine("testPolicyKeyValues END");
  }

  /**
   * This test does the following<br>
   * 1)Create client cache and region<br>
   * 2)Call registerInterest with specific keys (having extra keys other than
   * those pre-populated on server cache) and policy:KEYS<br>
   * 3)At the end of registerInterest call, verify that only prepolulated
   * entries are created in the client cache with null values (entries for keys
   * in the keylist which are not on the server should not be created on the
   * client as a result of registerInterest call)<br>
   */
  public void testBug35358()
  {
    LogWriter logger = getSystem().getLogWriter();
    logger.fine("testBug35358 BEGIN");
    Object[] objArr = new Object[2];
    objArr[0] = InterestResultPolicy.KEYS;
    /* registering for 5 extra keys */
    objArr[1] = new Integer(PREPOPULATED_ENTRIES + 5);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT)});
    vm1.invoke(InterestResultPolicyDUnitTest.class, "registerInterest", objArr);
    vm1.invoke(InterestResultPolicyDUnitTest.class, "verifyResult", objArr);
    Integer cnt = (Integer)vm0.invoke(InterestResultPolicyDUnitTest.class,
        "getEntryCount");
    assertEquals(cnt.intValue(), PREPOPULATED_ENTRIES);
    logger.fine("testBug35358 END");
  }

  /**
   * Creates cache instance
   *
   * @param props -
   *          properties of the distributed system
   * @return cache
   * @throws Exception -
   *           thrown if any problem occurs while connecting to distributed
   *           system or creating cache
   */
  private Cache createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * Closes the cache instance created and disconnects from the distributed
   * system.
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * Creates the cache and starts the server
   *
   * @throws Exception -
   *           thrown if any problem occurs in creating server cache or starting
   *           the server
   */
  public static Integer createServerCache() throws Exception
  {
    InterestResultPolicyDUnitTest test = new InterestResultPolicyDUnitTest(
        "temp");
    cache = test.createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.setSocketBufferSize(32768);
    server.start();
    return new Integer(port);
  }

  /**
   * Populates the server cache by putting some entries
   *
   * @throws Exception -
   *           thrown if any problem occurs while creating entries at server
   *           cache
   */
  public static void populateServerCache() throws Exception
  {
    Region region1 = cache.getRegion(Region.SEPARATOR+REGION_NAME);
    for (int i = 0; i < PREPOPULATED_ENTRIES; i++) {
      region1.put("key-" + i, "val-" + i);
    }
  }

  /**
   * Creates the client cache and region.
   *
   * @throws Exception -
   *           thrown if any problem occurs in creating cache or region
   */
  public static void createClientCache(String host, Integer port) throws Exception
  {
    PORT = port.intValue() ;
    InterestResultPolicyDUnitTest test = new InterestResultPolicyDUnitTest(
        "temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = test.createCache(props);
    Pool p = PoolManager.createFactory()
      .addServer(host, port.intValue())
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      .setReadTimeout(10000)
      .setThreadLocalConnections(true)
      .setSocketBufferSize(32768)
      .setMinConnections(3)
      // .setRetryAttempts(5)
      // .setRetryInterval(10000)
      .create("InterestResultPolicyDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  /**
   * Registers the test region on client with a keylist containing all keys
   * pre-populated on server and a given {@link InterestResultPolicy} type.
   *
   * @param interestPolicy -
   *          InterestResultPolicy type specified (NONE,KEYS,KEY_VALUES or
   *          DEFAULT)
   */
  public static void registerInterest(Object interestPolicy,
      Object totalKeysToRegister)
  {
    InterestResultPolicy policy = (InterestResultPolicy)interestPolicy;
    int totalKeys = ((Integer)totalKeysToRegister).intValue();
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    LogWriter logger = cache.getLogger();
    logger.fine("Registering interest in " + totalKeys + " keys");
    List keylist = new ArrayList();
    for (int i = 0; i < totalKeys; i++) {
      keylist.add("key-" + i);
    }

    try {
      region1.registerInterest(keylist, policy);
    }
    catch (CacheWriterException e) {
      Assert.fail("failed to register interestlist for the client", e);
    }
  }

  /**
   * Verifies the number of entries (including values) created on the test
   * region at the end of {@link Region#registerInterest} call depending on the
   * type of {@link InterestResultPolicy} registered for the region.
   *
   * @param interestPolicy -
   *          {@link InterestResultPolicy} registered for the region
   */
  public static void verifyResult(Object interestPolicy,
      Object totalKeysRegistered)
  {
    Region region1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
    int entriesSize = region1.entrySet(false).size();
    int keysSize = region1.keySet().size();
    int valuesSize = region1.values().size();
    InterestResultPolicy policy = (InterestResultPolicy)interestPolicy;
    LogWriter logger = cache.getLogger();

    logger.fine("policy = " + policy + " ==> entries = " + entriesSize
        + " ;keys = " + keysSize + ";values = " + valuesSize);

    if (policy.isNone()) {
      // nothing should be created on client cache
      assertEquals(0, entriesSize);
      assertEquals(0, keysSize);
      assertEquals(0, valuesSize);
    }
    else if (policy.isKeys()) {
      // all keys should be created with values null
      assertEquals(PREPOPULATED_ENTRIES, entriesSize);
      assertEquals(PREPOPULATED_ENTRIES, keysSize);
      assertEquals(0, valuesSize);
    }
    else if (policy.isKeysValues()) {
      // all the keys and values should be created
      assertEquals(PREPOPULATED_ENTRIES, entriesSize);
      assertEquals(PREPOPULATED_ENTRIES, keysSize);
      assertEquals(PREPOPULATED_ENTRIES, valuesSize);
    }
  }

  /**
   * Gets the total number of keys in the test region
   *
   * @return - total keys
   */
  public static Object getEntryCount()
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    int keysSize = region1.keySet().size();
    return new Integer(keysSize);
  }
}
