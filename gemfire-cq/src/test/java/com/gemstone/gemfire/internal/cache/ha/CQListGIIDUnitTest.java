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
package com.gemstone.gemfire.internal.cache.ha;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 *
 * @author ashetkar
 * @since 5.7
 *
 */
public class CQListGIIDUnitTest extends DistributedTestCase {
  private final static int CREATE = 0;

  private final static int UPDATE = 1;

  private final static int DESTROY = 2;

  private final static int INVALIDATE = 3;

  private final static int CLOSE = 4;

  private final static int REGION_CLEAR = 5;

  private final static int REGION_INVALIDATE = 6;

  protected static Cache cache = null;

  protected static VM serverVM0 = null;

  private static VM serverVM1 = null;

  protected static VM clientVM1 = null;

  protected static VM clientVM2 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String regionName = "CQListGIIDUnitTest";

  private static final Map map = new HashMap();

  private static LogWriter logger = null;

  public static final String[] regions = new String[] { "regionA", "regionB" };

  public static final String KEY = "key-";

  public String[] cqs = new String[] {
  // 0 - Test for ">"
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0",

      // 1 - Test for "=" and "and".
      "SELECT ALL * FROM /root/" + regions[0]
          + " p where p.ID = 2 and p.status='active'",

      // 2 - Test for "<" and "and".
      "SELECT ALL * FROM /root/" + regions[1]
          + " p where p.ID < 5 and p.status='active'",

      // FOLLOWING CQS ARE NOT TESTED WITH VALUES; THEY ARE USED TO TEST PARSING
      // LOGIC WITHIN CQ.
      // 3
      "SELECT * FROM /root/" + regions[0] + " ;",
      // 4
      "SELECT ALL * FROM /root/" + regions[0],
      // 5
      "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; "
          + "SELECT ALL * FROM /root/" + regions[0] + " TYPE Portfolio",
      // 6
      "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; "
          + "SELECT ALL * FROM /root/" + regions[0] + " p TYPE Portfolio",
      // 7
      "SELECT ALL * FROM /root/" + regions[1]
          + " p where p.ID < 5 and p.status='active';",
      // 8
      "SELECT ALL * FROM /root/" + regions[0] + "  ;",
      // 9
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.description = NULL",
      // 10
      "SELECT ALL * FROM /root/" + regions[0]
          + " p where p.ID > 0 and p.status='active'", };

  /**
   * @param name
   *          name of the test
   */
  public CQListGIIDUnitTest(String name) {
    super(name);
  }

  /**
   * Sets up the test.
   */
  public void setUp() throws Exception {
    super.setUp();

    final Host host = Host.getHost(0);
    serverVM0 = host.getVM(0);
    serverVM1 = host.getVM(1);
    clientVM1 = host.getVM(2);
    clientVM2 = host.getVM(3);

    PORT1 = ((Integer)serverVM0.invoke(CQListGIIDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    PORT2 = ((Integer)serverVM1.invoke(CQListGIIDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_ENTRY })).intValue();
  }

  /**
   * Tears down the test.
   */
  public void tearDown2() throws Exception {
    super.tearDown2();
    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    serverVM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    closeCache();
    clientVM1.invoke(CQListGIIDUnitTest.class, "closeCache");
    clientVM2.invoke(CQListGIIDUnitTest.class, "closeCache");
    // then close the servers
    serverVM0.invoke(CQListGIIDUnitTest.class, "closeCache");
    serverVM1.invoke(CQListGIIDUnitTest.class, "closeCache");
    disconnectAllFromDS();
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(null);
  }

  public static Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, Integer.valueOf(1));
  }

  public static Integer createServerCache(String ePolicy, Integer cap)
      throws Exception {
    new CQListGIIDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    // cache.createRegion(regionName, attrs);
    createRegion(regions[0], "root", attrs);
    createRegion(regions[1], "root", attrs);
    Thread.sleep(2000);
    logger = cache.getLogger();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    if (ePolicy != null) {
      File overflowDirectory = new File("bsi_overflow_"+port);
      overflowDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {overflowDirectory};

      server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
      server1.getClientSubscriptionConfig().setCapacity(cap.intValue());
      // specify diskstore for this server
      server1.getClientSubscriptionConfig().setDiskStoreName(dsf.setDiskDirs(dirs1).create("bsi").getName());
    }
    server1.start();
    Thread.sleep(2000);
    return Integer.valueOf(server1.getPort());
  }

  public static Integer createOneMoreBridgeServer(Boolean notifyBySubscription)
      throws Exception {
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(notifyBySubscription.booleanValue());
    server1.getClientSubscriptionConfig().setEvictionPolicy(
        HARegionQueue.HA_EVICTION_POLICY_MEMORY);
    // let this server to use default diskstore
    server1.start();
    return Integer.valueOf(server1.getPort());
  }

  public static final Region createRegion(String name, String rootName,
      RegionAttributes attrs) throws CacheException {
    Region root = cache.getRegion(rootName);
    if (root == null) {
      // don't put listeners on root region
      RegionAttributes rootAttrs = attrs;
      AttributesFactory fac = new AttributesFactory(attrs);
      ExpirationAttributes expiration = ExpirationAttributes.DEFAULT;

      // fac.setCacheListener(null);
      fac.setCacheLoader(null);
      fac.setCacheWriter(null);
      fac.setPoolName(null);
      fac.setPartitionAttributes(null);
      fac.setRegionTimeToLive(expiration);
      fac.setEntryTimeToLive(expiration);
      fac.setRegionIdleTimeout(expiration);
      fac.setEntryIdleTimeout(expiration);
      rootAttrs = fac.create();
      root = cache.createRegion(rootName, rootAttrs);
    }

    return createSubregion(root, name, attrs, null);
  }

  /**
   * A helper for creating a subregion, potentially using a package protected
   * method to do so.  
   * @param root the parent region
   * @param name the name of the subregion to create
   * @param attrs the attributes used to create the subregion
   * @param internalArgs if not null, then use the package protected creation mechanism
   * @return the subregion whose parent is the provided root
   * @throws CacheException
   * @see Region#createSubregion(String, RegionAttributes)
   * @see LocalRegion#createSubregion(String, RegionAttributes, InternalRegionArguments)
   */
  public static Region createSubregion(Region root, String name,
      RegionAttributes attrs, final InternalRegionArguments internalArgs) throws CacheException
  {
    if (internalArgs == null) {
      return root.createSubregion(name, attrs);
    } else {
      try {
        LocalRegion lr = (LocalRegion) root;
        return lr.createSubregion(name, attrs, internalArgs);
      } catch (IOException ioe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(ioe);
        throw assErr;
      } catch (ClassNotFoundException cnfe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(cnfe);
        throw assErr;
      } 
    }
  }
  
  public static void createClientCache(Integer port1, Integer port2,
      String rLevel) throws Exception {
    createClientCache(port1, port2, Integer.valueOf(-1), rLevel, Boolean.FALSE);
  }

  public static void createClientCache(Integer port1, Integer port2,
      String rLevel, Boolean addListener) throws Exception {
    createClientCache(port1, port2, Integer.valueOf(-1), rLevel, addListener);
  }

  public static void createClientCache(Integer port1, Integer port2,
      Integer port3, String rLevel) throws Exception {
    createClientCache(port1, port2, port3, rLevel, Boolean.FALSE);
  }

  public static void destroyClientPool() {
    cache.getRegion("root").getSubregion(regions[0]).close();
    cache.getRegion("root").getSubregion(regions[1]).close();
    PoolManager.find("clientPool").destroy();
  }
  
  public static void createClientCache(Integer port1, Integer port2,
      Integer port3, String rLevel, Boolean addListener) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    String host = DistributedTestCase.getIPLiteral();

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new CQListGIIDUnitTest("temp").createCache(props);

    PoolFactory pf = PoolManager.createFactory();
    int endPointCount = 1;
    pf.addServer(host, port1);
    if (port2.intValue() != -1) {
      pf.addServer(host, port2);
      endPointCount++;
    }
    if (port3.intValue() != -1) {
      pf.addServer(host, port3);
      endPointCount++;
    }
    pf.setRetryAttempts(5);
    pf.setReadTimeout(2500);
    pf.setSocketBufferSize(32768);
    pf.setPingInterval(1000);
    pf.setMinConnections(endPointCount*2);
    pf.setSubscriptionRedundancy(Integer.parseInt(rLevel));
    pf.setSubscriptionEnabled(true).create("clientPool");

    try {
      cache.getQueryService();
    }
    catch (Exception cqe) {
      fail("Failed to getCQService.", cqe);
    }

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName("clientPool");

    RegionAttributes attrs = factory.create();
    createRegion(regions[0], "root", attrs);
    createRegion(regions[1], "root", attrs);
    logger = cache.getLogger();
  }

  /* Register CQs */
  public static void createCQ(String cqName, String queryStr) {
    getLogWriter().info("### Create CQ. ###" + cqName);
    // Get CQ Service.
    QueryService cqService = null;
    try {
      cqService = cache.getQueryService();
    }
    catch (Exception cqe) {
      fail("Failed to getCQService.", cqe);
    }
    // Create CQ Attributes.
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = { new CqQueryTestListener(getLogWriter()) };
    ((CqQueryTestListener)cqListeners[0]).cqName = cqName;

    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();

    // Create CQ.
    try {
      CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
      assertTrue("newCq() state mismatch", cq1.getState().isStopped());
    }
    catch (Exception ex) {
      getLogWriter().info("CqService is :" + cqService);
      ex.printStackTrace();
      AssertionError err = new AssertionError("Failed to create CQ " + cqName
          + " . ");
      err.initCause(ex);
      throw err;
    }
  }

  public static void executeCQ(String cqName, Boolean initialResults) {
    getLogWriter().info("### DEBUG EXECUTE CQ START ####");
    // Get CQ Service.
    QueryService cqService = null;
    CqQuery cq1 = null;
    cqService = cache.getQueryService();

    // Get CqQuery object.
    try {
      cq1 = cqService.getCq(cqName);
      if (cq1 == null) {
        getLogWriter().info(
            "Failed to get CqQuery object for CQ name: " + cqName);
        fail("Failed to get CQ " + cqName, new Exception("Failed to get CQ "
            + cqName));
      }
      else {
        getLogWriter().info("Obtained CQ, CQ name: " + cq1.getName());
        assertTrue("newCq() state mismatch", cq1.getState().isStopped());
      }
    }
    catch (Exception ex) {
      getLogWriter().info("CqService is :" + cqService);
      getLogWriter().error(ex);
      AssertionError err = new AssertionError("Failed to execute  CQ " + cqName);
      err.initCause(ex);
      throw err;
    }

    if (initialResults.booleanValue()) {
      SelectResults cqResults = null;

      try {
        cqResults = cq1.executeWithInitialResults();
      }
      catch (Exception ex) {
        getLogWriter().info("CqService is :" + cqService);
        ex.printStackTrace();
        AssertionError err = new AssertionError("Failed to execute  CQ "
            + cqName);
        err.initCause(ex);
        throw err;
      }
      getLogWriter().info("initial result size = " + cqResults.size());
      assertTrue("executeWithInitialResults() state mismatch", cq1.getState()
          .isRunning());
      // if (expectedResultsSize >= 0) {
      // assertEquals("unexpected results size", expectedResultsSize, cqResults
      // .size());
      // }
    }
    else {

      try {
        cq1.execute();
      }
      catch (Exception ex) {
        getLogWriter().info("CqService is :" + cqService);
        ex.printStackTrace();
        AssertionError err = new AssertionError("Failed to execute  CQ "
            + cqName);
        err.initCause(ex);
        throw err;
      }
      assertTrue("execute() state mismatch", cq1.getState().isRunning());
    }
  }

  public static void registerInterestListCQ(String regionName, int keySize) {
    // Get CQ Service.
    Region region = null;
    try {
      region = cache.getRegion("root").getSubregion(regionName);
      region.getAttributesMutator().setCacheListener(
          new CertifiableTestCacheListener(getLogWriter()));
    }
    catch (Exception cqe) {
      AssertionError err = new AssertionError("Failed to get Region.");
      err.initCause(cqe);
      throw err;
    }

    try {
      List list = new ArrayList();
      for (int i = 1; i <= keySize; i++) {
        list.add(KEY + i);
      }
      region.registerInterest(list);
    }
    catch (Exception ex) {
      AssertionError err = new AssertionError("Failed to Register InterestList");
      err.initCause(ex);
      throw err;
    }
  }

  public static void waitForCreated(String cqName, String key) {
    waitForEvent(0, cqName, key);
  }

  public static void waitForEvent(int event, String cqName, String key) {
    // Get CQ Service.
    QueryService cqService = null;
    try {
      cqService = cache.getQueryService();
    }
    catch (Exception cqe) {
      cqe.printStackTrace();
      fail("Failed to getCQService.", cqe);
    }

    CqQuery cQuery = cqService.getCq(cqName);
    if (cQuery == null) {
      fail("Failed to get CqQuery for CQ : " + cqName, new Exception(
          "Failed to get CqQuery for CQ : " + cqName));
    }

    CqAttributes cqAttr = cQuery.getCqAttributes();
    CqListener[] cqListener = cqAttr.getCqListeners();
    CqQueryTestListener listener = (CqQueryTestListener)cqListener[0];

    switch (event) {
      case CREATE:
        listener.waitForCreated(key);
        break;

      case UPDATE:
        listener.waitForUpdated(key);
        break;

      case DESTROY:
        listener.waitForDestroyed(key);
        break;

      case INVALIDATE:
        listener.waitForInvalidated(key);
        break;

      case CLOSE:
        listener.waitForClose();
        break;

      case REGION_CLEAR:
        listener.waitForRegionClear();
        break;

      case REGION_INVALIDATE:
        listener.waitForRegionInvalidate();
        break;

    }
  }

  public static void registerInterestListAll() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    }
    catch (Exception ex) {
      fail("failed in registerInterestListAll", ex);
    }
  }

  public static void registerInterestList() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("k1");
      r.registerInterest("k3");
      r.registerInterest("k5");
    }
    catch (Exception ex) {
      fail("failed while registering keys", ex);
    }
  }

  public static void putEntries(String rName, Integer num) {
    try {
      Region r = cache.getRegion("root").getSubregion(rName);
      assertNotNull(r);
      for (int i = 0; i < num.longValue(); i++) {
        r.put(KEY + i, new Portfolio(i + 1));
      }
      getLogWriter().info(
          "### Number of Entries in Region " + rName + ": " + r.keys().size());
    }
    catch (Exception ex) {
      fail("failed in putEntries()", ex);
    }
  }

  /**
   *
   *
   * @throws Exception
   */
  public void _testSpecificClientCQIsGIIedPart1() throws Exception {
    Integer size = Integer.valueOf(10);
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });
    serverVM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    // createClientCache(Integer.valueOf(PORT1), Integer.valueOf(PORT2), "1");
    clientVM1.invoke(CQListGIIDUnitTest.class, "createClientCache",
        new Object[] { Integer.valueOf(PORT1), Integer.valueOf(PORT2), "1" });
    clientVM2.invoke(CQListGIIDUnitTest.class, "createClientCache",
        new Object[] { Integer.valueOf(PORT1), Integer.valueOf(PORT2), "0" });

    clientVM1.invoke(CQListGIIDUnitTest.class, "createCQ", new Object[] {
        "testSpecificClientCQIsGIIed_0", cqs[0] });
    clientVM1.invoke(CQListGIIDUnitTest.class, "executeCQ", new Object[] {
        "testSpecificClientCQIsGIIed_0", Boolean.FALSE });
    clientVM2.invoke(CQListGIIDUnitTest.class, "createCQ", new Object[] {
        "testSpecificClientCQIsGIIed_0", cqs[0] });
    clientVM2.invoke(CQListGIIDUnitTest.class, "executeCQ", new Object[] {
        "testSpecificClientCQIsGIIed_0", Boolean.FALSE });

    serverVM1.invoke(CQListGIIDUnitTest.class, "stopServer");

    serverVM0.invoke(CQListGIIDUnitTest.class, "putEntries", new Object[] {
        regions[0], size });

    serverVM1.invoke(CQListGIIDUnitTest.class, "startServer");
    Thread.sleep(3000); // TODO: Find a better 'n reliable alternative

    serverVM0.invoke(CQListGIIDUnitTest.class, "VerifyCUMCQList", new Object[] {
        size, Integer.valueOf(2) });
    serverVM1.invoke(CQListGIIDUnitTest.class, "VerifyCUMCQList", new Object[] {
        size, Integer.valueOf(1) });
    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    serverVM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
  }

  /**
   * This test asserts that cq list of a client for an event is not lost if that
   * client's queue has been GII'ed to a server where that event already
   * existed.
   *
   * @throws Exception
   */
  public void testClientCQNotLostAtGIIReceiver() throws Exception {
    Integer size = Integer.valueOf(10);
    VM serverVM2 = clientVM2;

    int port3 = ((Integer)serverVM2.invoke(CQListGIIDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();

    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "45000" });

    // createClientCache(Integer.valueOf(PORT1), Integer.valueOf(PORT2), "1");
    createClientCache(Integer.valueOf(PORT1), Integer.valueOf(PORT2),
        Integer.valueOf(port3), "1");
    try {
    clientVM1.invoke(CQListGIIDUnitTest.class, "createClientCache",
        new Object[] { Integer.valueOf(PORT1), Integer.valueOf(port3),
            Integer.valueOf(PORT2), "1" });
    try {
    createCQ("testSpecificClientCQIsGIIed_0", cqs[0]);
    executeCQ("testSpecificClientCQIsGIIed_0", Boolean.FALSE);
    clientVM1.invoke(CQListGIIDUnitTest.class, "createCQ", new Object[] {
        "testSpecificClientCQIsGIIed_0", cqs[0] });
    clientVM1.invoke(CQListGIIDUnitTest.class, "executeCQ", new Object[] {
        "testSpecificClientCQIsGIIed_0", Boolean.FALSE });

    serverVM0.invoke(CQListGIIDUnitTest.class, "putEntries", new Object[] {
        regions[0], size });

    serverVM1.invoke(CQListGIIDUnitTest.class, "VerifyCUMCQList", new Object[] {
        size, Integer.valueOf(1) });

    serverVM2.invoke(CQListGIIDUnitTest.class, "stopServer");
    Thread.sleep(3000); // TODO: Find a better 'n reliable alternative

    serverVM0.invoke(CQListGIIDUnitTest.class, "VerifyCUMCQList", new Object[] {
      size, Integer.valueOf(2) });
    serverVM1.invoke(CQListGIIDUnitTest.class, "VerifyCUMCQList", new Object[] {
        size, Integer.valueOf(2) });
    } finally {
      clientVM1.invoke(CQListGIIDUnitTest.class, "destroyClientPool");
    }

    } finally {
      destroyClientPool();
    }
  }

  public static void VerifyCUMCQList(Integer numOfKeys, Integer numOfClients) {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServerImpl server = (CacheServerImpl)iter.next();
        Map haContainer = server.getAcceptor().getCacheClientNotifier()
            .getHaContainer();
        Object[] keys = haContainer.keySet().toArray();
        logger.fine("### numOfKeys :" + numOfKeys.intValue() + " keys.length : " + keys.length +
            " haContainer size : " + haContainer.size());
        assertEquals(numOfKeys.intValue(), keys.length);
        for (int i = 0; i < numOfKeys.intValue(); i++) {
          logger.fine("i=: " + i);
          ClientUpdateMessageImpl cum = (ClientUpdateMessageImpl)haContainer
              .get(keys[i]);
          assertNotNull(cum);
          assertNotNull(cum.getClientCqs());
          assertEquals("This test may fail if the image provider gets an ack from client before providing image",
              numOfClients.intValue(), cum.getClientCqs().size());
        }
      }
    }
    catch (Exception e) {
      fail("failed in VerifyCUMCQList()" + e, e);
    }
  }

  private static void stopOneBridgeServer(Integer port) {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        if (server.getPort() == port.intValue()) {
          server.stop();
        }
      }
    }
    catch (Exception e) {
      fail("failed in stopOneBridgeServer()" + e);
    }
  }

  public static void stopServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.stop();
      }
    }
    catch (Exception e) {
      fail("failed in stopServer()" + e);
    }
  }

  public static void startServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.start();
      }
    }
    catch (Exception e) {
      fail("failed in startServer()" + e);
    }
  }

  public static void waitTillMessagesAreDispatched(Integer port, Long waitLimit) {
    try {
      boolean dispatched = false;
      Map haContainer = null;
      haContainer = cache.getRegion(Region.SEPARATOR
          + CacheServerImpl.generateNameForClientMsgsRegion(port.intValue()));
      if (haContainer == null) {
        Object[] servers = cache.getCacheServers().toArray();
        for (int i = 0; i < servers.length; i++) {
          if (port.intValue() == ((CacheServerImpl)servers[i]).getPort()) {
            haContainer = ((CacheServerImpl)servers[i]).getAcceptor()
                .getCacheClientNotifier().getHaContainer();
            break;
          }
        }
      }
      long startTime = System.currentTimeMillis();
      while (waitLimit.longValue() > (System.currentTimeMillis() - startTime)) {
        if (haContainer.size() == 0) {
          dispatched = true;
          break;
        }
        try {
          Thread.sleep(50);
        }
        catch (InterruptedException ie) {
          fail("interrupted");
        }
      }
      logger.fine("Exiting sleep, time elapsed was: "
          + (System.currentTimeMillis() - startTime));
      if (!dispatched) {
        throw new Exception(
            "Test tuning issue: The HARegionQueue is not fully drained, so cannot continue the test.");
      }
    }
    catch (Exception e) {
      fail("failed in waitTillMessagesAreDispatched()" + e);
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}
