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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * This test verifies that all entry operations (create,put,destroy,invalidate)
 * which propagate from one server1 to another server2 do get delivered to the
 * client connected to server2 (server2 is primary for client)
 *
 */

public class OperationsPropagationDUnitTest extends DistributedTestCase
{

  /**
   * Server1
   */
  VM server1 = null;

  /**
   * Server2
   */
  VM server2 = null;

  /**
   * Client
   */
  VM client1 = null;

  /**
   * Port of server1
   */
  public static int PORT1;

  /**
   * Port of server2
   */
  public static int PORT2;

  /**
   * Name of the region
   */
  private static final String REGION_NAME = "OperationsPropagationDUnitTest_Region";

  /**
   * cache
   */
  private static Cache cache = null;

  /**
   * Constructor
   *
   * @param name
   */
  public OperationsPropagationDUnitTest(String name) {
    super(name);
  }

  /**
   * Create the server1, server2 (in the same DS) and client1 (which is
   * connected only to server2
   */
  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    PORT1 = ((Integer)server1.invoke(OperationsPropagationDUnitTest.class,
        "createServerCache")).intValue();
    PORT2 = ((Integer)server2.invoke(OperationsPropagationDUnitTest.class,
        "createServerCache")).intValue();
    client1.invoke(OperationsPropagationDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(host), new Integer(PORT2) });

  }

  /**
   * close the caches of the client and the servers
   */
  public void tearDown2() throws Exception
  {
    super.tearDown2();
    client1.invoke(OperationsPropagationDUnitTest.class, "closeCache");
    server1.invoke(OperationsPropagationDUnitTest.class, "closeCache");
    server2.invoke(OperationsPropagationDUnitTest.class, "closeCache");

  }

  /**
   * closes the cache and disconnects the vm from teh distributed system
   *
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * connect to the DS and create a cache
   *
   * @param props
   * @throws Exception
   */
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  /**
   * region to be created for the test
   */
  protected static Region region = null;

  /**
   * Create the server
   *
   * @return
   * @throws Exception
   */
  public static Integer createServerCache() throws Exception
  {
    new OperationsPropagationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);

    CacheServerImpl server = (CacheServerImpl)cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }

  /**
   * create the client and connect it to the server with the given port
   *
   * @param port2
   * @throws Exception
   */
  public static void createClientCache(String host, Integer port2) throws Exception
  {
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new OperationsPropagationDUnitTest("temp").createCache(props);
    props.setProperty("retryAttempts", "2");
    props.setProperty("endpoints", "ep1="+host+":" + PORT2);
    props.setProperty("redundancyLevel", "-1");
    props.setProperty("establishCallbackConnection", "true");
    props.setProperty("LBPolicy", "Sticky");
    props.setProperty("readTimeout", "2000");
    props.setProperty("socketBufferSize", "1000");
    props.setProperty("retryInterval", "250");
    props.setProperty("connectionsPerServer", "2");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    
    ClientServerTestCase.configureConnectionPool(factory, host, PORT2,-1, true, -1, 2, null);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS");

  }

  public static final String CREATE_KEY = "createKey";

  public static final String CREATE_VALUE = "createValue";

  public static final String UPDATE_KEY = "updateKey";

  public static final String UPDATE_VALUE1 = "updateValue1";

  public static final String UPDATE_VALUE2 = "updateValue2";

  public static final String INVALIDATE_KEY = "invalidateKey";

  public static final String INVALIDATE_VALUE = "invalidateValue";

  public static final String DESTROY_KEY = "destroyKey";

  public static final String DESTROY_VALUE = "destroyValue";

  public static final String PUTALL_KEY = "putAllKey";

  public static final String PUTALL_VALUE = "putAllValue";

  public static final String PUTALL_KEY2 = "putAllKey2";

  public static final String PUTALL_VALUE2 = "putAllValue2";

  /**
   * This test: 1) First the initial keys and values 2) Verify that the initial
   * keys and values have reached the client 3) Do the operations which we want
   * to propagate (create, update, invalidate and destroy) 4) Verify the
   * operations reached the client
   * 5) Do a removeAll
   * 6) Verify it reached the client
   *
   * @throws Exception
   */
  public void testOperationsPropagation() throws Exception
  {
    server1.invoke(OperationsPropagationDUnitTest.class, "initialPutKeyValue");
    client1.invoke(OperationsPropagationDUnitTest.class,
        "assertKeyValuePresent");
    server1.invoke(OperationsPropagationDUnitTest.class, "doOperations");
    client1.invoke(OperationsPropagationDUnitTest.class,
        "assertOperationsSucceeded");
    server1.invoke(OperationsPropagationDUnitTest.class, "doRemoveAll");
    client1.invoke(OperationsPropagationDUnitTest.class,
        "assertRemoveAllSucceeded");
  }

  /**
   * put the initial keys and values
   *
   */
  public static void initialPutKeyValue()
  {
    try {
      region.put(UPDATE_KEY, UPDATE_VALUE1);
      region.put(INVALIDATE_KEY, INVALIDATE_VALUE);
      region.put(DESTROY_KEY, DESTROY_VALUE);
    }
    catch (Exception e) {
      fail(" Test failed due to " + e, e);
    }

  }

  /**
   * do the operations which you want to propagate
   *
   */
  public static void doOperations()
  {
    try {
      region.create(CREATE_KEY, CREATE_VALUE);
      region.put(UPDATE_KEY, UPDATE_VALUE2);
      region.invalidate(INVALIDATE_KEY);
      region.destroy(DESTROY_KEY);
      Map map = new HashMap();
      map.put(PUTALL_KEY,PUTALL_VALUE);
      map.put(PUTALL_KEY2,PUTALL_VALUE2);
      region.putAll(map);
    }
    catch (Exception e) {
      fail(" Test failed due to " + e, e);
    }

  }

  /**
   * assert the initial key values are present
   *
   */
  public static void assertKeyValuePresent()
  {
    try {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(UPDATE_KEY);
          return UPDATE_VALUE1.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*
       * if (!(region.get(UPDATE_KEY).equals(UPDATE_VALUE1))) { fail(" Expected
       * value to be " + UPDATE_VALUE1 + " but it is " +
       * region.get(UPDATE_KEY)); }
       */
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(INVALIDATE_KEY);
          return INVALIDATE_VALUE.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*
       * if (!(region.get(INVALIDATE_KEY).equals(INVALIDATE_VALUE))) { fail("
       * Expected value to be " + INVALIDATE_VALUE + " but it is " +
       * region.get(INVALIDATE_KEY)); }
       */
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(DESTROY_KEY);
          return DESTROY_VALUE.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*
       * if (!(region.get(DESTROY_KEY).equals(DESTROY_VALUE))) { fail(" Expected
       * value to be " + DESTROY_VALUE + " but it is " +
       * region.get(DESTROY_KEY)); }
       */

    }
    catch (Exception e) {
      fail(" Test failed due to " + e, e);
    }

  }

  /**
   * assert the operations reached the client successfully
   *
   */
  public static void assertOperationsSucceeded()
  {

    try {
      //Thread.sleep(5000);
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(CREATE_KEY);
          return CREATE_VALUE.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*if (!(region.get(CREATE_KEY).equals(CREATE_VALUE))) {
       fail("CREATE operation did not propagate to client : Expected value to be "
       + CREATE_VALUE + " but it is " + region.get(CREATE_KEY));
       }*/
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(UPDATE_KEY);
          return UPDATE_VALUE2.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*if (!(region.get(UPDATE_KEY).equals(UPDATE_VALUE2))) {
       fail(" UPDATE operation did not propagate to Client : Expected value to be "
       + UPDATE_VALUE2 + " but it is " + region.get(UPDATE_KEY));
       }*/
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return !region.containsKey(DESTROY_KEY);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      

      /*if (region.containsKey(DESTROY_KEY)) {
       fail(" DESTROY operation did not propagate to Client : Expected "
       + DESTROY_KEY + " not to be present but it is ");
       }*/
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(INVALIDATE_KEY);
          return val == null;
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*if (!(region.get(INVALIDATE_KEY) == null)) {
       fail(" INVALIDATE operation did not propagate to Client : Expected value to be null but it is "
       + region.get(INVALIDATE_KEY));
       }*/
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(PUTALL_KEY);
          return PUTALL_VALUE.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      /*
       * if (!(region.get(PUTALL_KEY).equals(PUTALL_VALUE))) { fail("PUTALL
       * operation did not propagate to client : Expected value to be " +
       * PUTALL_VALUE + " but it is " + region.get(PUTALL_KEY)); }
       */
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = region.get(PUTALL_KEY2);
          return PUTALL_VALUE2.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
    }
    catch (Exception e) {
      fail(" Test failed due to " + e, e);
    }
  }
  
  public static void doRemoveAll() {
    region.removeAll(Arrays.asList(PUTALL_KEY, PUTALL_KEY2));
  }

  public static void assertRemoveAllSucceeded() {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Object val = region.get(PUTALL_KEY);
        return !region.containsKey(PUTALL_KEY) && !region.containsKey(PUTALL_KEY2);
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
  }
}
