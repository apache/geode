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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This test verifies that all entry operations (create,put,destroy,invalidate) which propagate from
 * one server1 to another server2 do get delivered to the client connected to server2 (server2 is
 * primary for client)
 */
@Category({ClientSubscriptionTest.class})
public class OperationsPropagationDUnitTest extends JUnit4DistributedTestCase {

  VM server1 = null;
  VM server2 = null;
  VM client1 = null;

  public int PORT1;
  public int PORT2;

  private static final String REGION_NAME =
      OperationsPropagationDUnitTest.class.getSimpleName() + "_Region";

  private static Cache cache = null;

  /**
   * Create the server1, server2 (in the same DS) and client1 (which is connected only to server2
   */
  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    PORT1 = ((Integer) server1.invoke(() -> OperationsPropagationDUnitTest.createServerCache()))
        .intValue();
    PORT2 = ((Integer) server2.invoke(() -> OperationsPropagationDUnitTest.createServerCache()))
        .intValue();
    client1.invoke(() -> OperationsPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(host), new Integer(PORT2)));
  }

  /**
   * close the caches of the client and the servers
   */
  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(() -> OperationsPropagationDUnitTest.closeCache());
    server1.invoke(() -> OperationsPropagationDUnitTest.closeCache());
    server2.invoke(() -> OperationsPropagationDUnitTest.closeCache());
  }

  /**
   * closes the cache and disconnects the vm from the distributed system
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * connect to the DS and create a cache
   */
  private void createCache(Properties props) throws Exception {
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
   */
  public static Integer createServerCache() throws Exception {
    new OperationsPropagationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);

    CacheServerImpl server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  /**
   * create the client and connect it to the server with the given port
   */
  public static void createClientCache(String host, Integer port2) throws Exception {
    int PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new OperationsPropagationDUnitTest().createCache(props);
    props.setProperty("retryAttempts", "2");
    props.setProperty("endpoints", "ep1=" + host + ":" + PORT2);
    props.setProperty("redundancyLevel", "-1");
    props.setProperty("establishCallbackConnection", "true");
    props.setProperty("LBPolicy", "Sticky");
    props.setProperty("readTimeout", "2000");
    props.setProperty("socketBufferSize", "1000");
    props.setProperty("retryInterval", "250");
    props.setProperty("connectionsPerServer", "2");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);

    ClientServerTestCase.configureConnectionPool(factory, host, PORT2, -1, true, -1, 2, null);
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
   * This test: 1) First the initial keys and values 2) Verify that the initial keys and values have
   * reached the client 3) Do the operations which we want to propagate (create, update, invalidate
   * and destroy) 4) Verify the operations reached the client 5) Do a removeAll 6) Verify it reached
   * the client
   */
  @Test
  public void testOperationsPropagation() throws Exception {
    server1.invoke(() -> OperationsPropagationDUnitTest.initialPutKeyValue());
    client1.invoke(() -> OperationsPropagationDUnitTest.assertKeyValuePresent());
    server1.invoke(() -> OperationsPropagationDUnitTest.doOperations());
    client1.invoke(() -> OperationsPropagationDUnitTest.assertOperationsSucceeded());
    server1.invoke(() -> OperationsPropagationDUnitTest.doRemoveAll());
    client1.invoke(() -> OperationsPropagationDUnitTest.assertRemoveAllSucceeded());
  }

  /**
   * put the initial keys and values
   */
  public static void initialPutKeyValue() {
    try {
      region.put(UPDATE_KEY, UPDATE_VALUE1);
      region.put(INVALIDATE_KEY, INVALIDATE_VALUE);
      region.put(DESTROY_KEY, DESTROY_VALUE);
    } catch (Exception e) {
      Assert.fail(" Test failed due to " + e, e);
    }
  }

  /**
   * do the operations which you want to propagate
   */
  public static void doOperations() {
    try {
      region.create(CREATE_KEY, CREATE_VALUE);
      region.put(UPDATE_KEY, UPDATE_VALUE2);
      region.invalidate(INVALIDATE_KEY);
      region.destroy(DESTROY_KEY);
      Map map = new HashMap();
      map.put(PUTALL_KEY, PUTALL_VALUE);
      map.put(PUTALL_KEY2, PUTALL_VALUE2);
      region.putAll(map);
    } catch (Exception e) {
      Assert.fail(" Test failed due to " + e, e);
    }
  }

  /**
   * assert the initial key values are present
   */
  public static void assertKeyValuePresent() {
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
      GeodeAwaitility.await().untilAsserted(wc);

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
      GeodeAwaitility.await().untilAsserted(wc);

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
      GeodeAwaitility.await().untilAsserted(wc);
    } catch (Exception e) {
      Assert.fail(" Test failed due to " + e, e);
    }
  }

  /**
   * assert the operations reached the client successfully
   */
  public static void assertOperationsSucceeded() {
    try {
      // Thread.sleep(5000);
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
      GeodeAwaitility.await().untilAsserted(wc);

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
      GeodeAwaitility.await().untilAsserted(wc);

      wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return !region.containsKey(DESTROY_KEY);
        }

        public String description() {
          return excuse;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);


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
      GeodeAwaitility.await().untilAsserted(wc);

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
      GeodeAwaitility.await().untilAsserted(wc);

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
      GeodeAwaitility.await().untilAsserted(wc);
    } catch (Exception e) {
      Assert.fail(" Test failed due to " + e, e);
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
    GeodeAwaitility.await().untilAsserted(wc);
  }
}
