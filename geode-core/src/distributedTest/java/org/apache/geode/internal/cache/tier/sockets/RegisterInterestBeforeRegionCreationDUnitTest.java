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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheObserverAdapter;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This test tests the scenario whereby a register interest has been called before the region has
 * been created. After that if a region is created and the new region gets data from another
 * mirrored node, it should propagate to the client which registered interest before the region was
 * created.
 *
 * The scenario is: - 2 servers 2 clients - client1 connected to server1 - client2 connected to
 * server2 - client2 registers interest for region1 on server2 before region1 is created - data is
 * put on region1 in server1 - mirrored region1 is created on server2 - data will come to region1 on
 * server2 via GII - data should be sent to client2
 */
@Category({ClientSubscriptionTest.class})
public class RegisterInterestBeforeRegionCreationDUnitTest extends JUnit4DistributedTestCase {

  /** Server1 VM **/
  static VM server1 = null;
  /** Server2 VM **/
  static VM server2 = null;
  /** Client1 VM **/
  static VM client1 = null;
  /** Client2 VM **/
  static VM client2 = null;
  /** Server1 port **/
  public static int PORT1;
  /** Server2 port **/
  public static int PORT2;
  /** Region name **/
  private static final String REGION_NAME =
      RegisterInterestBeforeRegionCreationDUnitTest.class.getSimpleName() + "_Region";
  /** Server2 VM **/
  protected static Cache cache = null;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
    CacheObserverHolder.setInstance(new CacheObserverAdapter());
  }

  /**
   * close the cache on all the vms
   */
  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(() -> RegisterInterestBeforeRegionCreationDUnitTest.closeCache());
    client2.invoke(() -> RegisterInterestBeforeRegionCreationDUnitTest.closeCache());
    server1.invoke(() -> RegisterInterestBeforeRegionCreationDUnitTest.closeCache());
    server2.invoke(() -> RegisterInterestBeforeRegionCreationDUnitTest.closeCache());
  }

  /**
   * - Creates the client-server configuration (which also registers interest) - put on server1 -
   * verify puts received on client1 - create region on server2 - verify puts received on server2
   * via GII - verify puts received on client2 via server2
   */
  @Ignore("TODO:YOGESH: test is disabled")
  @Test
  public void testRegisterInterestHappeningBeforeRegionCreation() throws Exception {
    createClientServerConfigurationForClearTest();
    server1.invoke(putFromServer());
    client1.invoke(verifyIfAllPutsGot());
    server2.invoke(createRegionOnServer());
    server2.invoke(verifyIfAllPutsGot());
    client2.invoke(verifyIfAllPutsGot());
  }

  private CacheSerializableRunnable putFromServer() {
    CacheSerializableRunnable putFromServer = new CacheSerializableRunnable("putFromServer") {
      public void run2() throws CacheException {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        for (int i = 0; i < 1000; i++) {
          region.put("key" + i, "value" + i);
        }
      }
    };
    return putFromServer;
  }


  private CacheSerializableRunnable verifyIfAllPutsGot() {
    CacheSerializableRunnable putFromServer =
        new CacheSerializableRunnable("createRegionOnServer") {
          public void run2() throws CacheException {
            final Region region = cache.getRegion(SEPARATOR + REGION_NAME);
            assertNotNull(region);
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                return region.size() == 1000;
              }

              public String description() {
                return null;
              }
            };
            GeodeAwaitility.await().untilAsserted(ev);
          }
        };
    return putFromServer;
  }


  private CacheSerializableRunnable createRegionOnServer() {
    CacheSerializableRunnable putFromServer =
        new CacheSerializableRunnable("createRegionOnServer") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setMirrorType(MirrorType.KEYS_VALUES);
            RegionAttributes attrs = factory.createRegionAttributes();
            cache.createVMRegion(REGION_NAME, attrs);
          }
        };
    return putFromServer;
  }

  // function to create 2servers and 2 clients
  private void createClientServerConfigurationForClearTest() throws Exception {
    // create server and region
    PORT1 = ((Integer) server1.invoke(
        () -> RegisterInterestBeforeRegionCreationDUnitTest.createServer(new Boolean(true))))
            .intValue();

    // only create server, no region
    PORT2 = ((Integer) server2.invoke(
        () -> RegisterInterestBeforeRegionCreationDUnitTest.createServer(new Boolean(false))))
            .intValue();

    // client1 connected to server1
    client1.invoke(() -> RegisterInterestBeforeRegionCreationDUnitTest
        .createClient(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));

    // client2 connected to server2
    client2.invoke(() -> RegisterInterestBeforeRegionCreationDUnitTest
        .createClient(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT2)));
  }

  public static Integer createServer(Boolean createRegion) throws Exception {
    new RegisterInterestBeforeRegionCreationDUnitTest().createCache(new Properties());
    boolean isCreateRegion = createRegion.booleanValue();
    if (isCreateRegion) {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setMirrorType(MirrorType.KEYS_VALUES);
      RegionAttributes attrs = factory.createRegionAttributes();
      cache.createVMRegion(REGION_NAME, attrs);
    }
    CacheServerImpl server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClient(String host, Integer port1) throws Exception {
    PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new RegisterInterestBeforeRegionCreationDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, PORT1).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1).setReadTimeout(2000).setSocketBufferSize(1000)
        .setMinConnections(2)
        // retryAttempts 2
        // retryInterval 250
        .create("RegisterInterestBeforeRegionCreationDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.createRegionAttributes();
    cache.createVMRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
