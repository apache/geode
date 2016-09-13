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
package org.apache.geode.management.internal.pulse;

import static org.apache.geode.distributed.ConfigurationProperties.*;
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
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.MBeanUtil;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This is for testing client IDs
 */
@Category(DistributedTest.class)
public class TestClientIdsDUnitTest extends JUnit4DistributedTestCase {

  private static final String k1 = "k1";
  private static final String k2 = "k2";

  private static final String client_k1 = "client-k1";

  private static final String client_k2 = "client-k2";

  /** name of the test region */
  private static final String REGION_NAME = "ClientHealthStatsDUnitTest_Region";

  private static VM server = null;

  private static VM client = null;

  private static VM client2 = null;

  private static VM managingNode = null;

  private ManagementTestBase helper;

  @Override
  public final void preSetUp() throws Exception {
    this.helper = new ManagementTestBase(){};
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    managingNode = host.getVM(0);
    server = host.getVM(1);
    client = host.getVM(2);
    client2 = host.getVM(3);
  }

  @Override
  public final void preTearDown() throws Exception {
    helper.closeCache(managingNode);
    helper.closeCache(server);
    helper.closeCache(client);
    helper.closeCache(client2);

    disconnectFromDS();
  }

  @Test
  public void testClientIds() throws Exception {
    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);
    int port = (Integer) createServerCache(server);
    DistributedMember serverMember = helper.getMember(server);
    createClientCache(client, NetworkUtils.getServerHostName(server.getHost()), port);
    createClientCache(client2, NetworkUtils.getServerHostName(server.getHost()), port);
    put(client);
    put(client2);
    verifyClientIds(managingNode, serverMember, port);
    helper.stopManagingNode(managingNode);
  }

  @SuppressWarnings("serial")
  private Object createServerCache(VM vm) {
    return vm.invoke(new SerializableCallable("Create Server Cache") {
      public Object call() {
        try {
          return createServerCache();
        } catch (Exception e) {
          fail("Error while createServerCache " + e);
        }
        return null;
      }
    });
  }

  @SuppressWarnings("serial")
  private void createClientCache(VM vm, final String host, final Integer port1) {
    vm.invoke(new SerializableCallable("Create Client Cache") {

      public Object call() {
        try {
          createClientCache(host, port1);
        } catch (Exception e) {
          fail("Error while createClientCache " + e);
        }
        return null;
      }
    });
  }

  private Cache createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    Cache cache = (GemFireCacheImpl) CacheFactory.create(ds);
    assertNotNull(cache);
    return cache;
  }

  private Integer createServerCache(DataPolicy dataPolicy) throws Exception {
    Cache cache = helper.createCache(false);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(dataPolicy);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  public Integer createServerCache() throws Exception {
    return createServerCache(DataPolicy.REPLICATE);
  }

  public Cache createClientCache(String host, Integer port1) throws Exception {

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    Cache cache = createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory()
        .addServer(host, port1.intValue()).setSubscriptionEnabled(false)
        .setThreadLocalConnections(true).setMinConnections(1)
        .setReadTimeout(20000).setPingInterval(10000).setRetryAttempts(1)
        .setSubscriptionEnabled(true).setStatisticInterval(1000)
        .create("CacheServerManagementDUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    return cache;

  }

  /**
   * get member id
   */
  @SuppressWarnings("serial")
  protected static DistributedMember getMember() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return cache.getDistributedSystem().getDistributedMember();
  }

  /**
   * Verify the Cache Server details
   * 
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyClientIds(final VM vm,
      final DistributedMember serverMember, final int serverPort) {
    SerializableRunnable verifyCacheServerRemote = new SerializableRunnable(
        "Verify Cache Server Remote") {
      public void run() {
        try {         
          final WaitCriterion waitCriteria = new WaitCriterion() {
            @Override
            public boolean done() {
              CacheServerMXBean bean = null;
              try {
                bean = MBeanUtil.getCacheServerMbeanProxy(
                    serverMember, serverPort);             
              if (bean != null) {               
                  if( bean.getClientIds().length > 0){
                    return true;
                  }
                } 
              }catch (Exception e) {                 
                LogWriterUtils.getLogWriter().info("exception occured " + e.getMessage() + CliUtil.stackTraceAsString((Throwable)e));
              }
              return false;
            }
            @Override
            public String description() {
              return "wait for getNumOfClients bean to complete and get results";
            }
          };
          Wait.waitForCriterion(waitCriteria, 2 * 60 * 1000, 3000, true);          
          
          //Now it is sure that bean would be available
          CacheServerMXBean bean = MBeanUtil.getCacheServerMbeanProxy(
              serverMember, serverPort);
          LogWriterUtils.getLogWriter().info("verifyClientIds = " + bean.getClientIds().length);
          assertEquals(true, bean.getClientIds().length > 0 ? true : false);
        } catch (Exception e) {
          fail("Error while verifying cache server from remote member " + e);
        }
      }
    };
    vm.invoke(verifyCacheServerRemote);
  }

  /**
   * Verify the Cache Server details
   * 
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void put(final VM vm) {
    SerializableRunnable put = new SerializableRunnable("put") {
      public void run() {
        try {
          Cache cache = GemFireCacheImpl.getInstance();
          Region<Object, Object> r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
          assertNotNull(r1);

          r1.put(k1, client_k1);
          assertEquals(r1.getEntry(k1).getValue(), client_k1);
          r1.put(k2, client_k2);
          assertEquals(r1.getEntry(k2).getValue(), client_k2);
          try {
            Thread.sleep(10000);
          } catch (Exception e) {
            // sleep
          }
          r1.clear();

          r1.put(k1, client_k1);
          assertEquals(r1.getEntry(k1).getValue(), client_k1);
          r1.put(k2, client_k2);
          assertEquals(r1.getEntry(k2).getValue(), client_k2);
          try {
            Thread.sleep(10000);
          } catch (Exception e) {
            // sleep
          }
          r1.clear();
        } catch (Exception ex) {
          Assert.fail("failed while put", ex);
        }
      }

    };
    vm.invoke(put);
  }

}
