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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.version.VersionManager;

@Category({OQLQueryTest.class})
public class PdxMultiThreadQueryDUnitTest extends PDXQueryTestBase {
  public static final Logger logger = LogService.getLogger();

  static final int numberOfEntries = 100;

  public PdxMultiThreadQueryDUnitTest() {
    super();
  }

  @Test
  public void testClientServerQuery() throws CacheException {
    final Host host = Host.getHost(0);
    final String hostName = NetworkUtils.getServerHostName();

    VM vm0 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM vm1 = host.getVM(VersionManager.CURRENT_VERSION, 1);
    VM vm2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM vm3 = host.getVM(VersionManager.CURRENT_VERSION, 3);

    // Start servers
    for (VM vm : Arrays.asList(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        configAndStartBridgeServer();
      });
    }

    // create pdx instance at servers
    vm0.invoke(() -> {
      Region region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "vmware"));
      }
    });

    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port2 = vm2.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    // Create client region
    vm3.invoke(() -> {
      ClientCacheFactory cf = new ClientCacheFactory();
      cf.addPoolServer(hostName, port1);
      cf.addPoolServer(hostName, port2);
      cf.addPoolServer(hostName, port0);
      cf.setPdxReadSerialized(false);
      ClientCache clientCache = getClientCache(cf);
      Region region =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

      logger.info("### Executing Query on server: " + queryString[1] + ": from client region: "
          + region.getFullPath());
      final int size = 100;
      int[] array = new int[size];
      for (int i = 0; i < size; i++) {
        array[i] = i;
      }
      Arrays.stream(array).parallel().forEach(a -> {
        try {
          SelectResults rs2 = region.query(queryString[1]);
          assertEquals(numberOfEntries, rs2.size());
        } catch (FunctionDomainException e) {
          e.printStackTrace();
        } catch (TypeMismatchException e) {
          e.printStackTrace();
        } catch (NameResolutionException e) {
          e.printStackTrace();
        } catch (QueryInvocationTargetException e) {
          e.printStackTrace();
        }
      });
      await().until(() -> TestObject.numInstance.get() == size * numberOfEntries);
    });

    this.closeClient(vm3);
    this.closeClient(vm2);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  @Test
  public void testClientServerQueryUsingRemoteQueryService()
      throws CacheException, InterruptedException {
    final Host host = Host.getHost(0);
    final String hostName = NetworkUtils.getServerHostName();

    VM vm0 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM vm1 = host.getVM(VersionManager.CURRENT_VERSION, 1);
    VM vm2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM vm3 = host.getVM(VersionManager.CURRENT_VERSION, 3);

    // Start servers
    for (VM vm : Arrays.asList(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        configAndStartBridgeServer();
      });
    }

    // create pdx instance at servers
    vm0.invoke(() -> {
      Region region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "vmware"));
      }
    });

    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port2 = vm2.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm3, poolName, new String[] {hostName}, new int[] {port1, port2, port0}, true);

    final int size = 100;
    AsyncInvocation[] asyncInvocationArray = new AsyncInvocation[size];
    for (int i = 0; i < size; i++) {
      asyncInvocationArray[i] =
          vm3.invokeAsync(() -> {
            ClientCache clientCache = getClientCache();
            QueryService remoteQueryService = (PoolManager.find(poolName)).getQueryService();
            logger.info("### Executing Query on server: " + queryString[1]);
            Query query = remoteQueryService.newQuery(queryString[1]);
            SelectResults rs2 = (SelectResults) query.execute();
            assertEquals(numberOfEntries, rs2.size());
          });
    }

    for (int i = 0; i < size; i++) {
      asyncInvocationArray[i].await();
    }
    vm3.invoke(() -> {
      await().until(() -> TestObject.numInstance.get() == size * numberOfEntries);
    });

    this.closeClient(vm3);
    this.closeClient(vm2);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  @Test
  public void testRetryWithPdxSerializationException()
      throws CacheException, InterruptedException {
    final Host host = Host.getHost(0);
    final String hostName = NetworkUtils.getServerHostName();

    VM vm0 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    VM vm1 = host.getVM(VersionManager.CURRENT_VERSION, 1);
    VM vm2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM vm3 = host.getVM(VersionManager.CURRENT_VERSION, 3);

    // Start servers
    for (VM vm : Arrays.asList(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        configAndStartBridgeServer();
      });
    }

    // create pdx instance at servers
    vm0.invoke(() -> {
      Region region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestPdxSerializable());
      }
    });

    final int port0 = vm0.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port1 = vm1.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());
    final int port2 = vm2.invoke(() -> PdxQueryDUnitTest.getCacheServerPort());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm3, poolName, new String[] {hostName, hostName, hostName},
        new int[] {port0, port1, port2}, true);

    try {
      vm3.invoke(() -> {
        TestPdxSerializable.throwExceptionOnDeserialization = true;
        QueryService remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        logger.info("### Executing Query on server: " + queryString[1]);
        Query query = remoteQueryService.newQuery(queryString[1]);
        SelectResults rs2 = (SelectResults) query.execute();
        assertEquals(numberOfEntries, rs2.size());
      });
    } finally {
      vm3.invoke(() -> {
        assertEquals(false, TestPdxSerializable.throwExceptionOnDeserialization);
        // the failed try will increment numInstance once
        assertEquals(numberOfEntries + 1, TestPdxSerializable.numInstance.get());
        TestPdxSerializable.numInstance.set(0);
      });
    }

    this.closeClient(vm3);
    this.closeClient(vm2);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  public static class TestPdxSerializable implements PdxSerializable {
    private static boolean throwExceptionOnDeserialization = false;
    public static AtomicInteger numInstance = new AtomicInteger();

    public TestPdxSerializable() {
      numInstance.incrementAndGet();
    }

    @Override
    public void toData(PdxWriter writer) {}

    @Override
    public void fromData(PdxReader reader) {
      if (throwExceptionOnDeserialization) {
        throwExceptionOnDeserialization = false;
        throw new PdxSerializationException("Deserialization should not be happening in this VM");
      }
    }
  }
}
