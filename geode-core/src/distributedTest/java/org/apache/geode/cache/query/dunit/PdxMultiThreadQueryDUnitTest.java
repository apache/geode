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

import static org.apache.geode.internal.Assert.fail;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
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
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.version.VersionManager;

@Category({OQLQueryTest.class})
public class PdxMultiThreadQueryDUnitTest extends PDXQueryTestBase {
  public static final Logger logger = LogService.getLogger();
  static final int numberOfEntries = 100;
  final String poolName = "testClientServerQueryPool";
  final String hostName = NetworkUtils.getServerHostName();
  private VM server0;
  private VM server1;
  private VM server2;
  private VM client;
  private int port0;
  private int port1;
  private int port2;

  public PdxMultiThreadQueryDUnitTest() {
    super();
  }

  @Before
  public void startUpServersAndClient() {
    final Host host = Host.getHost(0);

    server0 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    server1 = host.getVM(VersionManager.CURRENT_VERSION, 1);
    server2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    client = host.getVM(VersionManager.CURRENT_VERSION, 3);

    // Start servers
    for (VM vm : Arrays.asList(server0, server1, server2)) {
      vm.invoke((SerializableRunnableIF) this::configAndStartBridgeServer);
    }

    port0 = server0.invoke(PdxQueryDUnitTest::getCacheServerPort);
    port1 = server1.invoke(PdxQueryDUnitTest::getCacheServerPort);
    port2 = server2.invoke(PdxQueryDUnitTest::getCacheServerPort);
  }

  @After
  public void closeServersAndClient() {
    closeClient(client);
    closeClient(server2);
    closeClient(server1);
    closeClient(server0);
  }

  @Test
  public void testClientServerQuery() throws CacheException {
    // create pdx instance at servers
    server0.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "vmware"));
      }
    });

    // Create client region
    client.invoke(() -> {
      ClientCacheFactory cf = new ClientCacheFactory();
      cf.addPoolServer(hostName, port1);
      cf.addPoolServer(hostName, port2);
      cf.addPoolServer(hostName, port0);
      cf.setPdxReadSerialized(false);
      ClientCache clientCache = getClientCache(cf);
      Region<Object, Object> region =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);

      logger.info("### Executing Query on server: " + queryString[1] + ": from client region: "
          + region.getFullPath());
      final int size = 100;
      IntStream.range(0, size).parallel().forEach(a -> {
        try {
          SelectResults<TestObjectThrowsPdxSerializationException> selectResults =
              region.query(queryString[1]);
          assertThat(selectResults.size()).isEqualTo(numberOfEntries);
        } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
            | QueryInvocationTargetException e) {
          e.printStackTrace();
        }
      });
      await().until(() -> TestObject.numInstance.get() == size * numberOfEntries);
    });
  }

  @Test
  public void testClientServerQueryUsingRemoteQueryService()
      throws CacheException, InterruptedException {
    // create pdx instance at servers
    server0.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObject(i, "vmware"));
      }
    });

    // Create client pool.
    createPool(client, poolName, new String[] {hostName, hostName, hostName},
        new int[] {port0, port1, port2}, true);

    final int size = 100;
    AsyncInvocation[] asyncInvocationArray = new AsyncInvocation[size];
    for (int i = 0; i < size; i++) {
      asyncInvocationArray[i] =
          client.invokeAsync(() -> {
            QueryService remoteQueryService = (PoolManager.find(poolName)).getQueryService();
            logger.info("### Executing Query on server: " + queryString[1]);
            Query query = remoteQueryService.newQuery(queryString[1]);
            SelectResults<TestObjectThrowsPdxSerializationException> selectResults =
                uncheckedCast(query.execute());
            assertThat(selectResults.size()).isEqualTo(numberOfEntries);
          });
    }

    for (int i = 0; i < size; i++) {
      asyncInvocationArray[i].await();
    }
    client
        .invoke(() -> await().until(() -> TestObject.numInstance.get() == size * numberOfEntries));
  }

  @Test
  public void testRetrySucceedWithPdxSerializationException() throws CacheException {
    // create pdx instance at servers
    server0.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObjectThrowsPdxSerializationException());
      }
    });

    // Create client pool with 3 servers
    createPool(client, poolName, new String[] {hostName, hostName, hostName},
        new int[] {port0, port1, port2}, true);

    client.invoke(() -> {
      try {
        TestObjectThrowsPdxSerializationException.throwExceptionOnDeserialization = true;
        QueryService remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        logger.info("### Executing Query on server: " + queryString[1]);
        Query query = remoteQueryService.newQuery(queryString[1]);
        SelectResults<TestObjectThrowsPdxSerializationException> selectResults =
            uncheckedCast(query.execute());
        assertThat(selectResults.size()).isEqualTo(numberOfEntries);
        // the 2 failed try incremented numInstance
        assertThat(numberOfEntries + 2)
            .isEqualTo(TestObjectThrowsPdxSerializationException.numInstance.get());
      } finally {
        assertThat(TestObjectThrowsPdxSerializationException.throwExceptionOnDeserialization)
            .isFalse();
        TestObjectThrowsPdxSerializationException.numInstance.set(0);
      }
    });
  }

  @Test
  public void testRetryFailedWithServerConnectivityException() throws CacheException {
    // create pdx instance at servers
    server0.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < numberOfEntries; i++) {
        region.put("key-" + i, new TestObjectThrowsPdxSerializationException());
      }
    });

    // Create client pool with only 2 servers to test that retry will not run forever
    createPool(client, poolName, new String[] {hostName, hostName},
        new int[] {port0, port1}, true);

    client.invoke(() -> {
      try {
        TestObjectThrowsPdxSerializationException.throwExceptionOnDeserialization = true;
        QueryService remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        logger.info("### Executing Query on server: " + queryString[1]);
        Query query = remoteQueryService.newQuery(queryString[1]);
        SelectResults<TestObjectThrowsPdxSerializationException> selectResults =
            uncheckedCast(query.execute());
        fail("Expect ServerConnectivityException");
        assertThat(selectResults.size()).isEqualTo(0);
      } catch (ServerConnectivityException sce) {
        logger.info("Expect ServerConnectivityException after tried 2 servers");
      } finally {
        assertThat(TestObjectThrowsPdxSerializationException.numInstance.get()).isEqualTo(2);
        assertThat(TestObjectThrowsPdxSerializationException.throwExceptionOnDeserialization)
            .isFalse();
        TestObjectThrowsPdxSerializationException.numInstance.set(0);
      }
    });
  }

  public static class TestObjectThrowsPdxSerializationException implements PdxSerializable {
    private static boolean throwExceptionOnDeserialization = false;
    public static AtomicInteger numInstance = new AtomicInteger();

    public TestObjectThrowsPdxSerializationException() {
      numInstance.incrementAndGet();
    }

    @Override
    public void toData(PdxWriter writer) {}

    @Override
    public void fromData(PdxReader reader) {
      if (throwExceptionOnDeserialization) {
        if (numInstance.get() >= 2) {
          // after retried 2 servers, let the retry to 3rd server succeed
          throwExceptionOnDeserialization = false;
        }
        throw new PdxSerializationException("Deserialization should not be happening in this VM");
      }
    }
  }
}
