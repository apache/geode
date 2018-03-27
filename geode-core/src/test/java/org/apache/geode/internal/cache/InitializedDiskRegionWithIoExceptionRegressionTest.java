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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * If IOException occurs while updating an entry in an already initialized DiskRegion, then the
 * cache server should be stopped.
 *
 * <p>
 * TRAC #39079: Regions with persistence remain in use after IOException have occurred
 */
@Category(DistributedTest.class)
public class InitializedDiskRegionWithIoExceptionRegressionTest extends CacheTestCase {

  private String hostName;
  private String uniqueName;

  private File[] serverDiskDirs;

  private VM server;
  private VM client;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getHost(0).getVM(0);
    client = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    serverDiskDirs = new File[] {temporaryFolder.newFolder(uniqueName + "_server1_disk")};

    hostName = getServerHostName(server.getHost());

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;

    invokeInEveryVM(() -> {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    });

    addIgnoredException(uniqueName);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;

    invokeInEveryVM(() -> {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
    });
  }

  @Test
  public void cacheServerPersistWithIOExceptionShouldShutdown() throws Exception {
    // create server cache
    int port = server.invoke(() -> createServerCache());

    // create cache client
    client.invoke(() -> createClientCache(hostName, port));

    // validate
    server.invoke(() -> validateNoCacheServersRunning());
  }

  private int createServerCache() throws IOException {
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(uniqueName);
    props.setOverflow(true);
    props.setRolling(true);
    props.setDiskDirs(serverDiskDirs);
    props.setPersistBackup(true);

    DiskRegionHelperFactory.getSyncPersistOnlyRegion(getCache(), props, Scope.DISTRIBUTED_ACK);

    CacheServer cacheServer = getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void validateNoCacheServersRunning() throws IOException {
    Region<String, byte[]> region = getCache().getRegion(uniqueName);

    region.create("key1", new byte[16]);
    region.create("key2", new byte[16]);

    // Get the oplog handle & hence the underlying file & close it
    UninterruptibleFileChannel oplogFileChannel =
        ((LocalRegion) region).getDiskRegion().testHook_getChild().getFileChannel();
    oplogFileChannel.close();

    assertThatThrownBy(() -> region.put("key2", new byte[16]))
        .isInstanceOf(DiskAccessException.class);

    ((DiskRecoveryStore) region).getDiskStore().waitForClose();
    assertThat(region.getRegionService().isClosed()).isTrue();

    List<CacheServer> cacheServers = getCache().getCacheServers();
    assertThat(cacheServers).isEmpty();
  }

  private void createClientCache(String host, int port) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    getCache(config);

    Pool pool = PoolManager.createFactory().addServer(host, port).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(0).setThreadLocalConnections(true).setMinConnections(0)
        .setReadTimeout(20000).setRetryAttempts(1).create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(pool.getName());

    Region region = getCache().createRegion(uniqueName, factory.create());

    region.registerInterest("ALL_KEYS");
  }
}
