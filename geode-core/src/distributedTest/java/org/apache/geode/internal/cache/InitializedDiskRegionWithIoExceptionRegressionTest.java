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

import static org.apache.geode.cache.client.ClientRegionShortcut.LOCAL;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.UninterruptibleFileChannel;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * If IOException occurs while updating an entry in an already initialized DiskRegion, then the
 * cache server should be stopped.
 *
 * <p>
 * TRAC #39079: Regions with persistence remain in use after IOException have occurred
 */
@SuppressWarnings("serial")
public class InitializedDiskRegionWithIoExceptionRegressionTest implements Serializable {

  private String uniqueName;
  private String hostName;

  private VM server;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public DistributedDiskDirRule diskDirsRule = new DistributedDiskDirRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getHostName();

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;

    invokeInEveryVM(() -> {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    });

    addIgnoredException(uniqueName);
  }

  @After
  public void tearDown() throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;

    invokeInEveryVM(() -> {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
    });
  }

  @Test
  public void cacheServerPersistWithIOExceptionShouldShutdown() {
    // create server cache
    int port = server.invoke(() -> createServerCache());

    // create cache client
    client.invoke(() -> createClientCache(hostName, port));

    // validate
    server.invoke(() -> validateNoCacheServersRunning());
  }

  private int createServerCache() throws IOException {
    cacheRule.createCache();

    DiskRegionProperties diskRegionProps = new DiskRegionProperties();
    diskRegionProps.setRegionName(uniqueName);
    diskRegionProps.setOverflow(true);
    diskRegionProps.setRolling(true);
    diskRegionProps.setPersistBackup(true);

    DiskRegionHelperFactory.getSyncPersistOnlyRegion(cacheRule.getCache(), diskRegionProps,
        Scope.DISTRIBUTED_ACK);

    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void validateNoCacheServersRunning() throws IOException {
    Region<String, byte[]> region = cacheRule.getCache().getRegion(uniqueName);

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

    List<CacheServer> cacheServers = cacheRule.getCache().getCacheServers();
    assertThat(cacheServers).isEmpty();
  }

  private void createClientCache(String host, int port) {
    clientCacheRule.createClientCache();

    Pool pool = PoolManager.createFactory().addServer(host, port).setSubscriptionEnabled(false)
        .setSubscriptionRedundancy(0).setThreadLocalConnections(true).setMinConnections(0)
        .setReadTimeout(20000).setRetryAttempts(1).create(uniqueName);

    ClientRegionFactory crf = clientCacheRule.getClientCache().createClientRegionFactory(LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(uniqueName);
  }
}
