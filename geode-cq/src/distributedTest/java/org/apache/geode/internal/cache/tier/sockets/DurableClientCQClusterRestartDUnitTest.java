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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class DurableClientCQClusterRestartDUnitTest implements Serializable {
  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server;
  private VM locator;
  private VM client;
  private File locatorLog;
  private File serverLog;
  private File restartLocatorLog;
  private File restartServerLog;
  private int locatorPort;
  private String durableClientId;
  private final String cqName = "cqQuery";
  private final int numOfInvocations = 5;
  private final int uniquePort = new UniquePortSupplier().getAvailablePort();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedExecutorServiceRule executorServiceRule = new DistributedExecutorServiceRule();

  @Before
  public void setup() throws Exception {
    locator = VM.getVM(0);
    server = VM.getVM(1);
    client = VM.getVM(2);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    locatorLog = new File(temporaryFolder.newFolder(uniqueName), "locator.log");
    serverLog = temporaryFolder.getRoot().toPath().resolve("server.log").toFile();
    restartLocatorLog = temporaryFolder.getRoot().toPath().resolve("locator_restart.log").toFile();
    restartServerLog = temporaryFolder.getRoot().toPath().resolve("server_restart.log").toFile();
    durableClientId = uniqueName + "client";
  }

  @Test
  public void cqEventsNotLostIfClusterRestarted() {
    startClusterAndDoFunctionCalls(locatorLog, serverLog, 1);

    // restart cluster and perform another round of function calls.
    startClusterAndDoFunctionCalls(restartLocatorLog, restartServerLog, 2);

  }

  private void startClusterAndDoFunctionCalls(File locatorLog, File serverLog, int iteration) {
    locatorPort = locator.invoke(() -> startLocator(locatorLog));
    server.invoke(() -> createCacheServer(serverLog));
    server.invoke(this::createDiskRegionIfNotExist);
    client.invoke(this::setClientRegion);
    client.invoke(() -> clientCacheRule.getClientCache().readyForEvents());
    client.invoke(this::callFunctions);
    client.invoke(() -> verifyCQListenerInvocations(numOfInvocations * iteration));
    server.invoke(this::closeCache);
    server.bounceForcibly();
    locator.invoke(this::stopLocator);
  }

  private int startLocator(File locatorLog) throws IOException {
    InetAddress bindAddress = InetAddress.getByName(hostName);
    Locator locator =
        Locator.startLocatorAndDS(locatorPort, locatorLog, bindAddress, new Properties());
    return locator.getPort();
  }

  private void createCacheServer(File logFile) throws Exception {
    Properties systemProperties = new Properties();
    systemProperties.setProperty("gemfire.jg-bind-port", Integer.toString(uniquePort));
    cacheRule.createCache(createServerConfig(logFile), systemProperties);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
  }

  private Properties createServerConfig(File logFile) {
    Properties config = new Properties();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());
    return config;
  }

  private void closeCache() {
    cacheRule.closeAndNullCache();
  }

  private void stopLocator() {
    Locator.getLocator().stop();
  }

  private void createDiskRegionIfNotExist() throws IOException {
    if (cacheRule.getCache().getRegion(regionName) != null) {
      return;
    }
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    DiskStoreFactory diskStoreFactory =
        cacheRule.getCache().createDiskStoreFactory(diskStoreAttributes);
    diskStoreFactory.setDiskDirs(new File[] {createOrGetDir()});
    diskStoreFactory.create(getDiskStoreName());

    RegionFactory regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE_PERSISTENT);
    regionFactory.setDiskStoreName(getDiskStoreName());
    regionFactory.create(regionName);
  }

  private File createOrGetDir() throws IOException {
    File dir = new File(temporaryFolder.getRoot(), getDiskStoreName());
    if (!dir.exists()) {
      dir = temporaryFolder.newFolder(getDiskStoreName());
    }
    return dir;
  }

  private String getDiskStoreName() {
    return getClass().getSimpleName() + VM.getVMId();
  }

  private void callFunctions() throws Exception {
    InternalRegion region = (InternalRegion) clientCacheRule.getClientCache().getRegion(regionName);
    Pool pool = region.getServerProxy().getPool();

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numOfInvocations; i++) {
      futures.add(executorServiceRule.submit(() -> invokeFunction(pool)));
    }
    for (Future<Void> future : futures) {
      future.get(getTimeout().toMillis(), MILLISECONDS);
    }
  }

  private void invokeFunction(Pool pool) {
    @SuppressWarnings("unchecked")
    Execution execution = FunctionService.onServer(pool).setArguments(regionName);
    ResultCollector resultCollector = execution.execute(new TestFunction());
    resultCollector.getResult();
  }

  private void setClientRegion() throws Exception {
    ClientCache clientCache = clientCacheRule.getClientCache();
    if (clientCache == null) {
      Properties config = new Properties();
      config.setProperty(DURABLE_CLIENT_ID, durableClientId);
      clientCacheRule.createClientCache(config);
    }

    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    if (region != null) {
      return;
    }
    Pool pool = PoolManager.createFactory()
        .addLocator(hostName, locatorPort)
        .setSubscriptionEnabled(true)
        .create(uniqueName);

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);

    registerCQ();
  }

  private void registerCQ() throws Exception {
    ClientCache clientCache = clientCacheRule.getClientCache();

    QueryService queryService = clientCache.getQueryService();
    CqAttributesFactory cqaf = new CqAttributesFactory();
    cqaf.addCqListener(new TestCqListener());
    CqAttributes cqAttributes = cqaf.create();

    queryService.newCq(cqName, "Select * from " + SEPARATOR + regionName,
        cqAttributes, true).executeWithInitialResults();
  }

  private void verifyCQListenerInvocations(int expected) {
    QueryService cqService = clientCacheRule.getClientCache().getQueryService();
    await().untilAsserted(() -> {
      CqListener cqListener = cqService.getCq(cqName).getCqAttributes().getCqListener();
      assertThat(((TestCqListener) cqListener).numEvents.get()).isEqualTo(expected);
    });
  }

  public static class TestFunction implements Function, DataSerializable {
    private final Random random = new Random();

    @Override
    @SuppressWarnings("unchecked")
    public void execute(FunctionContext context) {
      CqService cqService = ((InternalCache) context.getCache()).getCqService();
      waitUntilCqRegistered(cqService);

      String regionName = context.getArguments().toString();
      Region<Object, Object> region = context.getCache().getRegion(regionName);
      int key = random.nextInt();
      region.put(key, key);
      context.getResultSender().lastResult(true);
    }

    private void waitUntilCqRegistered(CqService cqService) {
      await().untilAsserted(() -> assertThat(cqService.getAllCqs().size()).isGreaterThan(0));
    }

    @Override
    public void toData(DataOutput out) {}

    @Override
    public void fromData(DataInput in) {}
  }

  private static class TestCqListener implements CqListener, Serializable {
    AtomicInteger numEvents = new AtomicInteger();

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents.incrementAndGet();
    }

    @Override
    public void onError(CqEvent aCqEvent) {}
  }
}
