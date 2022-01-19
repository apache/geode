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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.util.DelayedAction;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * TODO:GEODE-4935: why does this test NOT actually involve disk recover?
 *
 * <p>
 * TRAC #45934: AssertionError: Version stamp should have a member at this point for entry ...
 *
 * <p>
 * Occurs when a crash and recovery from disk in one VM while another one is spooling up its cache.
 * The one that is starting attempts a GII from the one that recovers and throws an exception when
 * the initial image version tags do not contain membership IDs.
 *
 * <pre>
 * java.lang.AssertionError: Version stamp should have a member at this point for entry VersionedThinDiskLRURegionEntry@83cf00 (key=3; rawValue=VMCachedDeserializable@25892505; version={v0; rv0; ds=0; time=0};member=null)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.Oplog.create(Oplog.java:3600)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.DiskStoreImpl.put(DiskStoreImpl.java:638)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.DiskRegion.put(DiskRegion.java:306)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.DiskEntry$Helper.writeToDisk(DiskEntry.java:501)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.DiskEntry$Helper.update(DiskEntry.java:603)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.AbstractDiskRegionEntry.setValue(AbstractDiskRegionEntry.java:105)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.AbstractRegionEntry.initialImageInit(AbstractRegionEntry.java:673)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.AbstractRegionMap.initialImagePut(AbstractRegionMap.java:893)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.InitialImageOperation.processChunk(InitialImageOperation.java:681)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.InitialImageOperation$ImageProcessor.process(InitialImageOperation.java:874)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.ReplyMessage.process(ReplyMessage.java:207)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.internal.cache.InitialImageOperation$ImageReplyMessage.process(InitialImageOperation.java:1683)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.ReplyMessage.dmProcess(ReplyMessage.java:185)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.ReplyMessage.process(ReplyMessage.java:174)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.DistributionMessage.scheduleAction(DistributionMessage.java:301)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.DistributionMessage$1.run(DistributionMessage.java:364)
 *     at Remote Member 'frodo(30627)<v17>:55972' in java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
 *     at Remote Member 'frodo(30627)<v17>:55972' in java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:684)
 *     at Remote Member 'frodo(30627)<v17>:55972' in com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:992)
 *     at Remote Member 'frodo(30627)<v17>:55972' in java.lang.Thread.run(Thread.java:662)
 *     at com.gemstone.gemfire.distributed.internal.ReplyException.handleAsUnexpected(ReplyException.java:79)
 *     at com.gemstone.gemfire.internal.cache.InitialImageOperation.getFromOne(InitialImageOperation.java:328)
 *     at com.gemstone.gemfire.internal.cache.DistributedRegion.getInitialImageAndRecovery(DistributedRegion.java:1330)
 *     at com.gemstone.gemfire.internal.cache.DistributedRegion.initialize(DistributedRegion.java:1085)
 *     at com.gemstone.gemfire.internal.cache.GemFireCacheImpl.createVMRegion(GemFireCacheImpl.java:2603)
 *     at com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue.initializeRegion(SingleWriteSingleReadRegionQueue.java:1043)
 *     at com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue.<init>(SingleWriteSingleReadRegionQueue.java:226)
 *     at com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue.<init>(SingleWriteSingleReadRegionQueue.java:174)
 *     at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.initializeMessageQueue(GatewayImpl.java:1486)
 *     at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.<init>(GatewayImpl.java:1225)
 *     at com.gemstone.gemfire.internal.cache.GatewayImpl.initializeEventProcessor(GatewayImpl.java:968)
 *     at com.gemstone.gemfire.internal.cache.GatewayImpl.start(GatewayImpl.java:637)
 *     at com.gemstone.gemfire.internal.cache.GatewayImpl.start(GatewayImpl.java:585)
 *     at com.gemstone.gemfire.internal.cache.GatewayHubImpl.startGateways(GatewayHubImpl.java:604)
 *     at com.gemstone.gemfire.internal.cache.GatewayHubImpl.start(GatewayHubImpl.java:591)
 *     at com.gemstone.gemfire.internal.cache.GatewayHubImpl.start(GatewayHubImpl.java:529)
 *     at com.gemstone.gemfire.internal.cache.persistence.PersistentGatewayDUnitTest$5.run(PersistentGatewayDUnitTest.java:209)
 *     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 *     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
 *     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
 *     at java.lang.reflect.Method.invoke(Method.java:597)
 *     at hydra.MethExecutor.executeObject(MethExecutor.java:258)
 *     at hydra.RemoteTestModule.executeMethodOnObject(RemoteTestModule.java:269)
 *     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 *     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
 *     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
 *     at java.lang.reflect.Method.invoke(Method.java:597)
 *     at sun.rmi.server.UnicastServerRef.dispatch(UnicastServerRef.java:305)
 *     at sun.rmi.transport.Transport$1.run(Transport.java:159)
 *     at java.security.AccessController.doPrivileged(Native Method)
 *     at sun.rmi.transport.Transport.serviceCall(Transport.java:155)
 *     at sun.rmi.transport.tcp.TCPTransport.handleMessages(TCPTransport.java:535)
 *     at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run0(TCPTransport.java:790)
 *     at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run(TCPTransport.java:649)
 *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
 *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
 *     at java.lang.Thread.run(Thread.java:662) from com.gemstone.gemfire.internal.cache.persistence.PersistentGatewayDUnitTest$5.run with 0 args : "Create gateway region" (took 1277 ms)
 * </pre>
 */

public class DiskRecoveryWithVersioningGiiRegressionTest extends CacheTestCase {

  private static final int ENTRY_COUNT = 1000;

  private String uniqueName;
  private File diskDir;

  private VM serverWithDisk;
  private VM server;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    serverWithDisk = getHost(0).getVM(1);
    server = getHost(0).getVM(2);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    diskDir = temporaryFolder.newFolder(uniqueName + "_serverWithDisk_disk");

    serverWithDisk.invoke(this::createCacheWithDisk);
    server.invoke(this::createCache);
  }

  @After
  public void tearDown() throws Exception {
    AbstractUpdateOperation.test_InvalidVersion = false;
    DistributedCacheOperation.test_InvalidVersionAction = null;

    disconnectAllFromDS();
  }

  @Test
  public void giiFromMemberDoingDiskRecovery() throws Exception {
    serverWithDisk.invoke(() -> {
      AbstractUpdateOperation.test_InvalidVersion = true;
    });

    server.invoke(() -> {
      DistributedCacheOperation.test_InvalidVersionAction =
          new DelayedAction(this::unsetRemoteFlagInServerWithDisk);
      DistributedCacheOperation.test_InvalidVersionAction.allowToProceed();

      putEntries();
      validateRegionContents();
    });

    serverWithDisk.invoke(this::validateRegionContents);
  }

  private void createCacheWithDisk() {
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[] {diskDir});

    RegionFactory regionFactory = getCache().createRegionFactory(REPLICATE_PERSISTENT);
    regionFactory.setDiskStoreName(dsf.create(uniqueName).getName());
    regionFactory.create(uniqueName);
  }

  private void createCache() {
    RegionFactory<Integer, Integer> regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.create(uniqueName);
  }

  private void unsetRemoteFlagInServerWithDisk() {
    serverWithDisk.invoke(() -> AbstractUpdateOperation.test_InvalidVersion = false);
  }

  private void putEntries() {
    Region<Integer, Integer> region = getCache().getRegion(uniqueName);

    Map<Integer, Integer> values = new HashMap<>();
    for (int i = 0; i < ENTRY_COUNT; i++) {
      values.put(i, i);
    }
    region.putAll(values);
  }

  private void validateRegionContents() {
    Region<Integer, Integer> region = getCache().getRegion(uniqueName);

    assertThat(region.size()).isEqualTo(ENTRY_COUNT);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      assertThat(region.get(i)).isEqualTo(i);
    }
  }
}
