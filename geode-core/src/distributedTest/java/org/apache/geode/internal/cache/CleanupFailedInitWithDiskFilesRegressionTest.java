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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Confirms the bug 37241 is fixed. CleanupFailedInitialization on should also clean disk files
 * created
 *
 * <p>
 * TRAC #37241: cleanupFailedInitialization on should also clean disk files created
 */

public class CleanupFailedInitWithDiskFilesRegressionTest extends CacheTestCase {

  private String uniqueName;
  private File[] foldersForServer1;
  private File[] foldersForServer2;
  private File server2Disk2;

  private VM server1;
  private VM server2;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getHost(0).getVM(0);
    server2 = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    File server1Disk1 = temporaryFolder.newFolder(uniqueName + "_server1_disk1");
    File server1Disk2 = temporaryFolder.newFolder(uniqueName + "_server1_disk2");
    foldersForServer1 = new File[] {server1Disk1, server1Disk2};

    File server2Disk1 = temporaryFolder.newFolder(uniqueName + "_server2_disk1");
    server2Disk2 = temporaryFolder.newFolder(uniqueName + "_server2_disk2");
    foldersForServer2 = new File[] {server2Disk1, server2Disk2};
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * 1. Create persistent region server1 with scope global.
   * <p>
   * 2. Try to create persistent region with same name on server2 with scope d-ack.
   * <p>
   * 3. Region creation should fail. Check for all files created in the directory for server 2 gets
   * deleted.
   */
  @Test
  public void newDiskRegionShouldBeCleanedUp() {
    server1.invoke(() -> createRegionOnServer1());

    assertThatThrownBy(() -> server2.invoke(() -> createRegionOnServer2(Scope.DISTRIBUTED_ACK)))
        .isInstanceOf(RMIException.class).hasCauseInstanceOf(IllegalStateException.class);

    addIgnoredException(IllegalStateException.class);
    addIgnoredException(ReplyException.class);
    server2.invoke(() -> validateCleanupOfDiskFiles());
  }

  @Test
  public void recreatedDiskRegionShouldBeCleanedUp() {
    server1.invoke(() -> createRegionOnServer1());
    server2.invoke(() -> createRegionOnServer2(Scope.GLOBAL));
    server2.invoke(() -> closeRegion());

    assertThatThrownBy(() -> server2.invoke(() -> createRegionOnServer2(Scope.DISTRIBUTED_ACK)))
        .isInstanceOf(RMIException.class).hasCauseInstanceOf(IllegalStateException.class);

    addIgnoredException(IllegalStateException.class);
    addIgnoredException(ReplyException.class);
    server2.invoke(() -> validateCleanupOfDiskFiles());
  }

  private void createRegionOnServer1() {
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(foldersForServer1);

    DiskStore diskStore = dsf.create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(diskStore.getName());

    getCache().createRegion(uniqueName, factory.create());
  }

  private void createRegionOnServer2(Scope scope) {
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(foldersForServer2);

    DiskStore diskStore = dsf.create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(scope);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(diskStore.getName());

    getCache().createRegion(uniqueName, factory.create());
  }

  private void closeRegion() {
    getCache().getRegion(uniqueName).close();
  }

  private void validateCleanupOfDiskFiles() {
    await().untilAsserted(() -> assertThat(server2Disk2.listFiles()).hasSize(0));
  }
}
