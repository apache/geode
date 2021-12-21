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
package org.apache.geode.internal.cache.partitioned;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.test.junit.categories.RegionsTest;

/**
 * Tests the basic use cases for PR persistence.
 */
@Category({RegionsTest.class})
public class PersistPRKRFIntegrationTest {
  private static final String REGION_NAME = "testRegion";
  private static final String DISK_STORE_NAME = "testRegionDiskStore";
  private static final int BUCKETS = 1;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private InternalCache cache;
  private Region<String, String> testRegion;
  private BlockingWriter<String, String> blockingWriter;

  @Before
  public void setup() throws IOException {
    cache = (InternalCache) new CacheFactory().create();
    cache.createDiskStoreFactory().setDiskDirs(new File[] {tempFolder.newFolder("diskDir")})
        .create(DISK_STORE_NAME);
    PartitionAttributesImpl partitionAttributes = new PartitionAttributesImpl();
    partitionAttributes.setTotalNumBuckets(BUCKETS);
    testRegion = cache.<String, String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
        .setPartitionAttributes(partitionAttributes).setDiskStoreName(DISK_STORE_NAME)
        .create(REGION_NAME);

    blockingWriter = new BlockingWriter<>();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void closeDiskStoreDuringCreate() throws InterruptedException {
    testRegion.getAttributesMutator().setCacheWriter(blockingWriter);
    Future<Void> asyncCreate = CompletableFuture.runAsync(() -> testRegion.put("newKey", "value"));
    blockingWriter.awaitCreateInProgress();
    cache.closeDiskStores();
    blockingWriter.allowCreates();
    assertThatThrownBy(asyncCreate::get).hasRootCauseInstanceOf(CacheClosedException.class)
        .hasMessageContaining("The disk store is closed");
  }

  @Test
  public void closeDiskStoreDuringUpdate() throws InterruptedException {
    testRegion.put("existingKey", "value");
    testRegion.getAttributesMutator().setCacheWriter(blockingWriter);
    Future<Void> asyncUpdate =
        CompletableFuture.runAsync(() -> testRegion.put("existingKey", "newValue"));
    blockingWriter.awaitUpdateInProgress();
    cache.closeDiskStores();
    blockingWriter.allowUpdates();
    assertThatThrownBy(asyncUpdate::get).hasRootCauseInstanceOf(CacheClosedException.class)
        .hasMessageContaining("The disk store is closed");
  }

  @Test
  public void closeDiskStoreDuringDestroy() throws InterruptedException {
    testRegion.put("existingKey", "value");
    testRegion.getAttributesMutator().setCacheWriter(blockingWriter);
    Future<Void> asyncDestroy = CompletableFuture.runAsync(() -> testRegion.remove("existingKey"));
    blockingWriter.awaitDestroyInProgress();
    cache.closeDiskStores();
    blockingWriter.allowDestroys();
    assertThatThrownBy(asyncDestroy::get).hasRootCauseInstanceOf(CacheClosedException.class)
        .hasMessageContaining("The disk store is closed");
  }

  private static class BlockingWriter<K, V> extends CacheWriterAdapter<K, V> implements Declarable {
    private final CountDownLatch beforeCreateLatch = new CountDownLatch(1);
    private final CountDownLatch allowCreates = new CountDownLatch(1);
    private final CountDownLatch beforeUpdateLatch = new CountDownLatch(1);
    private final CountDownLatch allowUpdates = new CountDownLatch(1);
    private final CountDownLatch beforeDestroyLatch = new CountDownLatch(1);
    private final CountDownLatch allowDestroys = new CountDownLatch(1);

    @Override
    public void beforeCreate(EntryEvent event) {
      try {
        beforeCreateLatch.countDown();
        allowCreates.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void beforeDestroy(EntryEvent event) {
      try {
        beforeDestroyLatch.countDown();
        allowDestroys.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void beforeUpdate(EntryEvent event) {
      try {
        beforeUpdateLatch.countDown();
        allowUpdates.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    void allowCreates() {
      allowCreates.countDown();
    }

    void awaitCreateInProgress() throws InterruptedException {
      beforeCreateLatch.await();
    }

    void allowDestroys() {
      allowDestroys.countDown();
    }

    void awaitDestroyInProgress() throws InterruptedException {
      beforeDestroyLatch.await();
    }

    void allowUpdates() {
      allowUpdates.countDown();
    }

    void awaitUpdateInProgress() throws InterruptedException {
      beforeUpdateLatch.await();
    }
  }
}
