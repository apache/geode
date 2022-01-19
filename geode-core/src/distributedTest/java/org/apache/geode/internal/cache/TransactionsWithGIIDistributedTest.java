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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;

public class TransactionsWithGIIDistributedTest implements Serializable {
  private static final Logger logger = LogService.getLogger();
  private static final long TIMEOUT_SECONDS = GeodeAwaitility.getTimeout().getSeconds();

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;
  private final String regionName = "region";
  private final int numOfEntry = 2500;
  private final int workers = 31;
  private final ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
  }

  @Test
  public void correctVersionGeneratedForConcurrentOperationsInTxWithRebalance() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(regionName));
    vm0.invoke(() -> doPut("A"));
    vm0.invoke(this::doDestroy);
    vm0.invoke(() -> doPut("B"));

    vm1.invoke(() -> createPartitionedRegion(regionName));
    doConcurrentOpsAndRebalance("C", true);
    validateVersionsInVms(true, vm0, vm1);

    vm2.invoke(() -> createPartitionedRegion(regionName));
    doConcurrentOpsAndRebalance("D", true);
    validateVersionsInVms(true, vm0, vm1, vm2);

    vm3.invoke(() -> createPartitionedRegion(regionName));
    doConcurrentOpsAndRebalance("E", true);
    validateVersionsInVms(true, vm0, vm1, vm2, vm3);
  }

  private void createPartitionedRegion(String regionName) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);
    partitionAttributesFactory.setTotalNumBuckets(workers);

    RegionFactory regionFactory = cacheRule.getOrCreateCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void doConcurrentOpsAndRebalance(String s, boolean doRebalance) throws Exception {
    AsyncInvocation async0 = vm0.invokeAsync(this::doConcurrentDestroyInTx);
    AsyncInvocation async1;
    if (doRebalance) {
      async1 = vm1.invokeAsync(() -> doConcurrentPutInTx(s));
    } else {
      async1 = vm0.invokeAsync(() -> doConcurrentPutInTx(s));
    }
    if (doRebalance) {
      vm0.invoke(this::doRebalance);
    }
    async0.await();
    async1.await();
  }

  private void doRebalance() throws TimeoutException, InterruptedException {
    InternalResourceManager manager = cacheRule.getCache().getInternalResourceManager();
    manager.createRebalanceFactory().includeRegions(null).excludeRegions(null)
        .start().getResults(TIMEOUT_SECONDS, SECONDS);

    assertThat(manager.getRebalanceOperations()).isEmpty();
  }

  private void doConcurrentPutInTx(String s) throws Exception {
    for (int i = 0; i < workers; i++) {
      queue.add(i);
    }

    ExecutorService pool = Executors.newCachedThreadPool();
    Collection<Callable<Object>> tasks = new ArrayList<>();
    Callable<Object> task = () -> {
      doPutOpInTx(s);
      return null;
    };
    for (int i = 0; i < workers; i++) {
      tasks.add(task);
    }

    List<Future<Object>> futures = pool.invokeAll(tasks);
    for (Future future : futures) {
      future.get();
    }
  }

  private void doConcurrentDestroyInTx() throws Exception {
    for (int i = 0; i < workers; i++) {
      queue.add(i);
    }

    ExecutorService pool = Executors.newCachedThreadPool();
    Collection<Callable<Object>> tasks = new ArrayList<>();
    Callable<Object> task = () -> {
      doDestroyOpInTx();
      return null;
    };
    for (int i = 0; i < workers; i++) {
      tasks.add(task);
    }

    List<Future<Object>> futures = pool.invokeAll(tasks);
    for (Future future : futures) {
      future.get();
    }
  }

  private void doPutOpInTx(String s) {
    int worker;
    if (!queue.isEmpty()) {
      worker = queue.poll();
      Region<Number, String> region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < numOfEntry; i++) {
        if (i % workers == worker) {
          doTXPut(region, i, s);
        }
      }
    }
  }

  private void doDestroyOpInTx() {
    int worker;
    if (!queue.isEmpty()) {
      worker = queue.poll();
      Region<Number, String> region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < numOfEntry; i++) {
        if (i % workers == worker) {
          doTxDestroy(region, i);
        }
      }
    }
  }

  private void doPut(String s) {
    Region<Number, String> region = cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < numOfEntry; i++) {
      region.put(i, s);
    }
  }

  private void doTXPut(Region<Number, String> region, int i, String s) {
    TXManagerImpl manager = cacheRule.getCache().getTxManager();
    manager.begin();
    try {
      region.put(i, s);
      manager.commit();
    } catch (TransactionDataRebalancedException e) {
      if (manager.getTransactionId() != null) {
        manager.rollback();
      }
    } catch (CommitConflictException ignore) {
    }
  }

  private void doTxDestroy(Region<Number, String> region, int i) {
    TXManagerImpl manager = cacheRule.getCache().getTxManager();
    manager.begin();
    try {
      region.remove(i);
      manager.commit();
    } catch (TransactionDataRebalancedException e) {
      if (manager.getTransactionId() != null) {
        manager.rollback();
      }
    } catch (CommitConflictException ignore) {
    }
  }

  private void doDestroy() {
    Region<Number, String> region = cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < numOfEntry; i++) {
      try {
        region.destroy(i);
      } catch (EntryNotFoundException ignore) {
      }
    }
  }

  private void validateVersionsInVms(boolean isPartition, VM... vms) {
    for (VM vm : vms) {
      if (isPartition) {
        vm.invoke(this::validateEntryVersions);
      } else {
        vm.invoke(this::validateEntryVersionsInReplicateRegion);
      }
    }
  }

  private void validateEntryVersions() {
    PartitionedRegion region = (PartitionedRegion) cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < numOfEntry; i++) {
      BucketRegion bucketRegion = region.getDataStore().getLocalBucketByKey(i);
      if (bucketRegion != null) {
        RegionEntry entry = bucketRegion.getRegionMap().getEntry(i);
        validateVersion(entry);
      }
    }
  }

  @Test
  public void correctVersionGeneratedForConcurrentOperationsInTxInReplicateRegion()
      throws Exception {
    vm0.invoke(() -> createRegion(regionName));
    vm0.invoke(() -> doPut("A"));
    vm0.invoke(this::doDestroy);
    vm0.invoke(() -> doPut("B"));
    vm1.invoke(() -> {
      cacheRule.getOrCreateCache();
    });

    AsyncInvocation async = vm1.invokeAsync(() -> createRegion(regionName));
    doConcurrentOpsAndRebalance("C", false);
    async.await();
    validateVersionsInVms(false, vm0, vm1);
  }

  private void createRegion(String regionName) {
    RegionFactory<Integer, String> regionFactory =
        cacheRule.getOrCreateCache().createRegionFactory(REPLICATE);
    CacheListenerAdapter<Integer, String> listener = new CacheListenerAdapter<Integer, String>() {
      @Override
      public void afterDestroy(EntryEvent<Integer, String> e) {
        assertThat(e.getOperation().isEntry()).isTrue();
      }
    };
    regionFactory.addCacheListener(listener).create(regionName);
  }

  private void validateEntryVersionsInReplicateRegion() {
    LocalRegion region = uncheckedCast(cacheRule.getCache().getRegion(regionName));
    for (int i = 0; i < numOfEntry; i++) {
      RegionEntry entry = region.getRegionMap().getEntry(i);
      validateVersion(entry);
    }
  }

  private void validateVersion(RegionEntry entry) {
    if (entry != null) {
      VersionStamp stamp = entry.getVersionStamp();
      VersionTag tag = stamp.asVersionTag();
      if (tag.getEntryVersion() < 3) {
        logger.info("tag for key {} is " + tag, entry.getKey());
      }
      assertThat(tag.getEntryVersion()).isGreaterThan(2);
    }
  }
}
