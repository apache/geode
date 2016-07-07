/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.Analyzer;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.lucene.internal.IndexRepositoryFactory;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexStats;
import com.gemstone.gemfire.cache.lucene.internal.PartitionedRepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystemStats;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This test class adds more basic tests of lucene functionality
 * for partitioned regions. These tests should work across all types
 * of PRs and topologies.
 *
 */
public abstract class LuceneQueriesPRBase extends LuceneQueriesBase {

  @After
  public void cleanupRebalanceCallback() {
    removeCallback(dataStore1);
    removeCallback(dataStore2);
  }



  @Test
  public void returnCorrectResultsWhenRebalanceHappensOnIndexUpdate() throws InterruptedException {
    addCallbackToTriggerRebalance(dataStore1);

    putEntriesAndValidateQueryResults();
  }

  @Test
  public void returnCorrectResultsWhenMoveBucketHappensOnIndexUpdate() throws InterruptedException {
    final DistributedMember member2 = dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMoveBucket(dataStore1, member2);

    putEntriesAndValidateQueryResults();
  }

  @Test
  public void returnCorrectResultsWhenBucketIsMovedAndMovedBackOnIndexUpdate() throws InterruptedException {
    final DistributedMember member1 = dataStore1.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    final DistributedMember member2 = dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMoveBucket(dataStore1, member2);
    addCallbackToMoveBucket(dataStore2, member1);

    putEntriesAndValidateQueryResults();
  }

  protected void putEntriesAndValidateQueryResults() {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    put113Entries();

    dataStore2.invoke(() -> initDataStore(createIndex));
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    executeTextSearch(accessor, "world", "text", 113);
  }

  @Test
  public void returnCorrectResultsWhenRebalanceHappensAfterUpdates() throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));

    put113Entries();

    dataStore2.invoke(() -> initDataStore(createIndex));
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));

    rebalanceRegion(dataStore2);

    executeTextSearch(accessor, "world", "text", 113);
  }

  @Test
  public void returnCorrectResultsWhenRebalanceHappensWhileSenderIsPaused() throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    put113Entries();

    dataStore2.invoke(() -> initDataStore(createIndex));
    rebalanceRegion(dataStore2);
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));

    executeTextSearch(accessor, "world", "text", 113);
  }

  protected void put113Entries() {
    accessor.invoke(() -> {
      final Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      IntStream.range(0,113).forEach(i -> region.put(i, new TestObject("hello world")));
    });
  }

  private void addCallbackToTriggerRebalance(VM vm) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWrite(doOnce(key -> rebalanceRegion(vm)));
    });
  }

  protected void addCallbackToMoveBucket(VM vm, final DistributedMember destination) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWrite(doOnce(key -> moveBucket(destination, key)));
    });
  }

  private void moveBucket(final DistributedMember destination, final Object key) {
    Region<Object, Object> region = getCache().getRegion(REGION_NAME);
    DistributedMember source = getCache().getDistributedSystem().getDistributedMember();
    PartitionRegionHelper.moveBucketByKey(region, source, destination, key);
  }

  private void removeCallback(VM vm) {
    vm.invoke(IndexRepositorySpy::remove);
  }

  private void rebalanceRegion(VM vm) {
    // Do a rebalance
    vm.invoke(() -> {
        RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
        RebalanceResults results = op.getResults();
    });
  }

  protected static class IndexRepositorySpy extends IndexRepositoryFactory {

    private Consumer<Object> beforeWrite = key -> {};

    public static IndexRepositorySpy injectSpy() {
      IndexRepositorySpy factory = new IndexRepositorySpy();
      PartitionedRepositoryManager.indexRepositoryFactory = factory;
      return factory;
    }

    public static void remove() {
      PartitionedRepositoryManager.indexRepositoryFactory = new IndexRepositoryFactory();
    }

    private IndexRepositorySpy() {
    }

    @Override
    public IndexRepository createIndexRepository(final Integer bucketId,
                                                 final PartitionedRegion userRegion,
                                                 final PartitionedRegion fileRegion,
                                                 final PartitionedRegion chunkRegion,
                                                 final LuceneSerializer serializer,
                                                 final Analyzer analyzer,
                                                 final LuceneIndexStats indexStats,
                                                 final FileSystemStats fileSystemStats)
      throws IOException
    {
      final IndexRepository indexRepo = super.createIndexRepository(bucketId, userRegion, fileRegion, chunkRegion,
        serializer, analyzer,
        indexStats,
        fileSystemStats);
      final IndexRepository spy = Mockito.spy(indexRepo);

      Answer invokeBeforeWrite = invocation -> {
        beforeWrite.accept(invocation.getArgumentAt(0, Object.class));
        invocation.callRealMethod();
        return null;
      };
      doAnswer(invokeBeforeWrite).when(spy).update(any(), any());
      doAnswer(invokeBeforeWrite).when(spy).create(any(), any());
      doAnswer(invokeBeforeWrite).when(spy).delete(any());

      return spy;
    }

    /**
     * Add a callback that runs before a call to
     * {@link IndexRepository#create(Object, Object)}
     */
    public void beforeWrite(Consumer<Object> action) {
      this.beforeWrite = action;
    }
  }

  protected static <T> Consumer<T> doOnce(Consumer<T> consumer) {
    return new Consumer<T>() {
      boolean done;

      @Override
      public void accept(final T t) {
        if (!done) {
          done = true;
          consumer.accept(t);
        }
      }
    };
  };
}
