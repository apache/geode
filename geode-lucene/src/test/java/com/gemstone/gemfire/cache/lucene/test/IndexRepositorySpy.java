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
package com.gemstone.gemfire.cache.lucene.test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.function.Consumer;

import com.gemstone.gemfire.cache.lucene.internal.IndexRepositoryFactory;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexStats;
import com.gemstone.gemfire.cache.lucene.internal.PartitionedRepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystemStats;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import org.apache.lucene.analysis.Analyzer;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class IndexRepositorySpy extends IndexRepositoryFactory {

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
      return invocation.callRealMethod();
    };

    doAnswer(invokeBeforeWrite).when(spy).update(any(), any());
    doAnswer(invokeBeforeWrite).when(spy).create(any(), any());
    doAnswer(invokeBeforeWrite).when(spy).delete(any());

    return spy;
  }

  /**
   * Add a callback that runs before a call to
   * {@link IndexRepository#create(Object, Object)},
   * {@link IndexRepository#update(Object, Object)} or
   * {@link IndexRepository#delete(Object)}
   */
  public void beforeWriteIndexRepository(Consumer<Object> action) {
    this.beforeWrite = action;
  }

  /**
   * Return a consumer that will invoke the passed in consumer only once
   */
  public static <T> Consumer<T> doOnce(Consumer<T> consumer) {
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
  }

  /**
   * Return a consumer that will invoke the passed in consumer only after
   * it has been called exactly N times.
   */
  public static <T> Consumer<T> doAfterN(Consumer<T> consumer, int times) {
    return new Consumer<T>() {
      int count = 0;

      @Override
      public void accept(final T t) {
        if (++count == times) {
          consumer.accept(t);
        }
      }
    };
  }
}
