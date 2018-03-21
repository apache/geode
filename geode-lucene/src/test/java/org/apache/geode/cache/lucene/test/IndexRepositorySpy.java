/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockingDetails;

import java.io.IOException;
import java.util.function.Consumer;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.IndexRepositoryFactory;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.internal.PartitionedRepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.internal.cache.PartitionedRegion;

public class IndexRepositorySpy extends IndexRepositoryFactory {

  private Consumer<Object> beforeWrite = key -> {
  };

  public static IndexRepositorySpy injectSpy() {
    IndexRepositorySpy factory = new IndexRepositorySpy();
    PartitionedRepositoryManager.indexRepositoryFactory = factory;
    return factory;
  }

  public static void remove() {
    PartitionedRepositoryManager.indexRepositoryFactory = new IndexRepositoryFactory();
  }

  private IndexRepositorySpy() {}

  @Override
  public IndexRepository computeIndexRepository(final Integer bucketId, LuceneSerializer serializer,
      InternalLuceneIndex index, PartitionedRegion userRegion, IndexRepository oldRepository,
      PartitionedRepositoryManager partitionedRepositoryManager) throws IOException {
    final IndexRepository indexRepo = super.computeIndexRepository(bucketId, serializer, index,
        userRegion, oldRepository, partitionedRepositoryManager);
    if (indexRepo == null) {
      return null;
    }
    if (mockingDetails(indexRepo).isSpy()) {
      return indexRepo;
    }

    final IndexRepository spy = Mockito.spy(indexRepo);

    Answer invokeBeforeWrite = invocation -> {
      beforeWrite.accept(invocation.getArgument(0));
      return invocation.callRealMethod();
    };

    doAnswer(invokeBeforeWrite).when(spy).update(any(), any());
    doAnswer(invokeBeforeWrite).when(spy).create(any(), any());
    doAnswer(invokeBeforeWrite).when(spy).delete(any());

    return spy;
  }


  /**
   * Add a callback that runs before a call to {@link IndexRepository#create(Object, Object)},
   * {@link IndexRepository#update(Object, Object)} or {@link IndexRepository#delete(Object)}
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
   * Return a consumer that will invoke the passed in consumer only after it has been called exactly
   * N times.
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
