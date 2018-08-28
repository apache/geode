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
package org.apache.geode.cache.lucene.internal;

import java.util.function.Consumer;

import org.mockito.Mockito;

import org.apache.geode.internal.cache.InternalCache;

public class LuceneIndexFactorySpy extends LuceneIndexImplFactory {

  public static LuceneIndexFactorySpy injectSpy() {
    LuceneIndexFactorySpy factory = new LuceneIndexFactorySpy();
    LuceneServiceImpl.luceneIndexFactory = factory;
    return factory;
  }

  public static void remove() {
    LuceneServiceImpl.luceneIndexFactory = new LuceneIndexImplFactory();
  }

  private Consumer<Object> getRepositoryConsumer = key -> {
  };

  @Override
  public LuceneIndexImpl create(String indexName, String regionPath, InternalCache cache) {
    LuceneIndexForPartitionedRegion index =
        Mockito.spy(new ExtendedLuceneIndexForPartitionedRegion(indexName, regionPath, cache));
    return index;
  }


  public void setGetRespositoryConsumer(Consumer<Object> getRepositoryConsumer) {
    this.getRepositoryConsumer = getRepositoryConsumer;
  }

  private static class ExtendedLuceneIndexForPartitionedRegion
      extends LuceneIndexForPartitionedRegion {
    public ExtendedLuceneIndexForPartitionedRegion(final String indexName, final String regionPath,
        final InternalCache cache) {
      super(indexName, regionPath, cache);
    }

  }
}
