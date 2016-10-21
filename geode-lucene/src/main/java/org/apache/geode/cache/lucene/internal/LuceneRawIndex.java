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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.internal.cache.PartitionedRegion;

public class LuceneRawIndex extends LuceneIndexImpl {

  protected LuceneRawIndex(String indexName, String regionPath, Cache cache) {
    super(indexName, regionPath, cache);
  }

  @Override
  protected RepositoryManager createRepositoryManager() {
    HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(getFieldNames());
    return new RawLuceneRepositoryManager(this, mapper);
  }

  @Override
  public void dumpFiles(String directory) {
    return;
  }

}
