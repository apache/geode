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
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;

public interface InternalLuceneIndex extends LuceneIndex {

  RepositoryManager getRepositoryManager();

  /**
   * Dump the files for this index to the given directory.
   */
  void dumpFiles(String directory);

  /**
   * Destroy the index
   */
  void destroy(boolean initiator);

  LuceneIndexStats getIndexStats();

  Cache getCache();

  void initialize();

  boolean isIndexAvailable(int id);
}
