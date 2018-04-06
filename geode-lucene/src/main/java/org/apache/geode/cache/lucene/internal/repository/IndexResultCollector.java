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

package org.apache.geode.cache.lucene.internal.repository;

import org.apache.geode.cache.lucene.internal.distributed.CollectorManager;

/**
 * Interface for the collection that stores the results of a Lucene query executed on an
 * IndexRepository. See
 * {@link IndexRepository#query(org.apache.lucene.search.Query, int, IndexResultCollector)} to
 * understand how Lucene Queries are executed.
 */
public interface IndexResultCollector {
  /**
   * Returns the name of the Lucene IndexResultCollector that will store the results of the Lucene
   * query executed on the IndexRepository. The name is set while creating the IndexResultCollector
   * using {@link CollectorManager#newCollector(String)}
   *
   * @return Name/identifier of this collector
   */
  String getName();

  /**
   * Returns the number of Lucene query results that are being stored in the IndexResultCollector
   *
   * @return number of results collected by this collector
   */
  int size();

  /**
   * Collects a corresponding pair of Apache Geode key and Lucene score assigned to the document
   * which is returned by execution of a Lucene query. This is stored in the IndexResultCollector.
   *
   * @param key - Apache Geode key of the object stored in the region
   * @param score the score of this result document assigned by Lucene
   */
  void collect(Object key, float score);
}
