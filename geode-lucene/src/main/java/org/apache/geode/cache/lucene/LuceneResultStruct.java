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
package org.apache.geode.cache.lucene;


/**
 * An interface which stores a single result obtained by executing a Lucene query.
 *
 */
public interface LuceneResultStruct<K, V> {

  /**
   * Returns the Apache Geode region key of the result matching the Lucene Query
   *
   * @return The region key of the entry matching the query
   *
   */
  K getKey();

  /**
   * Returns the Apache Geode region key of the result matching the Lucene Query
   *
   * @return the region value of the entry matching the query.
   *
   */
  V getValue();

  /**
   * Return score the score of the entry matching the query. Scores are computed by Lucene based on
   * how closely the entry matches the query.
   *
   * @return float value representing the score of the entry obtained as a result of executing the
   *         Lucene query.
   */
  float getScore();
}
