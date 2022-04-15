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

package org.apache.geode.cache.lucene;

import java.io.DataOutput;
import java.io.Serializable;

import org.apache.lucene.search.Query;

import org.apache.geode.DataSerializer;


/**
 * <p>
 * A factory for {@link Query} objects. An implementation of this interface is required by
 * {@link LuceneQueryFactory#create(String, String, LuceneQueryProvider)} so that a query can be
 * serialized and distributed to multiple nodes.
 * <p>
 * Instances of this interface are serialized using the standard
 * {@link DataSerializer#writeObject(Object, DataOutput)},
 */
@FunctionalInterface
public interface LuceneQueryProvider extends Serializable {
  /**
   * @return A {@link Query} which will be executed against a Lucene index.
   * @param index The {@link LuceneIndex} the query is being executed against.
   * @throws LuceneQueryException if the provider fails to construct the query object. This will be
   *         propagated to callers of the {@link LuceneQuery} find methods.
   */

  Query getQuery(LuceneIndex index) throws LuceneQueryException;

}
