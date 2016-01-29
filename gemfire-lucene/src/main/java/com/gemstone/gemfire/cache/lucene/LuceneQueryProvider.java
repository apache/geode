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

import java.io.Serializable;

import org.apache.lucene.search.Query;

import com.gemstone.gemfire.annotations.Experimental;
import com.gemstone.gemfire.cache.query.QueryException;

/**
 * The instances of this class will be used for distributing Lucene Query objects and re-constructing the Query object.
 * If necessary the implementation needs to take care of serializing and de-serializing Lucene Query object. Geode
 * respects the DataSerializable contract to provide optimal object serialization. For instance,
 * {@link LuceneQueryProvider}'s toData method will be used to serialize it when it is sent to another member of the
 * distributed system. Implementation of DataSerializable can provide a zero-argument constructor that will be invoked
 * when they are read with DataSerializer.readObject.
 */
@Experimental
public interface LuceneQueryProvider extends Serializable {
  /**
   * @return A Lucene Query object which could be used for executing Lucene Search on indexed data
   * @param index local lucene index the query is being constructed against.
   * @throws QueryException if the provider fails to construct the query object
   */
  public Query getQuery(LuceneIndex index) throws QueryException;
}
