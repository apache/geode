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

package org.apache.geode.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Contains function arguments for text / lucene search
 */
public class LuceneFunctionContext<C extends IndexResultCollector>
    implements DataSerializableFixedID {
  private CollectorManager<C> manager;
  private int limit;
  private LuceneQueryProvider queryProvider;
  private String indexName;

  public LuceneFunctionContext() {
    this(null, null, null);
  }

  public LuceneFunctionContext(LuceneQueryProvider provider, String indexName) {
    this(provider, indexName, null);
  }

  public LuceneFunctionContext(LuceneQueryProvider provider, String indexName,
      CollectorManager<C> manager) {
    this(provider, indexName, manager, LuceneQueryFactory.DEFAULT_LIMIT);
  }

  public LuceneFunctionContext(LuceneQueryProvider provider, String indexName,
      CollectorManager<C> manager, int limit) {
    queryProvider = provider;
    this.indexName = indexName;
    this.manager = manager;
    this.limit = limit;
  }

  /**
   * @return The maximum count of result objects to be produced by the function
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Get the name of the index to query
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * On each member, search query is executed on one or more {@link IndexRepository}s. A
   * {@link CollectorManager} could be provided to customize the way results from these repositories
   * is collected and merged.
   *
   * @return {@link CollectorManager} instance to be used by function
   */
  public CollectorManager<C> getCollectorManager() {
    return manager;
  }

  public LuceneQueryProvider getQueryProvider() {
    return queryProvider;
  }

  @Override
  public int getDSFID() {
    return LUCENE_FUNCTION_CONTEXT;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(limit);
    context.getSerializer().writeObject(queryProvider, out);
    context.getSerializer().writeObject(manager, out);
    DataSerializer.writeString(indexName, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    limit = in.readInt();
    queryProvider = context.getDeserializer().readObject(in);
    manager = context.getDeserializer().readObject(in);
    indexName = DataSerializer.readString(in);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
