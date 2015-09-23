package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * Contains function arguments for text / lucene search
 */
public class LuceneFunctionContext<C extends IndexResultCollector> implements DataSerializableFixedID {
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

  public LuceneFunctionContext(LuceneQueryProvider provider, String indexName, CollectorManager<C> manager) {
    this(provider, indexName, manager, LuceneQueryFactory.DEFAULT_LIMIT);
  }

  public LuceneFunctionContext(LuceneQueryProvider provider, String indexName, CollectorManager<C> manager, int limit) {
    this.queryProvider = provider;
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
   * On each member, search query is executed on one or more {@link IndexRepository}s. A {@link CollectorManager} could
   * be provided to customize the way results from these repositories is collected and merged.
   * 
   * @return {@link CollectorManager} instance to be used by function
   */
  public CollectorManager<C> getCollectorManager() {
    return this.manager;
  }

  public LuceneQueryProvider getQueryProvider() {
    return queryProvider;
  }

  @Override
  public int getDSFID() {
    return LUCENE_FUNCTION_CONTEXT;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(limit);
    DataSerializer.writeObject(queryProvider, out);
    DataSerializer.writeObject(manager, out);
    DataSerializer.writeString(indexName, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    limit = in.readInt();
    queryProvider = DataSerializer.readObject(in);
    manager = DataSerializer.readObject(in);
    this.indexName = DataSerializer.readString(in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
