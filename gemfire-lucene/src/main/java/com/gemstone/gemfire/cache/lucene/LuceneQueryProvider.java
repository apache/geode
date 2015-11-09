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
   * @param The local lucene index the query is being constructed against.
   * @throws QueryException if the provider fails to construct the query object
   */
  public Query getQuery(LuceneIndex index) throws QueryException;
}
