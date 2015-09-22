package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.io.IOException;

import org.apache.lucene.search.Query;

/**
 * An Repository interface for the writing data to lucene.
 */
public interface IndexRepository {

  /**
   * Create a new entry in the lucene index
   * @throws IOException 
   */
  void create(Object key, Object value) throws IOException;

  /**
   * Update the entries in the lucene index
   * @throws IOException 
   */
  void update(Object key, Object value) throws IOException;
  
  /**
   * Delete the entries in the lucene index
   * @throws IOException 
   */
  void delete(Object key) throws IOException;
  
  /**
   * Query the index index repository, passing the results to the collector
   * Only the documents with the top scores, up to the limit, will be passed
   * to the collector, in order of score.
   * 
   * @param query
   * @param limit the maximum number of hits to return
   * @param collector the class to aggregate the hits
   * 
   * @throws IOException
   */
  public void query(Query query, int limit, IndexResultCollector collector) throws IOException;

  /**
   * Commit the changes to all lucene index
   * @throws IOException 
   */
  void commit() throws IOException;
  
  /**
   * Check to see if this repository is closed due to
   * underlying resources being closed or destroyed
   * @return true if this repository is closed.
   */
  public boolean isClosed();
}
