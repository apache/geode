package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.io.IOException;

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
   * Commit the changes to all lucene index
   * @throws IOException 
   */
  void commit() throws IOException;
}
