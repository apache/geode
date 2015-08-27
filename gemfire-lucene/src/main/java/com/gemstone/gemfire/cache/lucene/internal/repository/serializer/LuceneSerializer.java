package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import org.apache.lucene.document.Document;

/**
 * An interface for writing the fields of an
 * object into a lucene document
 */
public interface LuceneSerializer {

  /**
   * Add the fields of the given value to the document
   */
  void toDocument(Object value, Document doc);
  
}