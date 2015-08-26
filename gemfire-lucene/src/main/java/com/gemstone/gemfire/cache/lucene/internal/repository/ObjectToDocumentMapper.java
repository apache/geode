package com.gemstone.gemfire.cache.lucene.internal.repository;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;

/**
 * Interface for transforming a gemfire key and value into a lucene document
 */
public interface ObjectToDocumentMapper {

  /**
   * Transform a gemfire key and value into a document suitable for adding
   * to a Lucene IndexWriter. The document is expected to have a unique
   * key which can later be used to delete or update the document
   */
  Iterable<? extends IndexableField> transform(Object key, Object value);

  /**
   * Convert a gemfire key into a key search term that can be used to
   * update or delete the document associated with this key.
   */
  Term keyTerm(Object key);

}
