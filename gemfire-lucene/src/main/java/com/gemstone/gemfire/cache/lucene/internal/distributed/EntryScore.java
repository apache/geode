package com.gemstone.gemfire.cache.lucene.internal.distributed;

/**
 * Holds one entry matching search query and its metadata
 */
public class EntryScore {
  // Key of the entry matching search query
  final Object key;

  // The score of this document for the query.
  final float score;

  public EntryScore(Object key, float score) {
    this.key = key;
    this.score = score;
  }

  @Override
  public String toString() {
    return "key=" + key + " score=" + score;
  }
}
