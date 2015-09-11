package com.gemstone.gemfire.cache.lucene.internal.distributed;

import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;

/**
 * An implementation of {@link IndexResultCollector} to collect {@link EntryScore}. It is expected that the results will
 * be ordered by score of the entry.
 */
public class TopEntriesCollector implements IndexResultCollector {
  final String name;

  private final TopEntries entries;

  public TopEntriesCollector(String name) {
    this(name, LuceneQueryFactory.DEFAULT_LIMIT);
  }

  public TopEntriesCollector(String name, int limit) {
    this.name = name;
    this.entries = new TopEntries(limit);
  }

  @Override
  public void collect(Object key, float score) {
    collect(new EntryScore(key, score));
  }
  
  public void collect(EntryScore entry) {
    entries.addHit(entry);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int size() {
    TopEntries entries = getEntries();
    return entries == null ? 0 : entries.size();
  }

  /**
   * @return The entries collected by this collector
   */
  public TopEntries getEntries() {
    return entries;
  }
}
