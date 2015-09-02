package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;

/**
 * Holds a ordered collection of entries matching a search query.
 */
public class TopEntries {
  // ordered collection of entries
  final private List<EntryScore> hits = new ArrayList<>();

  // the maximum number of entries stored in this
  final int limit;

  // comparator to order entryScore instances
  final Comparator<EntryScore> comparator = new EntryScoreComparator();

  public TopEntries() {
    this(LuceneQueryFactory.DEFAULT_LIMIT);
  }

  public TopEntries(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException();
    }
    this.limit = limit;
  }

  /**
   * Adds an entry to the collection. The new entry must have a lower score than all previous entries added to the
   * collection. The new entry will be ignored if the limit is already reached.
   * 
   * @param entry
   */
  public void addHit(EntryScore entry) {
    if (hits.size() > 0) {
      EntryScore lastEntry = hits.get(hits.size() - 1);
      if (comparator.compare(lastEntry, entry) < 0) {
        throw new IllegalArgumentException();
      }
    }

    if (hits.size() >= limit) {
      return;
    }

    hits.add(entry);
  }

  /**
   * @return count of entries in the collection
   */
  public int size() {
    return hits.size();
  }

  /**
   * @return The entries collection managed by this instance
   */
  public List<EntryScore> getHits() {
    return hits;
  }

  /**
   * Compares scores of two entries using natural ordering. I.e. it returns -1 if the first entry's score is less than
   * the second one.
   */
  class EntryScoreComparator implements Comparator<EntryScore> {
    @Override
    public int compare(EntryScore o1, EntryScore o2) {
      return Float.compare(o1.score, o2.score);
    }
  };
}
