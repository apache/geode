package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntries.EntryScoreComparator;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * An implementation of {@link CollectorManager} for managing {@link TopEntriesCollector}. This is used by a member to
 * collect top matching entries from local buckets
 */
public class TopEntriesCollectorManager implements CollectorManager<TopEntries, TopEntriesCollector> {
  private static final Logger logger = LogService.getLogger();

  final int limit;
  
  public TopEntriesCollectorManager() {
    this(LuceneQueryFactory.DEFAULT_LIMIT);
  }
  
  public TopEntriesCollectorManager(int resultLimit) {
    this.limit = resultLimit;
  }

  @Override
  public TopEntriesCollector newCollector(String name) {
    return new TopEntriesCollector(name, limit);
  }

  @Override
  public TopEntries reduce(Collection<IndexResultCollector> collectors) throws IOException {
    final EntryScoreComparator scoreComparator = new TopEntries().new EntryScoreComparator();

    // orders a entry with higher score above a doc with lower score
    Comparator<List<EntryScore>> entryListComparator = new Comparator<List<EntryScore>>() {
      @Override
      public int compare(List<EntryScore> l1, List<EntryScore> l2) {
        EntryScore o1 = l1.get(0);
        EntryScore o2 = l2.get(0);
        return scoreComparator.compare(o1, o2);
      }
    };

    // The queue contains iterators for all bucket results. The queue puts the entry with the highest score at the head
    // using score comparator.
    PriorityQueue<List<EntryScore>> entryListsPriorityQueue;
    entryListsPriorityQueue = new PriorityQueue<List<EntryScore>>(Collections.reverseOrder(entryListComparator));
    TopEntries mergedResult = new TopEntries();

    for (IndexResultCollector collector : collectors) {
      if (logger.isDebugEnabled()) {
        logger.debug("Number of entries found in bucket {} is {}", collector.getName(), collector.size());
      }

      if (collector.size() > 0) {
        entryListsPriorityQueue.add(((TopEntriesCollector)collector).getEntries().getHits());
      }
    }

    logger.debug("Only {} count of entries will be reduced. Other entries will be ignored", limit);
    while (entryListsPriorityQueue.size() > 0 && limit > mergedResult.size()) {

      List<EntryScore> list = entryListsPriorityQueue.remove();
      EntryScore entry = list.remove(0);
      mergedResult.addHit(entry);

      if (list.size() > 0) {
        entryListsPriorityQueue.add(list);
      }
    }

    return mergedResult;
  }
}
