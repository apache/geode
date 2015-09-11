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
public class TopEntriesCollectorManager implements CollectorManager<TopEntriesCollector> {
  private static final Logger logger = LogService.getLogger();

  final int limit;
  final String id;

  public TopEntriesCollectorManager() {
    this(null, 0);
  }

  public TopEntriesCollectorManager(String id) {
    this(id, 0);
  }

  public TopEntriesCollectorManager(String id, int resultLimit) {
    this.limit = resultLimit <= 0 ? LuceneQueryFactory.DEFAULT_LIMIT : resultLimit;
    this.id = id == null ? String.valueOf(this.hashCode()) : id;
    logger.debug("Max count of entries to be produced by {} is {}", id, limit);
  }

  @Override
  public TopEntriesCollector newCollector(String name) {
    return new TopEntriesCollector(name, limit);
  }

  @Override
  public TopEntriesCollector reduce(Collection<TopEntriesCollector> collectors) throws IOException {
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
    TopEntriesCollector mergedResult = new TopEntriesCollector(id, limit);

    for (IndexResultCollector collector : collectors) {
      logger.debug("Number of entries found in collector {} is {}", collector.getName(), collector.size());

      if (collector.size() > 0) {
        entryListsPriorityQueue.add(((TopEntriesCollector) collector).getEntries().getHits());
      }
    }

    logger.debug("Only {} count of entries will be reduced. Other entries will be ignored", limit);
    while (entryListsPriorityQueue.size() > 0 && limit > mergedResult.size()) {

      List<EntryScore> list = entryListsPriorityQueue.remove();
      EntryScore entry = list.remove(0);
      mergedResult.collect(entry);

      if (list.size() > 0) {
        entryListsPriorityQueue.add(list);
      }
    }

    logger.debug("Reduced size of {} is {}", mergedResult.name, mergedResult.size());
    return mergedResult;
  }
}
