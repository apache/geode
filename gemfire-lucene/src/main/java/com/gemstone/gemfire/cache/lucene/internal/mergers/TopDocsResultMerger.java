package com.gemstone.gemfire.cache.lucene.internal.mergers;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.internal.LuceneQueryResultsImpl;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * An implementation of {@link ResultMerger} which combines search results from shards on the basis of score
 */
public class TopDocsResultMerger extends ResultMerger<LuceneResultStruct> {
  // execute merge within the context of this query
  LuceneQuery queryCtx;
  
  private static final Logger logger = LogService.getLogger();

  @Override
  public void init(LuceneQuery query) {
    this.queryCtx = query;
  }

  @Override
  public LuceneQueryResults mergeResults(List<LuceneQueryResults> results) {
    if (logger.isDebugEnabled()) {
      logger.debug("Merging {} shard results for query {}", results.size(), queryCtx);
    }
    
    // orders a doc with higher score above a doc with lower score
    Comparator<List<LuceneResultStruct>> comparator = new Comparator<List<LuceneResultStruct>>() {
      @Override
      public int compare(List<LuceneResultStruct> l1, List<LuceneResultStruct> l2) {
        LuceneResultStruct o1 = l1.get(0);
        LuceneResultStruct o2 = l2.get(0);
        int result = Float.compare(o1.getScore(), o2.getScore()) * (-1);
        if (result != 0) {
          return result;
        }
        
        return Integer.compare(o1.getKey().hashCode(), o2.getKey().hashCode());
      }
    };

    // The queue contains iterators for all shard results. The iterator with the highest score doc is put at the head of the queue using score comparator
    PriorityQueue<List<LuceneResultStruct>> queue = new PriorityQueue<List<LuceneResultStruct>>(comparator);

    LuceneQueryResultsImpl mergedResult = new LuceneQueryResultsImpl();
    for (LuceneQueryResults result : results) {
      if (logger.isDebugEnabled()) {
        logger.debug("Count of results returned from shard {} is {}", result.getID(), result.size());
      }
      
      List<LuceneResultStruct> list = result.getHits();
      if (list.size() > 0) {
        queue.add(list);
      }
    }

    int limit = queryCtx == null ? LuceneQueryFactory.DEFAULT_LIMIT : queryCtx.getLimit();
    logger.debug("Merge result count will be limited to: " + limit);
    while (queue.size() > 0 && limit-- > 0) {
      List<LuceneResultStruct> list = queue.remove();
      mergedResult.addHit(list.remove(0));
      if (list.size() > 0) {
        queue.add(list);
      }
    }

    return mergedResult;
  }
}
