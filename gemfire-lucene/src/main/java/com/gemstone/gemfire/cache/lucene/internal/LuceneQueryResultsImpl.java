package com.gemstone.gemfire.cache.lucene.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.distributed.DistributedMember;

public class LuceneQueryResultsImpl implements LuceneQueryResults,
    ResultCollector<LuceneQueryResults, List<LuceneQueryResults>> {

  // list of docs matching search query
  private List<LuceneResultStruct> hits = new ArrayList<>();
  private float maxScore = Float.MIN_VALUE;

  @Override
  public List<LuceneQueryResults> getResult() throws FunctionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<LuceneQueryResults> getResult(long timeout, TimeUnit unit) throws FunctionException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addResult(DistributedMember memberID, LuceneQueryResults resultOfSingleExecution) {
    // results.add(resultOfSingleExecution);
  }

  @Override
  public void endResults() {
    // TODO Auto-generated method stub

  }

  @Override
  public void clearResults() {
    // TODO Auto-generated method stub

  }

  @Override
  public List<LuceneResultStruct> getNextPage() {
    return null;
  }

  @Override
  public boolean hasNextPage() {
    return false;
  }

  @Override
  public Object getID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    return hits.size();
  }

  @Override
  public float getMaxScore() {
    return maxScore;
  }

  /**
   * Adds a result hit to the result set
   * 
   * @param hit
   */
  public void addHit(LuceneResultStruct hit) {
    hits.add(hit);
    if (hit.getScore() > maxScore) {
      maxScore = hit.getScore();
    }
  }
}
