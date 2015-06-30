package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.distributed.DistributedMember;

public class LuceneQueryResultsImpl<E> implements LuceneQueryResults<E>, ResultCollector {

  @Override
  public Object getResult() throws FunctionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
    // TODO Auto-generated method stub
    
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
  public List getNextPage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasNextPage() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

}
