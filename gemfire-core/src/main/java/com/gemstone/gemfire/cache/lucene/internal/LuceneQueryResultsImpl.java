/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
