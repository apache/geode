/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.PageableLuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;
import com.gemstone.gemfire.cache.lucene.internal.distributed.EntryScore;

/**
 * Implementation of PageableLuceneQueryResults that fetchs a page at a time
 * from the server, given a set of EntryScores (key and score).
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class PageableLuceneQueryResultsImpl<K,V> implements PageableLuceneQueryResults<K,V> {

  /**
   *  list of docs matching search query
   */
  private final List<EntryScore<K>> hits;
  
  /**
   * The maximum score. Lazily evaluated
   */
  private float maxScore = Float.MIN_VALUE;
  
  /**
   * The user region where values are stored.
   */
  private final Region<K, V> userRegion;
  
  /**
   * The start of the next page of results we want to fetch 
   */
  private int currentHit = 0;
  
  /**
   * The page size the user wants.
   */
  private int pageSize;
  
  public PageableLuceneQueryResultsImpl(List<EntryScore<K>> hits, Region<K,V> userRegion, int pageSize) {
    this.hits = hits;
    this.userRegion = userRegion;
    this.pageSize = pageSize == 0 ? Integer.MAX_VALUE : pageSize;
  }

  @Override
  public List<LuceneResultStruct<K,V>> getNextPage() {
    if(!hasNextPage()) {
      return null;
    }
    
    int end = currentHit + pageSize;
    end = end > hits.size() ? hits.size() : end;
    List<EntryScore<K>> scores = hits.subList(currentHit, end);
    
    ArrayList<K> keys = new ArrayList<K>(hits.size());
    for(EntryScore<K> score : scores) {
      keys.add(score.getKey());
    }
    
    Map<K,V> values = userRegion.getAll(keys);
    
    ArrayList<LuceneResultStruct<K,V>> results = new ArrayList<LuceneResultStruct<K,V>>(hits.size());
    for(EntryScore<K> score : scores) {
      V value = values.get(score.getKey());
      results.add(new LuceneResultStructImpl(score.getKey(), value, score.getScore()));
    }
    

    currentHit = end;
    
    return results;
  }

  @Override
  public boolean hasNextPage() {
    return hits.size() > currentHit;
  }

  @Override
  public int size() {
    return hits.size();
  }

  @Override
  public float getMaxScore() {
    if(maxScore == Float.MIN_VALUE) {
      for(EntryScore<K> score : hits) {
        maxScore = Math.max(maxScore, score.getScore());
      }
    }
    
    return maxScore;
  }
}
