/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: ExecutionContext.java 36363 2012-07-05 18:24:18Z dsmith $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.pdx.internal.PdxString;

/**
 * This ExecutionContext will be used ONLY for querying because this
 * is a bit heavt-weight context whose life is longer in JVM than
 * {@link ExecutionContext} which will be used ONLY for index updates.
 *
 * @author shobhit
 * @author jhuynh
 * @since 7.0
 */
public class QueryExecutionContext extends ExecutionContext {

  private int nextFieldNum = 0;
  private Query query;
  private IntOpenHashSet successfulBuckets;

  private boolean cqQueryContext = false;
  
  private SelectResults results;
  
  private List bucketList;
  
  private boolean indexUsed = false;
  
  /**
   * stack used to determine which execCache to currently be using
   */
  private final Stack execCacheStack = new Stack();
  
  /**
   * a map that stores general purpose maps for caching data that is valid 
   * for one query execution only
   */
  private final Map execCaches = new HashMap();

  /**
   * This map stores PdxString corresponding to the bind argument
   */
  private Map<Integer, PdxString> bindArgumentToPdxStringMap;
  
  /**
   * List of query index names that the user has hinted on using
   */
  
  private ArrayList<String> hints = null;
  
  /**
   * @param bindArguments
   * @param cache
   */
  public QueryExecutionContext(Object[] bindArguments, Cache cache) {
    super(bindArguments, cache);
  }

  /**
   * @param bindArguments
   * @param cache
   * @param results
   */
  public QueryExecutionContext(Object[] bindArguments, Cache cache,
      SelectResults results, Query query) {
    super(bindArguments, cache);
    this.results = results;
    this.query = query;
  }

  /**
   * @param bindArguments
   * @param cache
   * @param query
   */
  public QueryExecutionContext(Object[] bindArguments, Cache cache, Query query) {
    super(bindArguments, cache);
    this.query = query;
  }


  // General purpose caching methods for data that is only valid for one
  // query execution
  void cachePut(Object key, Object value) {
    if (key.equals(CompiledValue.QUERY_INDEX_HINTS)) {
      setHints((ArrayList)value);
      return;
    }
    //execCache can be empty in cases where we are doing adds to indexes
    //in that case, we use a default execCache
    int scopeId = -1;
    if (!execCacheStack.isEmpty()) {
      scopeId = (Integer) execCacheStack.peek();
    }
    Map execCache = (Map)execCaches.get(scopeId);
    if (execCache == null) {
      execCache = new HashMap();
      execCaches.put(scopeId, execCache);
    }
    execCache.put(key, value);
  }

  public Object cacheGet(Object key) {
    //execCache can be empty in cases where we are doing adds to indexes
    //in that case, we use a default execCache
    int scopeId = -1;
    if (!execCacheStack.isEmpty()) {
      scopeId = (Integer) execCacheStack.peek();
    }
    Map execCache = (Map)execCaches.get(scopeId);
    return execCache != null? execCache.get(key): null;
  }

  public void pushExecCache(int scopeNum) {
    execCacheStack.push(scopeNum);
  }
  
  public void popExecCache() {
    execCacheStack.pop();
  }

  /**
   * Added to reset the state from the last execution. This is added for CQs only.
   */
  public void reset(){
    super.reset();
    this.execCacheStack.clear();
  }

  int nextFieldNum() {
    return this.nextFieldNum++;
  }
  
  public void setCqQueryContext(boolean cqQuery){
    this.cqQueryContext = cqQuery;
  }

  public boolean isCqQueryContext(){
    return this.cqQueryContext;
  }

  public SelectResults getResults() {
    return this.results;
  }

  public Query getQuery() {
    return query;
  }
  
  public void setBucketList(List list) {
    this.bucketList = list;
    this.successfulBuckets = new IntOpenHashSet();
  }

  public List getBucketList() {
    return this.bucketList;
  }
  
  public void addToSuccessfulBuckets(int bId) {
    this.successfulBuckets.add(bId);
  }
  
  public int[] getSuccessfulBuckets() {
    return this.successfulBuckets.toIntArray();
  }
  
  /**
   * creates new PdxString from String and caches it
   */
  public PdxString getSavedPdxString(int index){
    if(bindArgumentToPdxStringMap == null){
      bindArgumentToPdxStringMap = new HashMap<Integer, PdxString>();
    } 
    
    PdxString pdxString = bindArgumentToPdxStringMap.get(index-1);
    if(pdxString == null){
      pdxString = new PdxString((String)bindArguments[index-1]);
      bindArgumentToPdxStringMap.put(index-1, pdxString);
    }
    return pdxString;
    
  }
  
  public boolean isIndexUsed() {
    return indexUsed;
  }
  
  void setIndexUsed(boolean indexUsed) {
    this.indexUsed = indexUsed;
  }
  
  public void setHints(ArrayList<String> hints) {
    this.hints = new ArrayList();
    this.hints.addAll(hints);
  }
  
  /**
   * @param indexName of index to check if in the hinted list
   * @return true if the index name was hinted by the user
   */
  public boolean isHinted(String indexName) {
    return hints != null? hints.contains(indexName):false;
  }
  
  /**
   * Hint size is used for filter ordering.
   * Smaller values have preference
   */
  public int getHintSize(String indexName) {
    return -(hints.size() - hints.indexOf(indexName));
  }
  
  public boolean hasHints() {
    return hints != null;
  }
  
  public boolean hasMultiHints() {
    return hints != null && hints.size() > 1;
  }
}