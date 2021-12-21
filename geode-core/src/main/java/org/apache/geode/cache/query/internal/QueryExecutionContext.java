/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.geode.cache.query.Query;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.internal.PdxString;

/**
 * This ExecutionContext will be used ONLY for querying because this is a bit heavt-weight context
 * whose life is longer in JVM than {@link ExecutionContext} which will be used ONLY for index
 * updates.
 *
 * @since GemFire 7.0
 */
public class QueryExecutionContext extends ExecutionContext {

  private int nextFieldNum = 0;

  private final Query query;

  private final boolean cqQueryContext;

  private List bucketList;

  private boolean indexUsed = false;

  /**
   * stack used to determine which execCache to currently be using
   */
  private final Stack execCacheStack = new Stack();

  /**
   * a map that stores general purpose maps for caching data that is valid for one query execution
   * only
   */
  private final Map execCaches = new HashMap();

  /**
   * This map stores PdxString corresponding to the bind argument
   */
  private Map<Integer, PdxString> bindArgumentToPdxStringMap;

  /**
   * List of query index names that the user has hinted on using
   */
  private ArrayList hints = null;

  public QueryExecutionContext(Object[] bindArguments, InternalCache cache) {
    super(bindArguments, cache);
    query = null;
    cqQueryContext = false;
  }

  public QueryExecutionContext(Object[] bindArguments, InternalCache cache,
      boolean cqQueryContext) {
    super(bindArguments, cache);
    query = null;
    this.cqQueryContext = cqQueryContext;
  }

  public QueryExecutionContext(Object[] bindArguments, InternalCache cache, Query query) {
    super(bindArguments, cache);
    this.query = query;
    cqQueryContext = ((DefaultQuery) query).isCqQuery();
  }

  @Override
  void cachePut(Object key, Object value) {
    if (key.equals(CompiledValue.QUERY_INDEX_HINTS)) {
      setHints((ArrayList) value);
      return;
    }
    // execCache can be empty in cases where we are doing adds to indexes
    // in that case, we use a default execCache
    int scopeId = -1;
    if (!execCacheStack.isEmpty()) {
      scopeId = (Integer) execCacheStack.peek();
    }
    Map execCache = (Map) execCaches.get(scopeId);
    if (execCache == null) {
      execCache = new HashMap();
      execCaches.put(scopeId, execCache);
    }
    execCache.put(key, value);
  }

  @Override
  public Object cacheGet(Object key) {
    return cacheGet(key, null);
  }

  @Override
  public Object cacheGet(Object key, Object defaultValue) {
    // execCache can be empty in cases where we are doing adds to indexes
    // in that case, we use a default execCache
    int scopeId = -1;
    if (!execCacheStack.isEmpty()) {
      scopeId = (Integer) execCacheStack.peek();
    }
    Map execCache = (Map) execCaches.get(scopeId);
    if (execCache == null) {
      return defaultValue;
    }
    if (execCache.containsKey(key)) {
      return execCache.get(key);
    }
    return defaultValue;
  }

  @Override
  public void pushExecCache(int scopeNum) {
    execCacheStack.push(scopeNum);
  }

  @Override
  public void popExecCache() {
    execCacheStack.pop();
  }

  /**
   * Added to reset the state from the last execution. This is added for CQs only.
   */
  @Override
  public void reset() {
    super.reset();
    execCacheStack.clear();
  }

  @Override
  int nextFieldNum() {
    return nextFieldNum++;
  }

  @Override
  public boolean isCqQueryContext() {
    return cqQueryContext;
  }

  @Override
  public Query getQuery() {
    return query;
  }

  @Override
  public void setBucketList(List list) {
    bucketList = list;
  }

  @Override
  public List getBucketList() {
    return bucketList;
  }

  /**
   * creates new PdxString from String and caches it
   */
  @Override
  public PdxString getSavedPdxString(int index) {
    if (bindArgumentToPdxStringMap == null) {
      bindArgumentToPdxStringMap = new HashMap<>();
    }

    PdxString pdxString = bindArgumentToPdxStringMap.get(index - 1);
    if (pdxString == null) {
      pdxString = new PdxString((String) bindArguments[index - 1]);
      bindArgumentToPdxStringMap.put(index - 1, pdxString);
    }
    return pdxString;
  }

  public boolean isIndexUsed() {
    return indexUsed;
  }

  void setIndexUsed(boolean indexUsed) {
    this.indexUsed = indexUsed;
  }

  private void setHints(ArrayList<String> hints) {
    this.hints = new ArrayList<>();
    this.hints.addAll(hints);
  }

  /**
   * @param indexName of index to check if in the hinted list
   * @return true if the index name was hinted by the user
   */
  public boolean isHinted(String indexName) {
    return hints != null && hints.contains(indexName);
  }

  /**
   * Hint size is used for filter ordering. Smaller values have preference
   */
  public int getHintSize(String indexName) {
    return -(hints.size() - hints.indexOf(indexName));
  }

  boolean hasHints() {
    return hints != null;
  }

  boolean hasMultiHints() {
    return hints != null && hints.size() > 1;
  }
}
