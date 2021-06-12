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

package org.apache.geode.internal.cache.execute;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalDataSet;

/**
 * Context available to data dependent functions. When function is executed using
 * {@link FunctionService#onRegion(Region)}, the FunctionContext can be type casted to
 * RegionFunctionContext. Methods provided to retrieve the Region and filter passed to the function
 * execution
 *
 *
 * @since GemFire 6.0
 *
 * @see FunctionContextImpl
 */
public class RegionFunctionContextImpl extends FunctionContextImpl
    implements InternalRegionFunctionContext {

  private final Region dataSet;

  private final Set<?> filter;

  private final Map<String, LocalDataSet> colocatedLocalDataMap;

  private final int[] localBucketArray;

  private final boolean isPossibleDuplicate;

  private final Object principal;

  public RegionFunctionContextImpl(final Cache cache, final String functionId,
      final Region<?, ?> dataSet, final Object args, final Set<?> routingObjects,
      final Map<String, LocalDataSet> colocatedLocalDataMap, int[] localBucketArray,
      ResultSender<?> resultSender, boolean isPossibleDuplicate) {
    this(cache, functionId, dataSet, args, routingObjects, colocatedLocalDataMap, localBucketArray,
        resultSender, isPossibleDuplicate, null);
  }

  public RegionFunctionContextImpl(final Cache cache, final String functionId,
      final Region<?, ?> dataSet, final Object args, final Set<?> routingObjects,
      final Map<String, LocalDataSet> colocatedLocalDataMap, int[] localBucketArray,
      ResultSender<?> resultSender, boolean isPossibleDuplicate, Object principal) {
    super(cache, functionId, args, resultSender);
    this.dataSet = dataSet;
    this.filter = routingObjects;
    this.colocatedLocalDataMap = colocatedLocalDataMap;
    this.localBucketArray = localBucketArray;
    this.isPossibleDuplicate = isPossibleDuplicate;

    if (principal == null) {
      this.principal = ((InternalCache) cache).getSecurityService().getPrincipal();
    } else {
      this.principal = principal;
    }
    setFunctionContexts();
  }

  private void setFunctionContexts() {
    if (this.colocatedLocalDataMap != null) {
      for (LocalDataSet ls : this.colocatedLocalDataMap.values()) {
        ls.setFunctionContext(this);
      }
    }
  }

  /**
   * Returns the Region on which function is executed
   *
   * @see FunctionService#onRegion(Region)
   *
   * @return Returns the Region on which function is executed
   */
  @Override
  public <K, V> Region<K, V> getDataSet() {
    return this.dataSet;
  }

  /**
   * Returns subset of keys provided by the invoking thread (aka routing objects) which may exist in
   * the local data set.
   *
   * @see Execution#withFilter(Set)
   *
   * @return the objects should be local to this context
   */
  @Override
  public Set<?> getFilter() {
    return this.filter;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("[RegionFunctionContextImpl:");
    buf.append("dataSet=");
    buf.append(this.dataSet);
    buf.append(";filter=");
    buf.append(this.filter);
    buf.append(";args=");
    buf.append(getArguments());
    buf.append(";principal=");
    buf.append(getPrincipal());
    buf.append(']');
    return buf.toString();
  }

  @Override
  public Region getLocalDataSet(Region r) {
    if (this.colocatedLocalDataMap != null) {
      return this.colocatedLocalDataMap.get(r.getFullPath());
    } else {
      return null;
    }
  }

  @Override
  public Map<String, LocalDataSet> getColocatedLocalDataSets() {
    if (this.colocatedLocalDataMap != null) {
      HashMap<String, LocalDataSet> ret =
          new HashMap<String, LocalDataSet>(this.colocatedLocalDataMap);
      ret.remove(this.dataSet.getFullPath());
      return Collections.unmodifiableMap(ret);
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public boolean isPossibleDuplicate() {
    return this.isPossibleDuplicate;
  }

  @Override
  public <K, V> int[] getLocalBucketArray(Region<K, V> region) {
    if (!region.getAttributes().getDataPolicy().withPartitioning()) {
      return null;
    }
    return this.localBucketArray;
  }

  @Override
  public Object getPrincipal() {
    return principal;
  }
}
