/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.execute;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * Context available to data dependent functions. When function is executed
 * using {@link FunctionService#onRegion(Region)}, the FunctionContext can be
 * type casted to RegionFunctionContext. Methods provided to retrieve the Region
 * and filter passed to the function execution
 * 
 * @author Yogesh Mahajan
 * 
 * @since 6.0
 * 
 * @see FunctionContextImpl
 */
public class RegionFunctionContextImpl extends FunctionContextImpl implements
    InternalRegionFunctionContext {

  private final Region dataSet;

  private final Set<?> filter;

  private final Map<String, LocalDataSet> colocatedLocalDataMap;

  private final Set<Integer> localBucketSet;

  private final boolean isPossibleDuplicate;

  public RegionFunctionContextImpl(final String functionId,
      final Region dataSet, final Object args,
      final Set<?> routingObjects,
      final Map<String, LocalDataSet> colocatedLocalDataMap,
      Set<Integer> localBucketSet, ResultSender<?> resultSender,
      boolean isPossibleDuplicate) {
    super(functionId, args, resultSender);
    this.dataSet = dataSet;
    this.filter = routingObjects;
    this.colocatedLocalDataMap = colocatedLocalDataMap;
    this.localBucketSet = localBucketSet;
    this.isPossibleDuplicate = isPossibleDuplicate;
    setFunctionContexts();
    // set the region context for keys if required
    if (routingObjects != null) {
      final LocalRegion r = (LocalRegion)this.dataSet;
      if (r.keyRequiresRegionContext()) {
        for (Object key : routingObjects) {
          if (key instanceof KeyWithRegionContext) {
            ((KeyWithRegionContext)key).setRegionContext(r);
          }
        }
      }
    }
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
  public <K, V> Region<K, V> getDataSet() {
    return this.dataSet;
  }

  /**
   * Returns subset of keys provided by the invoking thread (aka routing
   * objects) which may exist in the local data set.
   * 
   * @see Execution#withFilter(Set)
   * 
   * @return the objects should be local to this context
   */
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
    buf.append(']');
    return buf.toString();
  }

  public Region getLocalDataSet(Region r) {
    if (this.colocatedLocalDataMap != null) {
      return this.colocatedLocalDataMap.get(r.getFullPath());
    }
    else {
      return null;
    }
  }

  public Map<String, LocalDataSet> getColocatedLocalDataSets() {
    if (this.colocatedLocalDataMap != null) {
      HashMap<String, LocalDataSet> ret = new HashMap<String, LocalDataSet>(
          this.colocatedLocalDataMap);
      ret.remove(this.dataSet.getFullPath());
      return Collections.unmodifiableMap(ret);
    }
    else {
      return Collections.emptyMap();
    }
  }

  public boolean isPossibleDuplicate() {
    return this.isPossibleDuplicate;
  }

  public <K, V> Set<Integer> getLocalBucketSet(Region<K, V> region) {
    if (!region.getAttributes().getDataPolicy().withPartitioning()) {
      return null;
    }
    return this.localBucketSet;
  }
}
