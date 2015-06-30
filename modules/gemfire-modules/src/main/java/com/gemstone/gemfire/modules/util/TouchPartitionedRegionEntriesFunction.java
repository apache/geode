/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.cache.*;

import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

import java.util.Properties;
import java.util.Set;

/**
 * Touches the keys contained in the set of keys by performing a get on the
 * partitioned region.
 * 
 * @author Barry Oglesby
 */
public class TouchPartitionedRegionEntriesFunction implements Function, Declarable {

  private static final long serialVersionUID = -3700389655056961153L;

  private final Cache cache;
  
  public static final String ID = "touch-partitioned-region-entries";
  
  public TouchPartitionedRegionEntriesFunction() {
    this(CacheFactory.getAnyInstance());
  }
  
  public TouchPartitionedRegionEntriesFunction(Cache cache) {
    this.cache = cache;
  }
  
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext context) {
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    Set<String> keys = (Set<String>) rfc.getFilter();
    
    // Get local (primary) data for the context
    Region primaryDataSet = PartitionRegionHelper.getLocalDataForContext(rfc);
    
    if (this.cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder
        .append("Function ")
        .append(ID)
        .append(" received request to touch ")
        .append(primaryDataSet.getFullPath())
        .append("->")
        .append(keys);
      this.cache.getLogger().fine(builder.toString());
    }

    // Retrieve each value to update the lastAccessedTime.
    // Note: getAll is not supported on LocalDataSet.
    for (String key : keys) {
      primaryDataSet.get(key);
    }
    
    // Return result to get around NPE in LocalResultCollectorImpl
    context.getResultSender().lastResult(true);
  }
  
  public String getId() {
    return ID;
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return false;
  }

  public boolean hasResult() {
    return true;
  }

  public void init(Properties properties) {
  }
}

