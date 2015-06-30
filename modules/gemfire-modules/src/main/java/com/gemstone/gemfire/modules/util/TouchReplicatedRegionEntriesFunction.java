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
 * replicated region. This is a non-data-aware function invoked using onMembers
 * or onServers.
 * 
 * @author Barry Oglesby
 */
public class TouchReplicatedRegionEntriesFunction implements Function, Declarable {

  private static final long serialVersionUID = -7424895036162243564L;

  private final Cache cache;
  
  public static final String ID = "touch-replicated-region-entries";
  
  public TouchReplicatedRegionEntriesFunction() {
    this(CacheFactory.getAnyInstance());
  }
  
  public TouchReplicatedRegionEntriesFunction(Cache cache) {
    this.cache = cache;
  }
  
  public void execute(FunctionContext context) {
    Object[] arguments = (Object[]) context.getArguments();
    String regionName = (String) arguments[0];
    Set<String> keys = (Set<String>) arguments[1];
    if (this.cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder
        .append("Function ")
        .append(ID)
        .append(" received request to touch ")
        .append(regionName)
        .append("->")
        .append(keys);
      this.cache.getLogger().fine(builder.toString());
    }

    // Retrieve the appropriate Region and value to update the lastAccessedTime
    Region region = this.cache.getRegion(regionName);
    if (region != null) {
      region.getAll(keys);
    }
    
    // Return result to get around NPE in LocalResultCollectorImpl
    context.getResultSender().lastResult(true);
  }
  
  public String getId() {
    return ID;
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public boolean isHA() {
    return false;
  }

  public boolean hasResult() {
    // Setting this to false caused the onServers method to only execute the
    // function on one server.
    return true;
  }

  public void init(Properties properties) {
  }
}

