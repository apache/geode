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
package org.apache.geode.modules.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

/**
 * Touches the keys contained in the set of keys by performing a get on the partitioned region.
 *
 */
public class TouchPartitionedRegionEntriesFunction
    implements Function, Declarable, DataSerializable {

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
      builder.append("Function ").append(ID).append(" received request to touch ")
          .append(primaryDataSet.getFullPath()).append("->").append(keys);
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

  public void init(Properties properties) {}

  @Override
  public void toData(DataOutput out) throws IOException {

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

  }
}
