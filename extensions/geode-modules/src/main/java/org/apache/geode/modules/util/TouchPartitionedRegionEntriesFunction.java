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
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.security.ResourcePermission;

/**
 * Touches the keys contained in the set of keys by performing a get on the partitioned region.
 *
 */
public class TouchPartitionedRegionEntriesFunction
    implements Function, Declarable, DataSerializable {
  private static final long serialVersionUID = -3700389655056961153L;
  public static final String ID = "touch-partitioned-region-entries";

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext context) {
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    Set<String> keys = (Set<String>) rfc.getFilter();

    Cache cache = context.getCache();
    // Get local (primary) data for the context
    Region primaryDataSet = getLocalDataForContextViaRegionHelper(rfc);

    if (cache.getLogger().fineEnabled()) {
      String builder = "Function " + ID + " received request to touch "
          + primaryDataSet.getFullPath() + "->" + keys;
      cache.getLogger().fine(builder);
    }

    // Retrieve each value to update the lastAccessedTime.
    // Note: getAll is not supported on LocalDataSet.
    for (String key : keys) {
      primaryDataSet.get(key);
    }

    // Return result to get around NPE in LocalResultCollectorImpl
    context.getResultSender().lastResult(true);
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, regionName));
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void toData(DataOutput out) {}

  @Override
  public void fromData(DataInput in) {}

  // Helper methods added to improve unit testing of class
  Region getLocalDataForContextViaRegionHelper(RegionFunctionContext rfc) {
    return PartitionRegionHelper.getLocalDataForContext(rfc);
  }
}
