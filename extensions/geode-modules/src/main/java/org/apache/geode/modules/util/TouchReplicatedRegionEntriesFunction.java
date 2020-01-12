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
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Touches the keys contained in the set of keys by performing a get on the replicated region.
 * This is a non-data-aware function invoked using onMembers or onServers.
 *
 */
public class TouchReplicatedRegionEntriesFunction
    implements Function, Declarable, DataSerializable {
  private static final long serialVersionUID = -7424895036162243564L;
  public static final String ID = "touch-replicated-region-entries";

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext context) {
    Object[] arguments = (Object[]) context.getArguments();
    Cache cache = context.getCache();
    String regionName = (String) arguments[0];
    Set<String> keys = (Set<String>) arguments[1];
    if (cache.getLogger().fineEnabled()) {
      String builder = "Function " + ID + " received request to touch " + regionName + "->" + keys;
      cache.getLogger().fine(builder);
    }

    // Retrieve the appropriate Region and value to update the lastAccessedTime
    Region region = cache.getRegion(regionName);
    if (region != null) {
      region.getAll(keys);
    }

    // Return result to get around NPE in LocalResultCollectorImpl
    context.getResultSender().lastResult(true);
  }

  @Override
  // the actual regionName used in the function body is passed in as an function arugment,
  // this regionName is not really used in function. Hence requiring DATA:READ on all regions
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(ResourcePermissions.DATA_READ);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    // Setting this to false caused the onServers method to only execute the
    // function on one server.
    return true;
  }

  @Override
  public void toData(DataOutput out) {}

  @Override
  public void fromData(DataInput in) {}
}
