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

import static java.util.Collections.singletonList;
import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Touches the keys contained in the set of keys by performing a get on the replicated region.
 * This is a non-data-aware function invoked using onMembers or onServers.
 */
public class TouchReplicatedRegionEntriesFunction
    implements Function, Declarable, DataSerializable {
  private static final Logger logger = LogService.getLogger();

  public static final String ID = "touch-replicated-region-entries";

  @Override
  public void execute(FunctionContext context) {
    Object[] arguments = (Object[]) context.getArguments();
    Cache cache = context.getCache();
    String regionName = (String) arguments[0];
    Collection<String> keys = cast(arguments[1]);

    if (logger.isDebugEnabled()) {
      logger.debug("Function {} received request to touch {}->{}", ID, regionName, keys);
    }

    // Retrieve the appropriate Region and value to update the lastAccessedTime
    Region<?, ?> region = cache.getRegion(regionName);
    if (region != null) {
      region.getAll(keys);
    }

    // Return result to get around NPE in LocalResultCollectorImpl
    context.getResultSender().lastResult(true);
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    // the actual regionName used in the function body is passed in as an function argument,
    // this regionName is not really used in function. Hence requiring DATA:READ on all regions
    return singletonList(ResourcePermissions.DATA_READ);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public void toData(DataOutput out) {
    // nothing
  }

  @Override
  public void fromData(DataInput in) {
    // nothing
  }

  private static final long serialVersionUID = -7424895036162243564L;
}
