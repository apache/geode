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
package org.apache.geode.internal.cache.functions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/*
 * Function for Executing a fire-and-forget function on all servers as opposed to only executing on
 * the ones the client is currently connected to.
 */
public class FireAndForgetFunctionOnAllServers implements Function, DataSerializable {

  public FireAndForgetFunctionOnAllServers() {}

  public static final String ID = FireAndForgetFunctionOnAllServers.class.getName();

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public void execute(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    Cache cache = CacheFactory.getAnyInstance();
    String regionName = (String) context.getArguments();
    Region<String, Integer> region1 = cache.getRegion(regionName);
    if (region1 == null) {
      RegionFactory<String, Integer> rf;
      rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
      region1 = rf.create(regionName);
    }
    region1.put(ds.getDistributedMember().toString(), 1);

    logger.info("Executing FireAndForgetFunctionOnAllServers on Member : "
        + ds.getDistributedMember() + " with Context : " + context);

    if (!hasResult()) {
      return;
    }
  }

  @Override
  public String getId() {
    return FireAndForgetFunctionOnAllServers.ID;
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
  public void toData(DataOutput out) throws IOException {

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

  }
}
