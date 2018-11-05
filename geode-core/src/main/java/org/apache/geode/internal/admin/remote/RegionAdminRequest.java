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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.RegionNotFoundException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;

/**
 * A message that is sent to a particular app vm on a distribution manager to make an administration
 * request about a particular region.
 */
public abstract class RegionAdminRequest extends AdminRequest {
  // instance variables
  private String regionName;

  public void setRegionName(String name) {
    this.regionName = name;
  }

  public String getRegionName() {
    return this.regionName;
  }

  /**
   * @throws org.apache.geode.cache.CacheRuntimeException if no cache created
   */
  protected Region getRegion(DistributedSystem sys) {
    Cache cache = CacheFactory.getInstance(sys);
    Region r = cache.getRegion(regionName);
    if (r == null) {
      throw new RegionNotFoundException(
          String.format("Region %s not found in remote cache %s.",
              new Object[] {regionName, cache.getName()}));
    }
    return r;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.regionName, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionName = DataSerializer.readString(in);
  }
}
