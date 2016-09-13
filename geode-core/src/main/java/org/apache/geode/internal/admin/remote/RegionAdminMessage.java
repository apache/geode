/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.*;
import org.apache.geode.cache.*;
//import org.apache.geode.internal.*;
//import org.apache.geode.internal.cache.*;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.distributed.DistributedSystem;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular app vm on a distribution manager to
 * make an administration request about a particular region. It does not return a response.
 */
public abstract class RegionAdminMessage extends PooledDistributionMessage {
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
    return cache.getRegion(this.regionName);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.regionName, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionName = DataSerializer.readString(in);
  }
}
