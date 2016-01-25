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
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

/**
 * List of remote locators as a response
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 * 
 * 
 */
public class RemoteLocatorJoinResponse implements DataSerializableFixedID{

  private HashMap<Integer, Set<DistributionLocatorId>> locators = new HashMap<Integer, Set<DistributionLocatorId>>();
  
  /** Used by DataSerializer */
  public RemoteLocatorJoinResponse() {
    super();
  }

  public RemoteLocatorJoinResponse(
      Map<Integer, Set<DistributionLocatorId>> locators) {
    super();
    this.locators = new HashMap<Integer, Set<DistributionLocatorId>>();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : locators
        .entrySet()) {
      this.locators.put(entry.getKey(), new CopyOnWriteHashSet<DistributionLocatorId>(
          entry.getValue()));
    }
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.locators = DataSerializer.readHashMap(in);
    
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashMap(locators, out);
  }

  public Map<Integer, Set<DistributionLocatorId>> getLocators() {
    return this.locators;
  }

  @Override
  public String toString() {
    return "RemoteLocatorJoinResponse{locators=" + locators + "}";
  }

  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_JOIN_RESPONSE;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
