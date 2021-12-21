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
package org.apache.geode.cache.client.internal.locator.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * List of remote locators as a response
 *
 *
 *
 */
public class RemoteLocatorJoinResponse implements DataSerializableFixedID {

  private HashMap<Integer, Set<DistributionLocatorId>> locators =
      new HashMap<Integer, Set<DistributionLocatorId>>();

  /** Used by DataSerializer */
  public RemoteLocatorJoinResponse() {
    super();
  }

  public RemoteLocatorJoinResponse(Map<Integer, Set<DistributionLocatorId>> locators) {
    super();
    this.locators = new HashMap<Integer, Set<DistributionLocatorId>>();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : locators.entrySet()) {
      this.locators.put(entry.getKey(),
          new CopyOnWriteHashSet<DistributionLocatorId>(entry.getValue()));
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    locators = DataSerializer.readHashMap(in);

  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeHashMap(locators, out);
  }

  public Map<Integer, Set<DistributionLocatorId>> getLocators() {
    return locators;
  }

  @Override
  public String toString() {
    return "RemoteLocatorJoinResponse{locators=" + locators + "}";
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_JOIN_RESPONSE;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

}
