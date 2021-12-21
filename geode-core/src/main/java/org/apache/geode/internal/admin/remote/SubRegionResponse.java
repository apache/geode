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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Responds to {@link SubRegionResponse}.
 */
public class SubRegionResponse extends AdminResponse {
  // instance variables
  String[] subRegionNames;
  String[] userAttributes;

  /**
   * Returns a <code>SubRegionResponse</code> that will be returned to the specified recipient. The
   * message will contains a copy of the local manager's system config.
   */
  public static SubRegionResponse create(DistributionManager dm,
      InternalDistributedMember recipient, Region r) {
    SubRegionResponse m = new SubRegionResponse();
    m.setRecipient(recipient);

    Set subregions = r.subregions(false);

    List subNames = new ArrayList();
    List userAttrs = new ArrayList();
    Iterator it = subregions.iterator();
    while (it.hasNext()) {
      Region reg = (Region) it.next();
      subNames.add(reg.getName());
      userAttrs.add(CacheDisplay.getCachedObjectDisplay(reg.getUserAttribute(),
          GemFireVM.LIGHTWEIGHT_CACHE_VALUE));
    }

    String[] temp = new String[0];
    m.subRegionNames = (String[]) subNames.toArray(temp);
    m.userAttributes = (String[]) userAttrs.toArray(temp);

    return m;
  }

  // instance methods
  public Set getRegionSet(AdminRegion parent) {
    // String globalParentName = parent.getFullPath();
    HashSet result = new HashSet(subRegionNames.length);
    for (int i = 0; i < subRegionNames.length; i++) {
      result.add(new AdminRegion(subRegionNames[i], parent, userAttributes[i]));
    }
    return new HashSet(result);
  }

  @Override
  public int getDSFID() {
    return SUB_REGION_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(subRegionNames, out);
    DataSerializer.writeObject(userAttributes, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    subRegionNames = DataSerializer.readObject(in);
    userAttributes = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "SubRegionResponse from " + getRecipient();
  }
}
