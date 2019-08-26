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
import java.util.Collection;
import java.util.List;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Responds to {@link RootRegionResponse}.
 */
public class RootRegionResponse extends AdminResponse {

  private String[] regions;
  private String[] userAttrs;

  /**
   * Returns a {@code RootRegionResponse} that will be returned to the specified recipient. The
   * message will contains a copy of the local manager's system config.
   */
  public static RootRegionResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    RootRegionResponse m = new RootRegionResponse();
    try {
      InternalCache cache = (InternalCache) CacheFactory.getInstance(dm.getSystem());
      final Collection roots;
      if (!Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "PRDebug")) {
        roots = cache.rootRegions();
      } else {
        roots = cache.rootRegions(true);
      }

      List regionNames = new ArrayList();
      List userAttributes = new ArrayList();
      for (Object root : roots) {
        Region r = (Region) root;
        regionNames.add(r.getName());
        userAttributes.add(CacheDisplay.getCachedObjectDisplay(r.getUserAttribute(),
            GemFireVM.LIGHTWEIGHT_CACHE_VALUE));
      }

      String[] temp = new String[0];
      m.regions = (String[]) regionNames.toArray(temp);
      m.userAttrs = (String[]) userAttributes.toArray(temp);;

    } catch (CancelException ignore) {
      /* no cache yet */
    }

    m.setRecipient(recipient);
    return m;
  }

  public Region[] getRegions(RemoteGemFireVM vm) {
    if (this.regions.length > 0) {
      Region[] roots = new Region[this.regions.length];
      for (int i = 0; i < this.regions.length; i++) {
        roots[i] = new AdminRegion(this.regions[i], vm, this.userAttrs[i]);
      }
      return roots;
    } else {
      return new Region[0];
    }
  }

  @Override
  public int getDSFID() {
    return ROOT_REGION_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(this.regions, out);
    DataSerializer.writeObject(this.userAttrs, out);
  }

  @Override
  public void fromData(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.regions = DataSerializer.readObject(in);
    this.userAttrs = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "RootRegionResponse from " + this.getRecipient();
  }
}
