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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.RegionSubRegionSnapshot;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Admin response carrying region info for a member
 */
public class RegionSubRegionsSizeResponse extends AdminResponse implements Cancellable {

  private static final Logger logger = LogService.getLogger();

  private RegionSubRegionSnapshot snapshot;

  private boolean cancelled;

  public RegionSubRegionsSizeResponse() {
    // do nothing
  }

  public RegionSubRegionSnapshot getSnapshot() {
    return snapshot;
  }

  /**
   * Returns a {@code RegionSubRegionsSizeResponse} that will be returned to the specified
   * recipient. The message will contains a copy of the region snapshot
   */
  public static RegionSubRegionsSizeResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    RegionSubRegionsSizeResponse m = new RegionSubRegionsSizeResponse();
    m.setRecipient(recipient);
    m.snapshot = null;
    m.cancelled = false;

    return m;
  }

  void populateSnapshot(DistributionManager dm) {
    if (cancelled) {
      return;
    }

    DistributedSystem sys = dm.getSystem();
    InternalCache cache = (InternalCache) CacheFactory.getInstance(sys);

    if (cancelled) {
      return;
    }

    RegionSubRegionSnapshot root = new RegionSubRegionSnapshot();
    /*
     * This root exists only on admin side as a root of all root-region just to create a tree-like
     * structure
     */
    root.setName("Root");
    root.setParent(null);
    root.setEntryCount(0);

    Set rootRegions = cache.rootRegions();
    snapshot = root;

    populateRegionSubRegions(root, rootRegions, cache);
  }

  /**
   * Populates the collection of sub-region snapshots for the parentSnapShot with snapshots for the
   * regions given.
   *
   * @param parentSnapShot RegionSubRegionSnapshot of a parent region
   * @param regions collection of sub-regions of the region represented by parentSnapShot
   * @param cache cache instance is used for to get the LogWriter instance to log exceptions if any
   */
  private void populateRegionSubRegions(RegionSubRegionSnapshot parentSnapShot, Set regions,
      InternalCache cache) {
    if (cancelled) {
      return;
    }

    for (Object region : regions) {
      Region subRegion = (Region) region;

      try {
        RegionSubRegionSnapshot subRegionSnapShot = new RegionSubRegionSnapshot(subRegion);
        parentSnapShot.addSubRegion(subRegionSnapShot);

        Set subRegions = subRegion.subregions(false);
        populateRegionSubRegions(subRegionSnapShot, subRegions, cache);
      } catch (Exception e) {
        logger.debug("Failed to create snapshot for region: {}. Continuing with next region.",
            subRegion.getFullPath(), e);
      }
    }
  }

  @Override
  public synchronized void cancel() {
    cancelled = true;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeBoolean(cancelled);
    DataSerializer.writeObject(snapshot, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    cancelled = in.readBoolean();
    snapshot = DataSerializer.readObject(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  @Override
  public int getDSFID() {
    return REGION_SUB_SIZE_RESPONSE;
  }

  @Override
  public String toString() {
    return "RegionSubRegionsSizeResponse [from=" + getRecipient() + ' '
        + (snapshot == null ? "null" : snapshot.toString());
  }
}
