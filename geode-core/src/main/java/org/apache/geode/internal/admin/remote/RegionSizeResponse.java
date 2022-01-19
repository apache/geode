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

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Responds to {@link RegionSizeRequest}.
 */
public class RegionSizeResponse extends AdminResponse implements Cancellable {
  // instance variables
  private int entryCount;
  private int subregionCount;
  private transient boolean cancelled;

  /**
   * Returns a <code>RegionSizeResponse</code> that will be returned to the specified recipient.
   */
  public static RegionSizeResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    RegionSizeResponse m = new RegionSizeResponse();
    m.setRecipient(recipient);
    return m;
  }

  public void calcSize(Region r) {
    if (cancelled) {
      return;
    }

    Set nameSet = r.keySet();
    if (cancelled) {
      return;
    }
    entryCount = nameSet.size();
    Set subRegions = r.subregions(false);
    if (cancelled) {
      return;
    }
    subregionCount = subRegions.size();
  }

  @Override
  public synchronized void cancel() {
    cancelled = true;
  }

  // instance methods
  public int getEntryCount() {
    return entryCount;
  }

  public int getSubregionCount() {
    return subregionCount;
  }

  @Override
  public int getDSFID() {
    return REGION_SIZE_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(entryCount);
    out.writeInt(subregionCount);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    entryCount = in.readInt();
    subregionCount = in.readInt();
  }

  @Override
  public String toString() {
    return "RegionSizeResponse from " + getRecipient();
  }
}
