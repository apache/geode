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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalStatisticsDisabledException;

public class NonLocalRegionEntryWithStats extends NonLocalRegionEntry {
  private long hitCount;
  private long missCount;
  private long lastAccessed;

  public NonLocalRegionEntryWithStats(RegionEntry re, LocalRegion br, boolean allowTombstones) {
    super(re, br, allowTombstones);
    try {
      lastAccessed = re.getLastAccessed();
      hitCount = re.getHitCount();
      missCount = re.getMissCount();
    } catch (InternalStatisticsDisabledException unexpected) {
      Assert.assertTrue(false, "Unexpected " + unexpected);
    }
  }

  @Override
  public boolean hasStats() {
    return true;
  }

  @Override
  public long getLastAccessed() throws StatisticsDisabledException {
    return lastAccessed;
  }

  @Override
  public long getHitCount() throws StatisticsDisabledException {
    return hitCount;
  }

  @Override
  public long getMissCount() throws StatisticsDisabledException {
    return missCount;
  }

  public NonLocalRegionEntryWithStats() {
    // for fromData
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(lastAccessed);
    out.writeLong(hitCount);
    out.writeLong(missCount);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    lastAccessed = in.readLong();
    hitCount = in.readLong();
    missCount = in.readLong();
  }
}
