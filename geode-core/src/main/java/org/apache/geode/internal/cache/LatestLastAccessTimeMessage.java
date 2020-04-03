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
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Sends the region name and key of the entry that we want the last access time for. If for any
 * reason this message can not obtain the last access time then zero will be returned.
 *
 * @since Geode 1.4
 */
public class LatestLastAccessTimeMessage<K> extends PooledDistributionMessage
    implements MessageWithReply {

  private int processorId;
  private String regionName;
  private K key;

  public LatestLastAccessTimeMessage() {
    // nothing
  }

  public LatestLastAccessTimeMessage(LatestLastAccessTimeReplyProcessor replyProcessor,
      Set<InternalDistributedMember> recipients, InternalDistributedRegion region, K key) {
    this.setRecipients(recipients);
    this.processorId = replyProcessor.getProcessorId();
    this.key = key;
    this.regionName = region.getFullPath();
  }

  @Override
  public int getDSFID() {
    return LATEST_LAST_ACCESS_TIME_MESSAGE;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    long latestLastAccessTime = 0L;
    InternalCache cache = dm.getCache();
    if (cache == null) {
      return;
    }
    InternalDistributedRegion region =
        (InternalDistributedRegion) cache.getRegion(this.regionName);
    if (region == null) {
      return;
    }
    RegionEntry entry = region.getRegionEntry(this.key);
    if (entry == null) {
      return;
    }
    try {
      latestLastAccessTime = entry.getLastAccessed();
    } catch (InternalStatisticsDisabledException ignored) {
      // last access time is not available
    }
    ReplyMessage.send(getSender(), this.processorId, latestLastAccessTime, dm);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.processorId = DataSerializer.readPrimitiveInt(in);
    this.regionName = DataSerializer.readString(in);
    this.key = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveInt(this.processorId, out);
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeObject(this.key, out);
  }
}
