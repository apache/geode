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
package org.apache.geode.internal.cache.persistence;

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketPersistenceAdvisor;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class MembershipFlushRequest extends PooledDistributionMessage implements MessageWithReply {

  private String regionPath;
  private int processorId;

  public MembershipFlushRequest() {}

  public MembershipFlushRequest(String regionPath, int processorId) {
    this.regionPath = regionPath;
    this.processorId = processorId;
  }

  public static void send(Set<InternalDistributedMember> recipients, DistributionManager dm,
      String regionPath) throws ReplyException {
    ReplyProcessor21 processor = new ReplyProcessor21(dm, recipients);
    MembershipFlushRequest msg = new MembershipFlushRequest(regionPath, processor.getProcessorId());
    msg.setRecipients(recipients);
    dm.putOutgoing(msg);
    processor.waitForRepliesUninterruptibly();
  }


  @Override
  protected void process(ClusterDistributionManager dm) {
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);

    ReplyException exception = null;
    try {
      // get the region from the path, but do NOT wait on initialization,
      // otherwise we could have a distributed deadlock

      Cache cache = dm.getExistingCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionPath);
      if (region != null && region.getRegionAdvisor().isInitialized()) {
        ProxyBucketRegion[] proxyBuckets = region.getRegionAdvisor().getProxyBucketArray();
        // buckets are null if initPRInternals is still not complete
        if (proxyBuckets != null) {
          for (ProxyBucketRegion bucket : proxyBuckets) {
            final BucketPersistenceAdvisor persistenceAdvisor = bucket.getPersistenceAdvisor();
            if (persistenceAdvisor != null) {
              persistenceAdvisor.flushMembershipChanges();
            }
          }
        }
      }
    } catch (RegionDestroyedException e) {
      // ignore
    } catch (CancelException e) {
      // ignore
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      exception = new ReplyException(t);
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
      ReplyMessage replyMsg = new ReplyMessage();
      replyMsg.setRecipient(getSender());
      replyMsg.setProcessorId(processorId);
      if (exception != null) {
        replyMsg.setException(exception);
      }
      dm.putOutgoing(replyMsg);
    }
  }

  @Override
  public int getDSFID() {
    return PERSISTENT_MEMBERSHIP_FLUSH_REQUEST;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    processorId = in.readInt();
    regionPath = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(processorId);
    DataSerializer.writeString(regionPath, out);
  }
}
