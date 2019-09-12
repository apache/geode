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
package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A Partitioned Region meta-data update message. This is used to send all local bucket's meta-data
 * to other members with the same Partitioned Region.
 *
 * @since GemFire 6.6
 */
public class AllBucketProfilesUpdateMessage extends DistributionMessage
    implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;
  private int prId;
  private int processorId = 0;
  private Map<Integer, BucketAdvisor.BucketProfile> profiles;

  public AllBucketProfilesUpdateMessage() {}

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  private AllBucketProfilesUpdateMessage(Set recipients, int partitionedRegionId, int processorId,
      Map<Integer, BucketAdvisor.BucketProfile> profiles) {
    setRecipients(recipients);
    this.processorId = processorId;
    this.prId = partitionedRegionId;
    this.profiles = profiles;
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.prId);
      for (Map.Entry<Integer, BucketAdvisor.BucketProfile> profile : this.profiles.entrySet()) {
        pr.getRegionAdvisor().putBucketProfile(profile.getKey(), profile.getValue());
      }
    } catch (PRLocallyDestroyedException fre) {
      if (logger.isDebugEnabled())
        logger.debug("<region locally destroyed> ///{}", this);
    } catch (RegionDestroyedException e) {
      if (logger.isDebugEnabled())
        logger.debug("<region destroyed> ///{}", this);
    } catch (CancelException e) {
      if (logger.isDebugEnabled())
        logger.debug("<cache closed> ///{}", this);
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable ignore) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    } finally {
      if (this.processorId != 0) {
        ReplyMessage.send(getSender(), this.processorId, null, dm);
      }
    }
  }

  /**
   * Send a profile update to a set of members.
   *
   * @param recipients the set of members to be notified
   * @param dm the distribution manager used to send the message
   * @param prId the unique partitioned region identifier
   * @param profiles bucked id to profile map
   * @return an instance of reply processor if requireAck is true on which the caller can wait until
   *         the event has finished.
   */
  public static ReplyProcessor21 send(Set recipients, DistributionManager dm, int prId,
      Map<Integer, BucketAdvisor.BucketProfile> profiles) {
    if (recipients.isEmpty()) {
      return null;
    }
    ReplyProcessor21 rp = null;
    int procId = 0;
    rp = new ReplyProcessor21(dm, recipients);
    procId = rp.getProcessorId();
    AllBucketProfilesUpdateMessage m =
        new AllBucketProfilesUpdateMessage(recipients, prId, procId, profiles);
    dm.putOutgoing(m);
    return rp;
  }

  @Override
  public int getDSFID() {
    return PR_ALL_BUCKET_PROFILES_UPDATE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.prId = in.readInt();
    this.processorId = in.readInt();
    this.profiles = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.prId);
    out.writeInt(this.processorId);
    DataSerializer.writeObject(this.profiles, out);
  }

}
