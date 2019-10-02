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
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A Partitioned Region meta-data update message. This is used to send a bucket's meta-data to other
 * members with the same Partitioned Region.
 *
 * @since GemFire 5.1
 */
public class BucketProfileUpdateMessage extends DistributionMessage implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;
  private int prId;
  private int bucketId;
  private int processorId = 0;
  private BucketAdvisor.BucketProfile profile;

  public BucketProfileUpdateMessage() {}

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  private BucketProfileUpdateMessage(Set recipients, int partitionedRegionId, int processorId,
      int bucketId, BucketProfile profile) {
    setRecipients(recipients);
    this.processorId = processorId;
    this.prId = partitionedRegionId;
    this.bucketId = bucketId;
    this.profile = profile;
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.prId);
      // pr.waitOnBucketInitialization(); // While PR doesn't directly do GII, wait on this for
      // bucket initialization -- mthomas 5/17/2007
      pr.getRegionAdvisor().putBucketProfile(this.bucketId, this.profile);
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
   * @param bucketId the unique bucket identifier
   * @param bp the updated bucket profile to send
   * @param requireAck whether or not to expect a reply
   * @return an instance of reply processor if requireAck is true on which the caller can wait until
   *         the event has finished.
   */
  public static ReplyProcessor21 send(Set recipients, DistributionManager dm, int prId,
      int bucketId, BucketProfile bp, boolean requireAck) {
    if (recipients.isEmpty()) {
      return null;
    }
    ReplyProcessor21 rp = null;
    int procId = 0;
    if (requireAck) {
      rp = new ReplyProcessor21(dm, recipients);
      procId = rp.getProcessorId();
    }
    BucketProfileUpdateMessage m =
        new BucketProfileUpdateMessage(recipients, prId, procId, bucketId, bp);
    dm.putOutgoing(m);
    return rp;
  }

  @Override
  public int getDSFID() {
    return PR_BUCKET_PROFILE_UPDATE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.prId = in.readInt();
    this.bucketId = in.readInt();
    this.processorId = in.readInt();
    this.profile = (BucketAdvisor.BucketProfile) DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.prId);
    out.writeInt(this.bucketId);
    out.writeInt(this.processorId);
    DataSerializer.writeObject(this.profile, out);
  }

  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    String className = getClass().getName();
    String shortName =
        className.substring(className.lastIndexOf('.', className.lastIndexOf('.') - 1) + 1); // partition.<foo>
    return buff.append(shortName).append("(prid=").append(this.prId).append("; bucketid=")
        .append(this.bucketId).append("; sender=").append(getSender()).append("]; processorId=")
        .append(this.processorId).append("; profile=").append(this.profile).append(")").toString();
  }
}
