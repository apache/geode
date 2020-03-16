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
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionException;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class represents a partition message for removing indexes. An instance of this class is send
 * over the wire to remove indexes on remote vms. This class extends PartitionMessage
 * {@link org.apache.geode.internal.cache.partitioned.PartitionMessage}
 *
 *
 */
public class RemoveIndexesMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  /**
   * Name of the index to be removed.
   */
  private String indexName;

  /**
   * Boolean indicating only a single index has to be removed.
   */
  private boolean removeSingleIndex;

  /**
   * Constructor.
   */
  public RemoveIndexesMessage() {

  }

  /**
   * Constructor for remove indexes to be sent over the wire.
   *
   * @param recipients members to which this message has to be sent
   * @param regionId partitioned region id
   * @param processor the processor to reply to
   */
  public RemoveIndexesMessage(Set recipients, int regionId, ReplyProcessor21 processor) {
    super(recipients, regionId, processor);
  }

  /**
   * Constructor to remove a particular index which will be sent over the wire.
   *
   * @param recipients members to which this message has to be sent
   * @param regionId partitioned region id
   * @param processor the processor to reply to
   * @param removeSingleIndex boolean indicating to remove a partitular index
   *
   * @param indexName name of the index to be removed.
   *
   */
  public RemoveIndexesMessage(Set recipients, int regionId, ReplyProcessor21 processor,
      boolean removeSingleIndex, String indexName) {
    super(recipients, regionId, processor);
    this.removeSingleIndex = removeSingleIndex;
    this.indexName = indexName;
  }

  /**
   * This message may be sent to nodes before the PartitionedRegion is completely initialized due to
   * the RegionAdvisor(s) knowing about the existence of a partitioned region at a very early part
   * of the initialization
   */
  @Override
  protected boolean failIfRegionMissing() {
    return false;
  }

  /**
   * This method is responsible to remove index on the given partitioned region.
   *
   * @param dm Distribution maanger for the system
   * @param pr Partitioned region to remove indexes on.
   *
   * @throws CacheException indicates a cache level error
   * @throws ForceReattemptException if the peer is no longer available
   * @throws InterruptedException if the thread is interrupted in the operation for example during
   *         shutdown.
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime)
      throws CacheException, QueryException, ForceReattemptException, InterruptedException {
    // TODO Auto-generated method stub

    ReplyException replyEx = null;
    boolean result = true;
    int bucketIndexRemoved = 0; // invalid
    int numIndexesRemoved = 0;

    logger.info("Will remove the indexes on this pr : {}", pr);
    try {
      if (this.removeSingleIndex) {
        bucketIndexRemoved = pr.removeIndex(this.indexName);
      } else {
        bucketIndexRemoved = pr.removeIndexes(true); // remotely orignated
      }
      numIndexesRemoved = pr.getDataStore().getAllLocalBuckets().size();
    } catch (Exception ex) {
      result = false;
      replyEx = new ReplyException(ex);
    }

    // send back the reply.
    sendReply(getSender(), getProcessorId(), dm, replyEx, result, bucketIndexRemoved,
        numIndexesRemoved);

    return false;
  }

  /**
   * Send a reply for remove indexes message.
   *
   * @param member representing the actual index creatro in the system
   * @param procId waiting processor
   * @param dm distirbution manager to send the message
   * @param ex any exceptions
   * @param result represents remove index worked properly.
   * @param bucketIndexesRemoved number of bucket indexes removed properly.
   */
  void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, boolean result, int bucketIndexesRemoved, int totalNumBuckets) {
    RemoveIndexesReplyMessage.send(member, processorId, dm, ex, result, bucketIndexesRemoved,
        totalNumBuckets);

  }

  /**
   * Sends this RemoveIndexesMessage to all the participating members in the system.
   *
   * @param pr prartitioned region to remove the index on.
   * @return PartitionResponse indicating sucessful remove index
   *
   */

  public static PartitionResponse send(PartitionedRegion pr, Index ind, boolean removeAllIndex) {
    RemoveIndexesResponse processor = null;
    // PartitionResponse processor = null;
    RegionAdvisor advisor = (RegionAdvisor) (pr.getDistributionAdvisor());
    final Set recipients = new HashSet(advisor.adviseDataStore());
    // removing the originator for remove index command.
    recipients.remove(pr.getDistributionManager().getDistributionManagerId());

    // RemoveIndexesResponse processor = null;
    // RemoveIndexesMessage removeIndexesMsg = new RemoveIndexesMessage();
    if (recipients.size() > 0) {

      processor =
          (RemoveIndexesResponse) (new RemoveIndexesMessage()).createReplyProcessor(pr, recipients);

    }
    if (removeAllIndex) {
      RemoveIndexesMessage rm = new RemoveIndexesMessage(recipients, pr.getPRId(), processor);
      rm.setTransactionDistributed(pr.getCache().getTxManager().isDistributed());
      /* Set failures = */ pr.getDistributionManager().putOutgoing(rm);
    } else {
      // remove a single index.
      RemoveIndexesMessage rm =
          new RemoveIndexesMessage(recipients, pr.getPRId(), processor, true, ind.getName());
      rm.setTransactionDistributed(pr.getCache().getTxManager().isDistributed());
      /* Set failures = */ pr.getDistributionManager().putOutgoing(rm);
    }
    return processor;

  }

  @Override
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients) {
    return new RemoveIndexesResponse(r.getSystem(), recipients);
  }

  @Override
  public int getDSFID() {
    return PR_REMOVE_INDEXES_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.removeSingleIndex = in.readBoolean();
    if (this.removeSingleIndex)
      this.indexName = in.readUTF();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeBoolean(this.removeSingleIndex);
    if (this.removeSingleIndex)
      out.writeUTF(this.indexName);
  }

  /**
   * Processes remove index on the receiver.
   */
  @Override
  public void process(final ClusterDistributionManager dm) {

    Throwable thr = null;
    boolean sendReply = true;
    PartitionedRegion pr = null;

    try {
      logger.info("Trying to get pr with id : {}", this.regionId);
      pr = PartitionedRegion.getPRFromId(this.regionId);
      logger.info("Remove indexes message got the pr {}", pr);

      if (pr == null /* && failIfRegionMissing() */ ) {
        throw new PartitionedRegionException(
            String.format(
                "Could not get Partitioned Region from Id %s for message %s received on member= %s map= %s",
                new Object[] {Integer.valueOf(this.regionId), this, dm.getId(),
                    PartitionedRegion.dumpPRId()}));
      }
      // remove the indexes on the pr.
      sendReply = operateOnPartitionedRegion(dm, pr, 0);


    } catch (PRLocallyDestroyedException pde) {
      if (logger.isDebugEnabled()) {
        logger.debug("Region is locally Destroyed ");
      }
      thr = pde;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // log the exception at fine level if there is no reply to the message
      if (this.processorId == 0) {
        logger.debug("{} exception while processing message: {}", this, t.getMessage(), t);
      } else if (logger.isTraceEnabled(LogMarker.DM_VERBOSE) && (t instanceof RuntimeException)) {
        logger.trace(LogMarker.DM_VERBOSE, "Exception caught while processing message: {}",
            t.getMessage(), t);
      }
      if (t instanceof RegionDestroyedException && pr != null) {
        if (pr.isClosed) {
          logger.info("Region is locally destroyed, throwing RegionDestroyedException for {}",
              pr);
          thr = new RegionDestroyedException(
              String.format("Region is locally destroyed on %s",
                  dm.getId()),
              pr.getFullPath());
        }
      } else {
        thr = t;
      }
    } finally {
      if (sendReply && this.processorId != 0) {
        ReplyException rex = null;
        if (thr != null) {
          rex = new ReplyException(thr);
        }
        sendReply(getSender(), this.processorId, dm, rex, pr, 0);
      }
    }


  }

  /**
   * Class representing remove index response. This class has all the information for successful or
   * unsucessful remove index on the member of the partitioned region.
   *
   *
   */
  public static class RemoveIndexesResponse extends PartitionResponse {

    /**
     * Number of buckets index removed.
     */
    private int numBucketIndexRemoved;

    /**
     * Total number of buckets in the sytem.
     */
    private int numTotalRemoteBuckets;

    /**
     * Constructor.
     */
    public RemoveIndexesResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    /**
     * Waits for the response from the members for remove indexes call on this system.
     *
     */
    public RemoveIndexesResult waitForResults() throws CacheException, ForceReattemptException {
      waitForCacheException();
      return new RemoveIndexesResult(0);
    }

    /**
     * Sets the relevant information in the response.
     *
     * @param result true if index removed properly
     * @param numBucketsIndexesRemoved number of buckets indexes removed remotely for a memeber.
     * @param numTotalBuckets number of total buckets in the member.
     */
    public void setResponse(boolean result, int numBucketsIndexesRemoved, int numTotalBuckets) {
      // this.result = result;
      this.numBucketIndexRemoved += numBucketsIndexesRemoved;
      this.numTotalRemoteBuckets += numTotalBuckets;
    }

    /**
     * Returns number of remotely removed indexes.
     */
    public int getRemoteRemovedIndexes() {
      return this.numBucketIndexRemoved;
    }

    /**
     * Returns the total number of remote buckets.
     */
    public int getTotalRemoteBuckets() {
      return this.numTotalRemoteBuckets;
    }

  }// RemoveIndexResponse

  /**
   * Class representing remove index results on pr.
   *
   */
  public static class RemoveIndexesResult {

    /**
     * Constructor.
     *
     * @param numBucketIndexRemoved number of total bucket indexes removed.
     *
     */
    public RemoveIndexesResult(int numBucketIndexRemoved) {

      // this.numBucketIndexRemoved = numBucketIndexRemoved;
    }

  } // RemoveIndexesResult



  /**
   * Class for index creation reply. This class has the information about sucessful or unsucessful
   * index creation.
   *
   */
  public static class RemoveIndexesReplyMessage extends ReplyMessage {

    /** Indexes removed or not. */
    private boolean result;

    /**
     * Number of buckets locally remove indexes
     */
    private int numBucketsIndexesRemoved;

    /** Number of total bukets in this vm. */
    private int numTotalBuckets;

    /**
     * Default constructor.
     *
     */
    public RemoveIndexesReplyMessage() {

    }


    /**
     * Constructor for index creation reply message.
     *
     * @param processorId processor id of the waiting processor
     * @param ex any exceptions
     * @param result ture if indexes removed properly else false
     * @param numBucketsIndexesRemoved number of buckets indexed.
     * @param numTotalBuckets number of total buckets.
     */
    RemoveIndexesReplyMessage(int processorId, ReplyException ex, boolean result,
        int numBucketsIndexesRemoved, int numTotalBuckets) {
      super();
      super.setException(ex);
      this.result = result;
      this.numBucketsIndexesRemoved = numBucketsIndexesRemoved;
      this.numTotalBuckets = numTotalBuckets;
      setProcessorId(processorId);
    }

    @Override
    public int getDSFID() {
      return PR_REMOVE_INDEXES_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.result = in.readBoolean();
      this.numBucketsIndexesRemoved = in.readInt();
      this.numTotalBuckets = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(this.result);
      out.writeInt(this.numBucketsIndexesRemoved);
      out.writeInt(this.numTotalBuckets);
    }



    /**
     * Actual method sending the index creation reply message.
     *
     * @param recipient the originator of index creation message
     * @param processorId waiting processor id
     * @param dm distribution manager
     * @param ex any exceptions
     * @param result true is indexes removed sucessfully
     * @param numBucketsIndexesRemoved number of buckets indexed
     * @param numTotalBuckets total number of buckets
     */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, ReplyException ex, boolean result, int numBucketsIndexesRemoved,
        int numTotalBuckets) {
      RemoveIndexesReplyMessage rmIndMsg = new RemoveIndexesReplyMessage(processorId, ex, result,
          numBucketsIndexesRemoved, numTotalBuckets);
      rmIndMsg.setRecipient(recipient);
      dm.putOutgoing(rmIndMsg);
    }

    /**
     * Processes this RemoveIndexesReplyMessge on the receiver.
     *
     * @param dm distribution manager
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 p) {
      RemoveIndexesResponse processor = (RemoveIndexesResponse) p;
      if (processor != null) {
        processor.setResponse(this.result, this.numBucketsIndexesRemoved, this.numTotalBuckets);
        processor.process(this);
      }
    }

  } // RemvoeIndexReplyMessage

}
