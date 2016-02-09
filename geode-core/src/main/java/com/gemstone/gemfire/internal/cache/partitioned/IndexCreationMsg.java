/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexCreationException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.MultiIndexCreationException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.index.IndexCreationData;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

public class IndexCreationMsg extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  HashSet<IndexCreationData> indexDefinitions;

  /**
   * Constructor for the index creation message to be sent over the wire
   * with all the relevant information.
   * 
   * @param recipients
   *          members to which this message has to be sent
   * @param regionId
   *          partitioned region id
   * @param processor
   *          The processor to reply to
   * @param indexDefinitions
   *          definitions for the indexes
   *
   */

  IndexCreationMsg(Set recipients, int regionId,
      ReplyProcessor21 processor, HashSet<IndexCreationData> indexDefinitions) {
    super(recipients, regionId, processor);
    this.indexDefinitions = indexDefinitions;
  }

  /**
   * Empty default constructor.
   * 
   */
  public IndexCreationMsg() {

  }

  /**
   * This message may be sent to nodes before the PartitionedRegion is
   * completely initialized due to the RegionAdvisor(s) knowing about the
   * existance of a partitioned region at a very early part of the
   * initialization
   */
  @Override
  protected final boolean failIfRegionMissing() {
    return false;
  }

  /**
   * This method actually operates on the partitioned region and creates given
   * list of indexes from a index creation message.
   * 
   * @param dm
   *          distribution manager.
   * @param pr
   *          partitioned region on which to create an index.
   * @throws CacheException
   *           indicating a cache level error
   * @throws ForceReattemptException
   *           if the peer is no longer available
   */
  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException,
      ForceReattemptException {
    // region exists
    ReplyException replyEx = null;
    boolean result = false;
    List<Index> indexes = null;
    List<String> failedIndexNames = new ArrayList<String>();

    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (IndexCreationData icd : indexDefinitions) {
        sb.append(icd.getIndexName()).append(" ");
      }
      logger.debug("Processing index creation message on this remote partitioned region vm for indexes: {}", sb);
    }

    try {
      indexes = pr.createIndexes(true, indexDefinitions);
    } catch (IndexCreationException e1) {
      replyEx = new ReplyException(
          LocalizedStrings.IndexCreationMsg_REMOTE_INDEX_CREAION_FAILED
              .toLocalizedString(),
          e1);
    } catch (MultiIndexCreationException exx) {
      failedIndexNames.addAll(exx.getExceptionsMap().keySet());
      
      if (logger.isDebugEnabled()) {
        StringBuffer exceptionMsgs = new StringBuffer();
        for(Exception ex: exx.getExceptionsMap().values()) {
          exceptionMsgs.append(ex.getMessage()).append("\n");
        }
        logger.debug("Got an MultiIndexCreationException with \n: {}", exceptionMsgs);
        logger.debug("{} indexes were created succesfully", failedIndexNames.size());
      }
      replyEx = new ReplyException(
          LocalizedStrings.IndexCreationMsg_REMOTE_INDEX_CREAION_FAILED
              .toLocalizedString(),
          exx);
    }

    if (null == replyEx) {
      result = true;
    }

    if (result) {
      Map<String, Integer> indexBucketsMap = new HashMap<String, Integer>();
      for(Index index : indexes) {
        PartitionedIndex prIndex = (PartitionedIndex)index;
        indexBucketsMap.put(prIndex.getName(), prIndex.getNumberOfIndexedBuckets());
      }
      sendReply(getSender(), getProcessorId(), dm, replyEx, result, indexBucketsMap, pr.getDataStore().getAllLocalBuckets().size());
    } else {
      // add the indexes that were successfully created to the map
      Map<String, Integer> indexBucketsMap = new HashMap<String, Integer>();
      for (IndexCreationData icd : indexDefinitions) {
        // if the index was successfully created
        if(!failedIndexNames.contains(icd.getIndexName())) {
          PartitionedIndex prIndex = (PartitionedIndex)pr.getIndex(icd.getIndexName());
          indexBucketsMap.put(icd.getIndexName(), prIndex.getNumberOfIndexedBuckets());
        }
      }
      sendReply(getSender(), getProcessorId(), dm, replyEx, result, indexBucketsMap, pr.getDataStore().getAllLocalBuckets().size());
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Multi Index creation completed on remote host and has sent the reply to the originating vm.");
    }
    return false;
  }

  /**
   * Process this index creation message on the receiver.
   */
  @Override
  public final void process(final DistributionManager dm) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    Throwable thr = null;
    boolean sendReply = true;
    PartitionedRegion pr = null;

    try {
      if (isDebugEnabled) {
        logger.debug("Trying to get pr with id: {}", this.regionId);
      }
      try {
        if (isDebugEnabled) {
          logger.debug("Again trying to get pr with id : {}", this.regionId);
        }
        pr = PartitionedRegion.getPRFromId(this.regionId);
        if (isDebugEnabled) {
          logger.debug("Index creation message got the pr {}", pr);
        }
        if (null == pr) {
          boolean wait = true;
          int attempts = 0;
          while (wait && attempts < 30) { // max 30 seconds of wait.
            dm.getCancelCriterion().checkCancelInProgress(null);
            if (isDebugEnabled) {
              logger.debug("Waiting for Partitioned Region to be intialized with id {}for processing index creation messages",
                  this.regionId);
            }
            try {
              boolean interrupted = Thread.interrupted();
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                interrupted = true;
                dm.getCancelCriterion().checkCancelInProgress(e);
              } finally {
                if (interrupted)
                  Thread.currentThread().interrupt();
              }

              pr = PartitionedRegion.getPRFromId(this.regionId);
              if (null != pr) {
                wait = false;
                if (isDebugEnabled) {
                  logger.debug("Indexcreation message got the pr {}", pr);
                }
              }
              attempts++;
            } catch (CancelException ignorAndLoopWait) {
              if (isDebugEnabled) {
                logger.debug("IndexCreationMsg waiting for pr to be properly created with prId : {}", this.regionId);
              }
            }
          }

        }
      } catch (CancelException letPRInitialized) {
        // Not sure if the CacheClosedException is still thrown in response
        // to the PR being initialized.
        if (logger.isDebugEnabled()) {
          logger.debug("Waiting for notification from pr being properly created on {}", this.regionId);
        }

        boolean wait = true;
        while (wait) {
          dm.getCancelCriterion().checkCancelInProgress(null);
          try {
            boolean interrupted = Thread.interrupted();
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              interrupted = true;
              dm.getCancelCriterion().checkCancelInProgress(e);
            } finally {
              if (interrupted)
                Thread.currentThread().interrupt();
            }
            pr = PartitionedRegion.getPRFromId(this.regionId);
            wait = false;
            if (logger.isDebugEnabled()) {
              logger.debug("Indexcreation message got the pr {}", pr);
            }
          } catch (CancelException ignorAndLoopWait) {
            if (logger.isDebugEnabled()) {
              logger.debug("IndexCreationMsg waiting for pr to be properly created with prId : {}", this.regionId);
            }
          }
        }

      }

      if (pr == null /* && failIfRegionMissing() */) {
        String msg = LocalizedStrings.IndexCreationMsg_COULD_NOT_GET_PARTITIONED_REGION_FROM_ID_0_FOR_MESSAGE_1_RECEIVED_ON_MEMBER_2_MAP_3
            .toLocalizedString(new Object[] { Integer.valueOf(this.regionId),
                this, dm.getId(), PartitionedRegion.dumpPRId() });
        throw new PartitionedRegionException(msg, new RegionNotFoundException(
            msg));
      }
      sendReply = operateOnPartitionedRegion(dm, pr, 0);

    } catch (PRLocallyDestroyedException pre) {
      if (isDebugEnabled) {
        logger.debug("Region is locally Destroyed ");
      }
      thr = pre;
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
        logger.debug("{} exception while processing message:{}", this, t.getMessage(), t);
      } else if (logger.isDebugEnabled(LogMarker.DM) && (t instanceof RuntimeException)) {
        logger.debug(LogMarker.DM, "Exception caught while processing message: {}", t.getMessage(), t);
      }
      if (t instanceof RegionDestroyedException && pr != null) {
        if (pr.isClosed) {
          logger.info(LocalizedMessage.create(LocalizedStrings.IndexCreationMsg_REGION_IS_LOCALLY_DESTROYED_THROWING_REGIONDESTROYEDEXCEPTION_FOR__0, pr));
          thr = new RegionDestroyedException(
              LocalizedStrings.IndexCreationMsg_REGION_IS_LOCALLY_DESTROYED_ON_0
                  .toLocalizedString(dm.getId()), pr.getFullPath());
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
   * Methods that sends the actual index creation message to all the members.
   * 
   * @param recipient
   *          set of members.
   * @param pr
   *          partitoned region associated with the index.
   * @param indexDefinitions
   *          set of index definitions
   * @return partitionresponse a response for the index creation
   */
  public static PartitionResponse send(InternalDistributedMember recipient,
      PartitionedRegion pr, HashSet<IndexCreationData> indexDefinitions) {

    RegionAdvisor advisor = (RegionAdvisor) (pr.getDistributionAdvisor());
    final Set<InternalDistributedMember> recipients;
    /*
     * Will only send create index to all the members storing data
     */
    if (null == recipient) {
      recipients = new HashSet(advisor.adviseDataStore());
    } else {
      recipients = new HashSet<InternalDistributedMember>();
      recipients.add(recipient);
    }
    
    for (InternalDistributedMember rec : recipients) {
      if (rec.getVersionObject().compareTo(Version.GFE_81) < 0) {
        throw new UnsupportedOperationException("Indexes should not be created during rolling upgrade");
      }
    }
    
    IndexCreationResponse processor = null;
    if (logger.isDebugEnabled()) {
      logger.debug("Will be sending create index msg to : {}", recipients.toString());
    }
    if (recipients.size() > 0) {
      processor = (IndexCreationResponse) (new IndexCreationMsg())
          .createReplyProcessor(pr, recipients);
    }

    IndexCreationMsg indMsg = new IndexCreationMsg(recipients,
        pr.getPRId(), processor, indexDefinitions);
    if (logger.isDebugEnabled()) {
      logger.debug("Sending index creation message: {}, to member(s) {}.", indMsg, recipients);
    }
    /* Set failures = */pr.getDistributionManager().putOutgoing(indMsg);
    // Set failures =r.getDistributionManager().putOutgoing(m);
    // if (failures != null && failures.size() > 0) {
    // throw new ForceReattemptException("Failed sending <" + indMsg + ">");
    // }
    return processor;
  }

  // override reply processor type from PartitionMessage
  @Override
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients) {
    return new IndexCreationResponse(r.getSystem(), recipients);
  }

  /**
   * Send a reply for index creation message.
   * 
   * @param member
   *          representing the actual index creatro in the system
   * @param procId
   *          waiting processor
   * @param dm
   *          distribution manager to send the message
   * @param ex
   *          any exceptions
   * @param result
   *          represents index created properly or not.
   * @param indexBucketsMap
   *          Map of indexes created and number of buckets indexed
   * @param numTotalBuckets
   *          Number of total buckets in this vm
   */
  void sendReply(InternalDistributedMember member, int procId, DM dm,
      ReplyException ex, boolean result, Map<String, Integer> indexBucketsMap, int numTotalBuckets) {
    IndexCreationReplyMsg.send(member, procId, dm, ex, result,
        indexBucketsMap, numTotalBuckets);
  }

  public int getDSFID() {
    return PR_INDEX_CREATION_MSG;
  }

  @Override
  public final void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.indexDefinitions = DataSerializer.readHashSet(in);
  }
  
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
  
  @Override
  public final void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeHashSet(this.indexDefinitions, out);
  }

  /**
   * String representation of this message.
   */
  @Override
  public final String toString() {
    StringBuffer sb = new StringBuffer();
    for (IndexCreationData icd : indexDefinitions) {
      sb.append(icd.getIndexName()).append(" ");
    }
    return sb.toString();
  }

  /**
   * Class representing index creation response. This class has all the
   * information for successful or unsuccessful index creation on this member of
   * the partitioned region.
   * 
   * 
   */
  public static class IndexCreationResponse extends PartitionResponse {

    /** Map of indexes created and number of buckets indexed. */
    private Map<String, Integer> indexBucketsMap;

    /** Number of total bukets in this vm. */
    private int numTotalBuckets;

    /**
     * Construtor for index creation response message.
     * 
     * @param ds
     *          distributed system for this member.
     * @param recipients
     *          all the member associated with the index
     */
    IndexCreationResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    /**
     * Waits for the response from the members for index creation.
     * 
     * @return IndexCreationResult for creation of  indexes
     * @throws CacheException
     *           indicating a cache level error
     * @throws ForceReattemptException
     *           if the peer is no longer available
     */
    public IndexCreationResult waitForResult() throws CacheException,
        ForceReattemptException {
      try {
        waitForCacheException();
      } catch (RuntimeException re) {
        if (re instanceof PartitionedRegionException) {
          if (re.getCause() instanceof RegionNotFoundException) {
            // Region may not be available at the receiver.
            // ignore the exception.
            // This will happen when the region on the remote end is still in
            // initialization mode and is not yet created the region ID.
          } else {
            throw re;
          }
        } else {
          throw re;
        }
      }
      return new IndexCreationResult(this.indexBucketsMap, this.numTotalBuckets);
    }

    /**
     * Sets the relevant information in the response.
     * 
     * @param result
     *          true if index created properly
     * @param indexBucketsMap
     *          Map of indexes created and number of buckets indexed
     * @param numTotalBuckets
     *          Number of total buckets in this vm
     */
    public void setResponse(boolean result, Map<String, Integer> indexBucketsMap, int numTotalBuckets) {
      this.indexBucketsMap = indexBucketsMap;
      this.numTotalBuckets = numTotalBuckets;
    }
  }

  /**
   * Class representing index creation result.
   * 
   * 
   */
  public static class IndexCreationResult {
    /** Map of indexes created and number of buckets indexed. */
    private Map<String, Integer> indexBucketsMap;

    /** Number of total bukets in this vm. */
    private int numTotalBuckets;

    /**
     * Constructor for index creation result.
     * 
     * @param indexBucketsMap
     *          Map of indexes created and number of buckets indexed
     * @param numTotalBuckets
     *          Number of total buckets in this vm
     */
    IndexCreationResult(Map<String, Integer> indexBucketsMap, int numTotalBuckets) {
      this.indexBucketsMap = indexBucketsMap;
      this.numTotalBuckets = numTotalBuckets;
    }

    /**
     * Returns a map of index names and number of buckets indexed
     * 
     * @return indexBucketsMap
     */
    public Map<String, Integer> getIndexBucketsMap() {
      return this.indexBucketsMap;

    }

  }

  /**
   * Class for index creation reply. This class has the information about
   * successful index creation.
   * 
   * 
   */
  public static final class IndexCreationReplyMsg extends ReplyMessage {

    /** Index created or not. */
    private boolean result;

    /** Map of indexes created and number of buckets indexed. */
    private Map<String, Integer> indexBucketsMap;

    /** Number of total buckets in this vm. */
    private int numTotalBuckets;

    /** Boolean indicating weather its a data store. */
    private boolean isDataStore;

    public IndexCreationReplyMsg() {

    }

    /**
     * Constructor for index creation reply message.
     * 
     * @param processorId
     *          processor id of the waiting processor
     * @param ex
     *          any exceptions
     * @param result
     *          true if index created properly else false
     * @param indexBucketsMap
     *          Map of indexes created and number of buckets indexed
     * @param numTotalBuckets
     *          Number of total buckets in this vm
     */
    IndexCreationReplyMsg(int processorId, ReplyException ex,
        boolean result, boolean isDataStore, Map<String, Integer> indexBucketsMap, int numTotalBuckets) {
      super();
      super.setException(ex);
      this.result = result;
      this.indexBucketsMap = indexBucketsMap;
      this.numTotalBuckets = numTotalBuckets;
      this.isDataStore = isDataStore;
      setProcessorId(processorId);
    }

    @Override
    public int getDSFID() {
      return PR_INDEX_CREATION_REPLY_MSG;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.result = in.readBoolean();
      this.indexBucketsMap = DataSerializer.readObject(in);
      this.numTotalBuckets = in.readInt();
      this.isDataStore = in.readBoolean();

    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.result);
      DataSerializer.writeObject(this.indexBucketsMap, out);
      out.writeInt(this.numTotalBuckets);
      out.writeBoolean(this.isDataStore);
    }

    /**
     * Actual method sending the index creation reply message.
     * 
     * @param recipient
     *          the originator of index creation message
     * @param processorId
     *          waiting processor id
     * @param dm
     *          distribution manager
     * @param ex
     *          any exceptions
     * @param result
     *          true is index created successfully
     * @param indexBucketsMap
     *          Map of indexes created and number of buckets indexed
     * @param numTotalBuckets
     *          Number of total buckets in this vm
     */
    public static void send(InternalDistributedMember recipient,
        int processorId, DM dm, ReplyException ex, boolean result,
        Map<String, Integer> indexBucketsMap, int numTotalBuckets) {
      IndexCreationReplyMsg indMsg = new IndexCreationReplyMsg(
          processorId, ex, result, result, indexBucketsMap, numTotalBuckets);
      indMsg.setRecipient(recipient);
      dm.putOutgoing(indMsg);
    }

    /**
     * Processes the index creation result.
     * 
     * @param dm
     *          distribution manager
     */
    @Override
    public final void process(final DM dm, final ReplyProcessor21 p) {
      if (logger.isDebugEnabled()) {
        logger.debug("Processor id is : {}", this.processorId);
      }
      IndexCreationResponse processor = (IndexCreationResponse) p;
      if (processor != null) {
        processor.setResponse(this.result, this.indexBucketsMap, this.numTotalBuckets);
        processor.process(this);
      }
    }

  }
}
