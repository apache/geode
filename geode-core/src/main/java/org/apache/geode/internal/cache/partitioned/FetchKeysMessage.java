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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.ObjectIntProcedure;


public class FetchKeysMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  private Integer bucketId;

  /**
   * the interest policy to use in processing the keys
   */
  private int interestType;

  /**
   * the argument for the interest type (regex string, className, list of keys)
   */
  private Object interestArg;

  private boolean allowTombstones;

  private FetchKeysMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, Integer bucketId, int itype, Object interestArg,
      boolean allowTombstones) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
    this.interestType = itype;
    this.interestArg = interestArg;
    this.allowTombstones = allowTombstones;
  }

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public FetchKeysMessage() {}

  /**
   * Sends a PartitionedRegion message to fetch keys for a bucketId
   *
   * @param recipient the member that the fetch keys message is sent to
   * @param r the PartitionedRegion that contains the bucket
   * @param bucketId the identity of the bucket that contains the keys to be returned
   * @param allowTombstones whether to include destroyed entries in the result
   * @return the processor used to read the returned keys
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static FetchKeysResponse send(InternalDistributedMember recipient, PartitionedRegion r,
      Integer bucketId, boolean allowTombstones) throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "FetchKeysMessage NULL recipient");
    TXManagerImpl txManager = r.getCache().getTxManager();
    boolean resetTxState = isTransactionInternalSuspendNeeded(txManager);
    TXStateProxy txStateProxy = null;
    if (resetTxState) {
      txStateProxy = txManager.pauseTransaction();
    }

    try {
      FetchKeysMessage tmp = new FetchKeysMessage();

      FetchKeysResponse p =
          (FetchKeysResponse) tmp.createReplyProcessor(r, Collections.singleton(recipient));
      FetchKeysMessage m = new FetchKeysMessage(recipient, r.getPRId(), p, bucketId,
          InterestType.REGULAR_EXPRESSION, ".*", allowTombstones);
      m.setTransactionDistributed(txManager.isDistributed());

      Set failures = r.getDistributionManager().putOutgoing(m);
      if (failures != null && failures.size() > 0) {
        throw new ForceReattemptException(
            String.format("Failed sending < %s >", m));
      }
      return p;
    } finally {
      if (resetTxState) {
        txManager.unpauseTransaction(txStateProxy);
      }
    }
  }

  private static boolean isTransactionInternalSuspendNeeded(TXManagerImpl txManager) {
    TXStateProxy txState = txManager.getTXState();
    // handle distributed transaction when needed.
    return txState != null && txState.isRealDealLocal() && !txState.isDistTx();
  }

  /**
   * @return the FetchKeysResponse
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static FetchKeysResponse sendInterestQuery(InternalDistributedMember recipient,
      PartitionedRegion r, Integer bucketId, int itype, Object arg, boolean allowTombstones)
      throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "FetchKeysMessage NULL recipient");
    FetchKeysMessage tmp = new FetchKeysMessage();
    FetchKeysResponse p =
        (FetchKeysResponse) tmp.createReplyProcessor(r, Collections.singleton(recipient));
    FetchKeysMessage m =
        new FetchKeysMessage(recipient, r.getPRId(), p, bucketId, itype, arg, allowTombstones);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  // override processor type
  @Override
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients) {
    return new FetchKeysResponse(r.getSystem(), r, recipients);
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws CacheException, ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug("FetchKeysMessage operateOnRegion: {} bucketId: {} type: {} {}", r.getFullPath(),
          this.bucketId, InterestType.getString(interestType),
          (allowTombstones ? " with tombstones" : " without tombstones"));
    }

    PartitionedRegionDataStore ds = r.getDataStore();
    if (ds != null) {
      try {
        Set keys =
            ds.handleRemoteGetKeys(this.bucketId, interestType, interestArg, allowTombstones);
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "FetchKeysMessage sending {} keys back using processorId: : {}", keys.size(),
              getProcessorId(), keys);
        }
        r.getPrStats().endPartitionMessagesProcessing(startTime);
        FetchKeysReplyMessage.send(getSender(), getProcessorId(), dm, keys);
      } catch (PRLocallyDestroyedException pde) {
        if (logger.isDebugEnabled()) {
          logger.debug("FetchKeysMessage Encountered PRLocallyDestroyedException");
        }
        throw new ForceReattemptException(
            "Encountered PRLocallyDestroyedException",
            pde);
      }
    } else {
      logger.warn("FetchKeysMessage: data store not configured for this member");
    }

    // Unless there was an exception thrown, this message handles sending the response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
  }

  public int getDSFID() {
    return PR_FETCH_KEYS_MESSAGE;
  }

  /**
   * Versions in which on-wire form has changed, requiring new toData/fromData methods
   */
  public Version[] serializationVersions = null;

  public Version[] getSerializationVersions() {
    return serializationVersions;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = Integer.valueOf(in.readInt());
    this.interestType = in.readInt();
    this.interestArg = DataSerializer.readObject(in);
    this.allowTombstones = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.bucketId.intValue());
    out.writeInt(interestType);
    DataSerializer.writeObject(interestArg, out);
    out.writeBoolean(this.allowTombstones);
  }

  public static class FetchKeysReplyMessage extends ReplyMessage {
    /**
     * The number of the series
     */
    int seriesNum;
    /**
     * The message number in the series
     */
    int msgNum;
    /**
     * The total number of series
     */
    int numSeries;
    /**
     * Whether this is the last of a series
     */
    boolean lastInSeries;
    /**
     * the stream holding the chunk to send
     */
    transient HeapDataOutputStream chunkStream;
    /**
     * the array holding data received
     */
    transient byte[] chunk;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public FetchKeysReplyMessage() {}

    private FetchKeysReplyMessage(InternalDistributedMember recipient, int processorId,
        HeapDataOutputStream chunk, int seriesNum, int msgNum, int numSeries,
        boolean lastInSeries) {
      super();
      setRecipient(recipient);
      setProcessorId(processorId);
      this.seriesNum = seriesNum;
      this.msgNum = msgNum;
      this.numSeries = numSeries;
      this.lastInSeries = lastInSeries;
      this.chunkStream = chunk;
    }

    /**
     * Send an ack
     *
     * @throws ForceReattemptException if the peer is no longer available
     */
    public static void send(final InternalDistributedMember recipient, final int processorId,
        final DistributionManager dm, Set keys) throws ForceReattemptException {

      Assert.assertTrue(recipient != null, "FetchKeysReplyMessage NULL reply message");

      final int numSeries = 1;
      final int seriesNum = 0;

      // chunkEntries returns false if didn't finish
      if (logger.isDebugEnabled()) {
        logger.debug("Starting pr keys chunking for {} kets to member {}", keys.size(), recipient);
      }
      try {
        boolean finished = chunkSet(recipient, keys, InitialImageOperation.CHUNK_SIZE_IN_BYTES,
            false, new ObjectIntProcedure() {
              int msgNum = 0;

              boolean last = false;

              /**
               * @param a byte[] chunk
               * @param b positive if last chunk
               * @return true to continue to next chunk
               */
              public boolean executeWith(Object a, int b) {
                // if (this.last)
                // throw new
                // InternalGemFireError(LocalizedStrings.FetchKeysMessage_ALREADY_PROCESSED_LAST_CHUNK));
                HeapDataOutputStream chunk = (HeapDataOutputStream) a;
                this.last = b > 0;
                try {
                  boolean okay = sendChunk(recipient, processorId, dm, chunk, seriesNum, msgNum++,
                      numSeries, this.last);
                  return okay;
                } catch (CancelException e) {
                  return false;
                }
              }
            });

        if (logger.isDebugEnabled()) {
          logger.debug("{} pr keys chunking", (finished ? "Finished" : "DID NOT complete"));
        }
      } catch (IOException io) {
        throw new ForceReattemptException(
            "Unable to send response to fetch keys request",
            io);
      }
      // TODO [bruce] pass a reference to the cache or region down here so we can do this test
      // Assert.assertTrue(!cache is closed, "chunking interrupted but cache is still open");
    }


    static boolean sendChunk(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, HeapDataOutputStream chunk, int seriesNum, int msgNum,
        int numSeries, boolean lastInSeries) {
      FetchKeysReplyMessage reply = new FetchKeysReplyMessage(recipient, processorId, chunk,
          seriesNum, msgNum, numSeries, lastInSeries);
      Set failures = dm.putOutgoing(reply);
      return (failures == null) || (failures.size() == 0);
    }

    /**
     * Serialize the given set's elments into byte[] chunks, calling proc for each one. proc args:
     * the byte[] chunk and an int indicating whether it is the last chunk (positive means last
     * chunk, zero othewise). The return value of proc indicates whether to continue to the next
     * chunk (true) or abort (false).
     *
     * @return true if finished all chunks, false if stopped early
     */
    static boolean chunkSet(InternalDistributedMember recipient, Set set, int CHUNK_SIZE_IN_BYTES,
        boolean includeValues, ObjectIntProcedure proc) throws IOException {
      Iterator it = set.iterator();

      boolean keepGoing = true;
      boolean sentLastChunk = false;

      // always write at least one chunk
      final HeapDataOutputStream mos = new HeapDataOutputStream(
          InitialImageOperation.CHUNK_SIZE_IN_BYTES + 2048, recipient.getVersionObject());
      do {
        mos.reset();

        int avgItemSize = 0;
        int itemCount = 0;

        while ((mos.size() + avgItemSize) < InitialImageOperation.CHUNK_SIZE_IN_BYTES
            && it.hasNext()) {
          Object key = it.next();
          DataSerializer.writeObject(key, mos);

          // Note we track the itemCount so we can compute avgItemSize
          itemCount++;
          // Note we track avgItemSize to help us not to always go one item
          // past the max chunk size. When we go past it causes us to grow
          // the ByteBuffer that the chunk is stored in resulting in a copy
          // of the data.
          avgItemSize = mos.size() / itemCount;

        }

        // Write "end of chunk" entry to indicate end of chunk
        DataSerializer.writeObject((Object) null, mos);

        // send 1 for last message if no more data
        int lastMsg = it.hasNext() ? 0 : 1;
        keepGoing = proc.executeWith(mos, lastMsg);
        sentLastChunk = lastMsg == 1 && keepGoing;

        // if this region is destroyed while we are sending data, then abort.
      } while (keepGoing && it.hasNext());

      // return false if we were told to abort
      return sentLastChunk;
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 p) {
      final long startTime = getTimestamp();
      FetchKeysResponse processor = (FetchKeysResponse) p;

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "FetchKeysReplyMessage processor not found");
        }
        return;
      }

      processor.processChunk(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", processor, this);
      }

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.seriesNum);
      out.writeInt(this.msgNum);
      out.writeInt(this.numSeries);
      out.writeBoolean(this.lastInSeries);
      DataSerializer.writeObjectAsByteArray(this.chunkStream, out);
    }

    @Override
    public int getDSFID() {
      return PR_FETCH_KEYS_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.seriesNum = in.readInt();
      this.msgNum = in.readInt();
      this.numSeries = in.readInt();
      this.lastInSeries = in.readBoolean();
      this.chunk = DataSerializer.readByteArray(in);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("FetchKeysReplyMessage ").append("processorid=").append(this.processorId);
      if (getSender() != null) {
        sb.append(",sender=").append(this.getSender());
      }
      sb.append(",seriesNum=").append(seriesNum).append(",msgNum=").append(msgNum)
          .append(",numSeries=").append(numSeries).append(",lastInSeries=").append(lastInSeries);
      if (chunkStream != null) {
        sb.append(",size=").append(chunkStream.size());
      } else if (chunk != null) {
        sb.append(",size=").append(chunk.length);
      }
      if (getException() != null) {
        sb.append(", exception=").append(getException());
      }
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.partitioned.GetMessage.GetReplyMessage}
   *
   * @since GemFire 5.0
   */
  public static class FetchKeysResponse extends PartitionResponse {

    private final PartitionedRegion pr;

    private final Set returnValue;

    /**
     * lock used to synchronize chunk processing
     */
    private final Object endLock = new Object();

    /**
     * number of chunks processed
     */
    private volatile int chunksProcessed;

    /**
     * chunks expected (set when last chunk has been processed
     */
    private volatile int chunksExpected;

    /**
     * whether the last chunk has been processed
     */
    private volatile boolean lastChunkReceived;

    public FetchKeysResponse(InternalDistributedSystem ds, PartitionedRegion pr, Set recipients) {
      super(ds, recipients);
      this.pr = pr;
      returnValue = new HashSet();
    }

    void processChunk(FetchKeysReplyMessage msg) {
      // this processing algorighm won't work well if there are multiple recipients. currently the
      // retry logic for failed recipients is in PartitionedRegion. If we parallelize the sending
      // of this message, we'll need to handle failover in this processor class and track results
      // differently.

      boolean doneProcessing = false;

      if (msg.getException() != null) {
        process(msg);
      } else {
        try {
          ByteArrayInputStream byteStream = new ByteArrayInputStream(msg.chunk);
          DataInputStream in = new DataInputStream(byteStream);
          while (in.available() > 0) {
            Object key = DataSerializer.readObject(in);
            if (key != null) {
              synchronized (returnValue) {
                returnValue.add(key);
              }
            } else {
              // null should signal the end of the set of keys
              Assert.assertTrue(in.available() == 0);
            }
          }

          synchronized (this.endLock) {
            chunksProcessed = chunksProcessed + 1;

            if (((msg.seriesNum + 1) == msg.numSeries) && msg.lastInSeries) {
              lastChunkReceived = true;
              chunksExpected = msg.msgNum + 1;
            }

            if (lastChunkReceived && (chunksExpected == chunksProcessed)) {
              doneProcessing = true;
            }
            if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
              logger.trace(LogMarker.DM_VERBOSE,
                  "{} chunksProcessed={},lastChunkReceived={},chunksExpected={},done={}", this,
                  chunksProcessed, lastChunkReceived, chunksExpected, doneProcessing);
            }
          }
        } catch (Exception e) {
          processException(new ReplyException(
              "Error deserializing keys", e));
          checkIfDone(); // fix for hang in 41202
        }

        // if all chunks have been received, wake up the waiting thread
        if (doneProcessing) {
          process(msg);
        }
      }
    }

    /**
     * @return Set the keys associated with the bucketid of the {@link FetchKeysMessage}
     * @throws ForceReattemptException if the peer is no longer available
     */
    public Set waitForKeys() throws ForceReattemptException {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          logger.debug("FetchKeysResponse got remote CacheClosedException; forcing reattempt. {}",
              t.getMessage(), t);
          throw new ForceReattemptException(
              "FetchKeysResponse got remote CacheClosedException; forcing reattempt.",
              t);
        }
        if (t instanceof ForceReattemptException) {
          logger.debug("FetchKeysResponse got remote ForceReattemptException; rethrowing. {}",
              e.getMessage(), e);
          throw new ForceReattemptException(
              "Peer requests reattempt", t);
        }
        e.handleCause();
      }
      if (!this.lastChunkReceived) {
        throw new ForceReattemptException(
            "No replies received");
      }
      return this.returnValue;
    }
  }

}
