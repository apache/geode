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
package org.apache.geode.internal.cache.tx;

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
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.ObjectIntProcedure;

public class RemoteFetchKeysMessage extends RemoteOperationMessage {

  private static final Logger logger = LogService.getLogger();

  public RemoteFetchKeysMessage() {}

  private RemoteFetchKeysMessage(InternalDistributedMember recipient, String regionPath,
      ReplyProcessor21 processor) {
    super(recipient, regionPath, processor);
  }

  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws RemoteOperationException {
    if (!(r instanceof PartitionedRegion)) { // prs already wait on initialization
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }
    Set<?> keys = r.keySet();
    try {
      RemoteFetchKeysReplyMessage.send(getSender(), processorId, dm, keys);
    } catch (IOException io) {
      if (logger.isDebugEnabled()) {
        logger.debug("Caught exception while sending keys: {}", io.getMessage(), io);
        throw new RemoteOperationException(
            LocalizedStrings.FetchKeysMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCH_KEYS_REQUEST
                .toLocalizedString(),
            io);
      }
    }
    return false;
  }

  public int getDSFID() {
    return R_FETCH_KEYS_MESSAGE;
  }

  /**
   * @return the response
   */
  public static FetchKeysResponse send(LocalRegion currRegion, DistributedMember target) {
    FetchKeysResponse response =
        new FetchKeysResponse(currRegion.getSystem(), (InternalDistributedMember) target);
    RemoteFetchKeysMessage msg = new RemoteFetchKeysMessage((InternalDistributedMember) target,
        currRegion.getFullPath(), response);
    currRegion.getSystem().getDistributionManager().putOutgoing(msg);
    return response;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  public static class RemoteFetchKeysReplyMessage extends ReplyMessage {
    /** The number of the series */
    int seriesNum;
    /** The message number in the series */
    int msgNum;
    /** The total number of series */
    int numSeries;
    /** Whether this is the last of a series */
    boolean lastInSeries;
    /** the stream holding the chunk to send */
    transient HeapDataOutputStream chunkStream;
    /** the array holding data received */
    transient byte[] chunk;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoteFetchKeysReplyMessage() {}

    private RemoteFetchKeysReplyMessage(InternalDistributedMember recipient, int processorId,
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
     * @throws IOException if the peer is no longer available
     */
    public static void send(final InternalDistributedMember recipient, final int processorId,
        final DistributionManager dm, Set<?> keys) throws IOException {

      Assert.assertTrue(recipient != null, "FetchKeysReplyMessage NULL reply message");

      final int numSeries = 1;
      final int seriesNum = 0;

      // chunkEntries returns false if didn't finish
      if (logger.isDebugEnabled()) {
        logger.debug("Starting region keys chunking for {} keys to member {}", keys.size(),
            recipient);
      }
      boolean finished = chunkSet(recipient, keys, InitialImageOperation.CHUNK_SIZE_IN_BYTES, false,
          new ObjectIntProcedure() {
            int msgNum = 0;

            boolean last = false;

            /**
             * @param a byte[] chunk
             * @param b positive if last chunk
             * @return true to continue to next chunk
             */
            public boolean executeWith(Object a, int b) {
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
        logger.debug("{} region keys chunking", (finished ? "Finished" : "DID NOT complete"));
      }
    }


    static boolean sendChunk(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, HeapDataOutputStream chunk, int seriesNum, int msgNum,
        int numSeries, boolean lastInSeries) {
      RemoteFetchKeysReplyMessage reply = new RemoteFetchKeysReplyMessage(recipient, processorId,
          chunk, seriesNum, msgNum, numSeries, lastInSeries);
      Set<?> failures = dm.putOutgoing(reply);
      return (failures == null) || (failures.size() == 0);
    }

    /**
     * Serialize the given set's elements into byte[] chunks, calling proc for each one. proc args:
     * the byte[] chunk and an int indicating whether it is the last chunk (positive means last
     * chunk, zero otherwise). The return value of proc indicates whether to continue to the next
     * chunk (true) or abort (false).
     *
     * @return true if finished all chunks, false if stopped early
     */
    static boolean chunkSet(InternalDistributedMember recipient, Set<?> set,
        int CHUNK_SIZE_IN_BYTES, boolean includeValues, ObjectIntProcedure proc)
        throws IOException {
      @SuppressWarnings("rawtypes")
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

      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} Remote-processed {}", processor, this);
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
      return R_FETCH_KEYS_REPLY;
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
      sb.append("RemoteFetchKeysReplyMessage ").append("processorid=").append(this.processorId);
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


  public static class FetchKeysResponse extends ReplyProcessor21 {

    private final Set<Object> returnValue;

    /** lock used to synchronize chunk processing */
    private final Object endLock = new Object();
    /** number of chunks processed */
    private volatile int chunksProcessed;

    /** chunks expected (set when last chunk has been processed */
    private volatile int chunksExpected;

    /** whether the last chunk has been processed */
    private volatile boolean lastChunkReceived;


    public FetchKeysResponse(InternalDistributedSystem system, InternalDistributedMember member) {
      super(system, member);
      returnValue = new HashSet<>();
    }

    @Override
    public void process(DistributionMessage msg) {
      boolean doneProcessing = false;
      try {
        if (msg instanceof RemoteFetchKeysReplyMessage) {
          RemoteFetchKeysReplyMessage fkrm = (RemoteFetchKeysReplyMessage) msg;
          if (fkrm.getException() != null) {
            doneProcessing = true;
          } else {
            doneProcessing = processChunk((RemoteFetchKeysReplyMessage) msg);
          }
        } else {
          doneProcessing = true;
        }
      } finally {
        if (doneProcessing) {
          super.process(msg);
        }
      }
    }

    /**
     * @return true if done processing
     */
    boolean processChunk(RemoteFetchKeysReplyMessage msg) {
      // this processing algorithm won't work well if there are multiple recipients. currently the
      // retry logic for failed recipients is in PartitionedRegion. If we parallelize the sending
      // of this message, we'll need to handle fail over in this processor class and track results
      // differently.

      boolean doneProcessing = false;
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
            LocalizedStrings.FetchKeysMessage_ERROR_DESERIALIZING_KEYS.toLocalizedString(), e));
      }
      return doneProcessing;
    }

    @SuppressWarnings("rawtypes")
    public Set waitForKeys() {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        if (e.getCause() instanceof RemoteOperationException) {
          if (e.getCause().getCause() instanceof CancelException) {
            throw new TransactionDataNodeHasDepartedException("Node departed while fetching keys");
          }
        }
        e.handleCause();
        if (!this.lastChunkReceived) {
          throw new TransactionException(e);
        }
      }
      return Collections.unmodifiableSet(this.returnValue);
    }
  }
}
