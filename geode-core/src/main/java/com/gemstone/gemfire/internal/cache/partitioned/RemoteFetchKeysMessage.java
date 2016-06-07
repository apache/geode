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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.partitioned;

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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RemoteOperationException;
import com.gemstone.gemfire.internal.cache.RemoteOperationMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.ObjectIntProcedure;

/**
 * TODO this class should be moved to a different package
 */
public class RemoteFetchKeysMessage extends RemoteOperationMessage {

  private static final Logger logger = LogService.getLogger();
  
  public RemoteFetchKeysMessage() {
  }

  private RemoteFetchKeysMessage(InternalDistributedMember recipient, String regionPath, ReplyProcessor21 processor) {
    super(recipient, regionPath, processor);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.RemoteOperationMessage#operateOnRegion(com.gemstone.gemfire.distributed.internal.DistributionManager, com.gemstone.gemfire.internal.cache.LocalRegion, long)
   */
  @Override
  protected boolean operateOnRegion(DistributionManager dm, LocalRegion r,
      long startTime) throws RemoteOperationException {
    if ( ! (r instanceof PartitionedRegion) ) { // prs already wait on initialization
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }
    Set keys = r.keySet();
    try {
      RemoteFetchKeysReplyMessage.send(getSender(), processorId, dm, keys);
    } catch (ForceReattemptException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Caught exception while sending keys: {}", e.getMessage(),e);
      }
    }
    return false;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return R_FETCH_KEYS_MESSAGE;
  }

  /**
   * @param currRegion
   * @param target
   * @return the response
   */
  public static FetchKeysResponse send(LocalRegion currRegion, DistributedMember target) {
    FetchKeysResponse response = new FetchKeysResponse(currRegion.getSystem(),
        currRegion, (InternalDistributedMember)target);
    RemoteFetchKeysMessage msg = new RemoteFetchKeysMessage((InternalDistributedMember)target,
            currRegion.getFullPath(), response);
    currRegion.getSystem().getDistributionManager().putOutgoing(msg);
    return response;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.RemoteOperationMessage#toData(java.io.DataOutput)
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.RemoteOperationMessage#fromData(java.io.DataInput)
   */
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
    public RemoteFetchKeysReplyMessage() {
    }
  
    private RemoteFetchKeysReplyMessage(InternalDistributedMember recipient, int processorId, HeapDataOutputStream chunk,
                                  int seriesNum, int msgNum, int numSeries, boolean lastInSeries)
    {
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
     * @throws ForceReattemptException if the peer is no longer available
     */
    public static void send(final InternalDistributedMember recipient, final int processorId,
        final DM dm, Set keys) throws ForceReattemptException {

      Assert.assertTrue(recipient != null, "FetchKeysReplyMessage NULL reply message");
      
      final int numSeries = 1;
      final int seriesNum = 0;
        
      // chunkEntries returns false if didn't finish
      if (logger.isDebugEnabled()) {
        logger.debug("Starting pr keys chunking for {} keys to member {}", keys.size(), recipient);
      }
      try {
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
//              if (this.last)
//                throw new InternalGemFireError(LocalizedStrings.FetchKeysMessage_ALREADY_PROCESSED_LAST_CHUNK.toLocalizedString());
              HeapDataOutputStream chunk = (HeapDataOutputStream)a;
              this.last = b > 0;
              try {
                boolean okay = sendChunk(recipient, processorId, dm, chunk, seriesNum, msgNum++, numSeries, this.last);
                return okay;
              }
              catch (CancelException e) {
                return false;
              }
            }
          });
  
        if (logger.isDebugEnabled()) {
          logger.debug("{} pr keys chunking", (finished?"Finished" : "DID NOT complete"));
        }
      }
      catch (IOException io) {
        throw new ForceReattemptException(LocalizedStrings.FetchKeysMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCH_KEYS_REQUEST.toLocalizedString(), io);
      }
      // TODO [bruce] pass a reference to the cache or region down here so we can do this test
      //Assert.assertTrue(!cache is closed, "chunking interrupted but cache is still open");
    }
    
    
    static boolean sendChunk(InternalDistributedMember recipient, int processorId, DM dm, HeapDataOutputStream chunk,
                                      int seriesNum, int msgNum, int numSeries, boolean lastInSeries) {
      RemoteFetchKeysReplyMessage reply = new RemoteFetchKeysReplyMessage(recipient, processorId, chunk, seriesNum,
                                      msgNum, numSeries, lastInSeries);
      Set failures = dm.putOutgoing(reply);
      return (failures == null) || (failures.size() == 0);
    }
    
    /**
     * Serialize the given set's elments into byte[] chunks, calling proc for each
     * one. proc args: the byte[] chunk and an int indicating whether it
     * is the last chunk (positive means last chunk, zero othewise).
     * The return value of proc indicates whether to continue to the next
     * chunk (true) or abort (false).
     *
     * @return true if finished all chunks, false if stopped early
     */
    static boolean chunkSet(InternalDistributedMember recipient, Set set, int CHUNK_SIZE_IN_BYTES, boolean includeValues,
                      ObjectIntProcedure proc)
    throws IOException
    {
      Iterator it = set.iterator();

      boolean keepGoing = true;
      boolean sentLastChunk = false;

      // always write at least one chunk
      final HeapDataOutputStream mos = new HeapDataOutputStream(
          InitialImageOperation.CHUNK_SIZE_IN_BYTES+2048, recipient.getVersionObject());
      do {
        mos.reset();

        int avgItemSize = 0;
        int itemCount = 0;

        while ((mos.size()+avgItemSize) < InitialImageOperation.CHUNK_SIZE_IN_BYTES && it.hasNext()) {
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
        DataSerializer.writeObject((Object)null, mos);

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
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 p) {
      final long startTime = getTimestamp();
      FetchKeysResponse processor = (FetchKeysResponse)p;
  
      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "FetchKeysReplyMessage processor not found");
        }
        return;
      }

      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} Remote-processed {}", processor, this);
      }

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime()
          - startTime);
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
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
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
      sb.append("RemoteFetchKeysReplyMessage ")
        .append("processorid=").append(this.processorId);
      if (getSender() != null) { 
        sb.append(",sender=").append(this.getSender());
      }
      sb.append(",seriesNum=").append(seriesNum)
        .append(",msgNum=").append(msgNum)
        .append(",numSeries=").append(numSeries)
        .append(",lastInSeries=").append(lastInSeries);
      if (chunkStream != null) {
        sb.append(",size=").append(chunkStream.size());
      }
      else if (chunk != null) {
        sb.append(",size=").append(chunk.length);
      }
      if (getException() != null) {
        sb.append(", exception=").append(getException());
      }
      return sb.toString();
    }

  }
  
  
  public static class FetchKeysResponse extends ReplyProcessor21 {

    private final LocalRegion region;

    private final Set returnValue;

    /** lock used to synchronize chunk processing */
    private final Object endLock = new Object();
    /** number of chunks processed */
    private volatile int chunksProcessed;
    
    /** chunks expected (set when last chunk has been processed */
    private volatile int chunksExpected;
    
    /** whether the last chunk has been processed */
    private volatile boolean lastChunkReceived;
    
    
    public FetchKeysResponse(InternalDistributedSystem system,
        LocalRegion region, InternalDistributedMember member) {
      super(system, member);
      this.region = region;
      returnValue = new HashSet();
    }

    @Override
    public void process(DistributionMessage msg) {
      boolean doneProcessing = false;
      try {
        if (msg instanceof RemoteFetchKeysReplyMessage) {
          RemoteFetchKeysReplyMessage fkrm = (RemoteFetchKeysReplyMessage)msg;
          if (fkrm.getException() != null) {
            doneProcessing = true;
          } else {
            doneProcessing = processChunk((RemoteFetchKeysReplyMessage)msg);
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
     * @param msg
     * @return true if done processing
     */
    boolean processChunk(RemoteFetchKeysReplyMessage msg) {
      // this processing algorighm won't work well if there are multiple recipients.  currently the
      // retry logic for failed recipients is in PartitionedRegion.  If we parallelize the sending
      // of this message, we'll need to handle failover in this processor class and track results
      // differently.

      boolean doneProcessing = false;
      try {
        
        ByteArrayInputStream byteStream = new ByteArrayInputStream(msg.chunk);
        DataInputStream in = new DataInputStream(byteStream);
        while (in.available() > 0) {
          Object key = DataSerializer.readObject(in);
          if (key != null) {
            synchronized(returnValue) {
              returnValue.add(key);
            }
          }
          else {
            // null should signal the end of the set of keys
            Assert.assertTrue(in.available() == 0);
          }
        }

        synchronized(this.endLock) {
          chunksProcessed = chunksProcessed + 1;

          if (((msg.seriesNum+1) == msg.numSeries)  &&  msg.lastInSeries) {
            lastChunkReceived = true;
            chunksExpected = msg.msgNum + 1;
          }

          if (lastChunkReceived  &&  (chunksExpected == chunksProcessed)) {
            doneProcessing = true;
          }
          if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM, "{} chunksProcessed={},lastChunkReceived={},chunksExpected={},done={}",
                this, chunksProcessed, lastChunkReceived, chunksExpected, doneProcessing);
          }
        }
      }
      catch (Exception e) {
        processException(new ReplyException(LocalizedStrings.FetchKeysMessage_ERROR_DESERIALIZING_KEYS.toLocalizedString(), e));
      }
      return doneProcessing;
    }
    
    public Set waitForKeys() {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          logger.debug("RemoteFetchKeysResponse got remote CacheClosedException; forcing reattempt. {}", t.getMessage(), t);
        }
        if (t instanceof ForceReattemptException) {
          logger.debug("RemoteFetchKeysResponse got remote ForceReattemptException; rethrowing. {}", e.getMessage(), e);
        }
        if (t instanceof RegionDestroyedException) {
          RegionDestroyedException rde = (RegionDestroyedException) t;
          throw rde;
        }
        e.handleAsUnexpected();
        if (!this.lastChunkReceived) {
          throw new TransactionException(e);
        }
      }
      return Collections.unmodifiableSet(this.returnValue);
    }
  }
}
