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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
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
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionStamp;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.util.ObjectIntProcedure;

public final class FetchEntriesMessage extends PartitionMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  private int bucketId;
  
  public FetchEntriesMessage() {
  }

  private FetchEntriesMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, int bucketId) {
    super(recipient, regionId, processor);
    this.bucketId = bucketId;
  }

  /**
   * Sends a PartitionedRegion message to fetch all the entries for a bucketId
   * @param recipient the member that the fetch keys message is sent to 
   * @param r  the PartitionedRegion that contains the bucket
   * @param bucketId the identity of the bucket that contains the entries to be returned
   * @return the processor used to read the returned entries
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static FetchEntriesResponse send(InternalDistributedMember recipient, 
      PartitionedRegion r, int bucketId) 
      throws ForceReattemptException
  {
    Assert.assertTrue(recipient != null, "FetchEntriesMessage NULL reply message");
    FetchEntriesResponse p = new FetchEntriesResponse(r.getSystem(), r,
        recipient, bucketId);
    FetchEntriesMessage m = new FetchEntriesMessage(recipient, r.getPRId(), p,
        bucketId);

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_FAILED_SENDING_0.toLocalizedString(m));
    }

    return p;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, PartitionedRegion pr, long startTime)
      throws CacheException, ForceReattemptException
  {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "FetchEntriesMessage operateOnRegion: {}", pr.getFullPath());
    }

    PartitionedRegionDataStore ds = pr.getDataStore();
    BucketRegion entries = null;
    if (ds != null) {
      entries = ds.handleRemoteGetEntries(this.bucketId);
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "FetchKeysMessage send keys back using processorId: {}", getProcessorId());
      }
    }
    else {
      logger.warn(LocalizedMessage.create(LocalizedStrings.FetchEntriesMessage_FETCHKEYSMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER));
    }
    pr.getPrStats().endPartitionMessagesProcessing(startTime);
    FetchEntriesReplyMessage.send(getSender(), getProcessorId(), dm, 
        this.bucketId, entries);
    
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
    buff.append("; recipient=").append(this.getRecipient());
  }

  public int getDSFID() {
    return PR_FETCH_ENTRIES_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeInt(this.bucketId);
  }
  
  public static final class FetchEntriesReplyMessage extends ReplyMessage {
    
    /** The bucket id */
    int bucketId;
  
    /** The number of the series */
    int seriesNum;
    /** The message number in the series */
    int msgNum;
    /** The total number of series */
    int numSeries;
    /** Whether this is the last of a series */
    boolean lastInSeries;
    /** array holding chunk of entries received */
    transient byte[] chunk;
    /** stream holding chunk of entries to send */
    transient HeapDataOutputStream chunkStream;
    
    private boolean hasRVV;
    
    /** The versions in which this message was modified */
    private static final Version[] dsfidVersions = null;

    @Override
    public Version[] getSerializationVersions() {
      return dsfidVersions;
    }

    /**
     * Empty constructor to conform to DataSerializable interface 
     */
    public FetchEntriesReplyMessage() {
    }
  
    private FetchEntriesReplyMessage(InternalDistributedMember dest,
        int processorId, int buckId, HeapDataOutputStream chunk,
        int seriesNum, int msgNum, int numSeries, boolean lastInSeries, boolean hasRVV) {
      setRecipient(dest);
      setProcessorId(processorId);
      this.bucketId = buckId;
      this.seriesNum = seriesNum;
      this.msgNum = msgNum;
      this.numSeries = numSeries;
      this.lastInSeries = lastInSeries;
      this.chunkStream = chunk;
      this.hasRVV = hasRVV;
    }
    
    /** 
     * Send an ack
     * @throws ForceReattemptException if the peer is no longer available
     */
    public static void send(final InternalDistributedMember recipient, final int processorId,
                            final DM dm, final int bucketId, BucketRegion keys)   
        throws ForceReattemptException {

      Assert.assertTrue(recipient != null, "FetchEntriesReplyMessage NULL reply message");
      final int numSeries = 1;
      final int seriesNum = 0;
      
      final RegionVersionVector rvv = keys.getVersionVector();
      if(rvv != null) {
        RegionVersionVector clone = rvv.getCloneForTransmission();
        ReplyMessage.send(recipient, processorId, clone, dm);
      }
        
      // chunkEntries returns false if didn't finish
      if (logger.isDebugEnabled()) {
        logger.debug("Starting PR entries chunking for {} entries", keys.size());
      }
      try {
        boolean finished = chunkMap(recipient, keys, InitialImageOperation.CHUNK_SIZE_IN_BYTES, false,
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
//                throw new InternalGemFireError(LocalizedStrings.FetchEntriesMessage_ALREADY_PROCESSED_LAST_CHUNK.toLocalizedString());
              HeapDataOutputStream chunk = (HeapDataOutputStream)a;
              this.last = b > 0;
              try {
                boolean okay = sendChunk(recipient, processorId, bucketId, dm, chunk, seriesNum, msgNum++, numSeries, this.last, rvv != null);
                return okay;
              }
              catch (CancelException e) {
                return false;
              }
            }
          });
  
        if (logger.isDebugEnabled()) {
          logger.debug("{} PR entries chunking", (finished ? "Finished" : "DID NOT complete"));
        }
      }
      catch (IOException io) {
        // This is a little odd, since we're trying to send a reply.
        // One does not normally force a reply.  Is this correct?
        throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCHENTRIES_REQUEST.toLocalizedString(), io);
      }
    }
      
    static boolean sendChunk(InternalDistributedMember recipient, int processorId, int bucketId,
                             DM dm, HeapDataOutputStream chunk,
                             int seriesNum, int msgNum, int numSeries, boolean lastInSeries, boolean hasRVV) {
      FetchEntriesReplyMessage reply = new FetchEntriesReplyMessage(recipient,
          processorId, bucketId, chunk, seriesNum,
          msgNum, numSeries, lastInSeries, hasRVV);
      Set failures = dm.putOutgoing(reply);
      return (failures == null) || (failures.size() == 0);
    }
    

    /**
     * Serialize the given map's entries into byte[] chunks, calling proc for each
     * one. proc args: the byte[] chunk and an int indicating whether it
     * is the last chunk (positive means last chunk, zero othewise).
     * The return value of proc indicates whether to continue to the next
     * chunk (true) or abort (false).
     *
     * @return true if finished all chunks, false if stopped early
     */
    static boolean chunkMap(InternalDistributedMember receiver, BucketRegion map, int CHUNK_SIZE_IN_BYTES, boolean includeValues,
                      ObjectIntProcedure proc)
    throws IOException
    {
      Iterator it = map.entrySet().iterator();

      boolean keepGoing = true;
      boolean sentLastChunk = false;

      // always write at least one chunk
      final HeapDataOutputStream mos = new HeapDataOutputStream(
          InitialImageOperation.CHUNK_SIZE_IN_BYTES+2048, receiver.getVersionObject());
      do {
        mos.reset();

        int avgItemSize = 0;
        int itemCount = 0;

        while ((mos.size()+avgItemSize) < InitialImageOperation.CHUNK_SIZE_IN_BYTES && it.hasNext()) {

          LocalRegion.NonTXEntry entry = (LocalRegion.NonTXEntry)it.next();
          RegionEntry re = entry.getRegionEntry();
          synchronized(re) {
            // TODO:KIRK:OK Object value = re.getValueInVM(map);
            Object value = re._getValueRetain(map, true);
            try {
            if (value == null) {
              // only possible for disk entry
              value = re.getSerializedValueOnDisk((LocalRegion)entry.getRegion());
            }
            if ( !Token.isRemoved(value) ) {
              DataSerializer.writeObject(re.getKey(), mos);
              if (Token.isInvalid(value)) {
                value = null;
              }
              VersionStamp stamp = re.getVersionStamp();
              VersionTag versionTag = stamp != null ? stamp.asVersionTag() : null;
              if(versionTag != null) {
                versionTag.replaceNullIDs(map.getVersionMember());
              }
              DataSerializer.writeObject(value, mos);
              DataSerializer.writeObject(versionTag, mos);

              // Note we track the itemCount so we can compute avgItemSize
              itemCount++;
              // Note we track avgItemSize to help us not to always go one item
              // past the max chunk size. When we go past it causes us to grow
              // the ByteBuffer that the chunk is stored in resulting in a copy
              // of the data.
              avgItemSize = mos.size() / itemCount;
            }
            } finally {
              OffHeapHelper.release(value);
            }
          }
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
      FetchEntriesResponse processor = (FetchEntriesResponse)p;
  
      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "FetchEntriesReplyMessage processor not found");
        }
        return;
      }

      processor.processChunk(this);
      
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", processor, this);
      }

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime()
          - startTime);
    }
    
   
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.bucketId);
      out.writeInt(this.seriesNum);
      out.writeInt(this.msgNum);
      out.writeInt(this.numSeries);
      out.writeBoolean(this.lastInSeries);
      DataSerializer.writeObjectAsByteArray(this.chunkStream, out);
      out.writeBoolean(this.hasRVV);
    }
    
    @Override
    public int getDSFID() {
      return PR_FETCH_ENTRIES_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.bucketId = in.readInt();
      this.seriesNum = in.readInt();
      this.msgNum = in.readInt();
      this.numSeries = in.readInt();
      this.lastInSeries = in.readBoolean();
      this.chunk = DataSerializer.readByteArray(in);
      hasRVV = in.readBoolean();
    }
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("FetchEntriesReplyMessage ")
        .append("processorid=").append(this.processorId)
        .append(",bucketId=").append(this.bucketId);
      if (getSender() != null) { 
        sb.append(",sender=").append(this.getSender());
      }
      sb.append(",seriesNum=").append(seriesNum)
        .append(",msgNum=").append(msgNum)
        .append(",numSeries=").append(numSeries)
        .append(",lastInSeries=").append(lastInSeries);
      if (chunk != null) {
        sb.append(",size=").append(chunk.length);
      }
      else if (chunkStream != null) {
        sb.append(",size=").append(chunkStream.size());
      }
      if (getException() != null) {
        sb.append(",exception=").append(getException());
      }
      return sb.toString();
    }
  }
 
  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage}
   * @author mthomas
   * @since 5.0
   */
  public static class FetchEntriesResponse extends ReplyProcessor21  {

    private final PartitionedRegion pr;

    private volatile RegionVersionVector returnRVV; 
    private final HashMap<Object, Object> returnValue;
    private final HashMap<Object, VersionTag> returnVersions = new HashMap();
    private final Map<VersionSource,VersionSource> canonicalMembers = new ConcurrentHashMap<VersionSource,VersionSource>();
    
    /** lock used to synchronize chunk processing */
    private final Object endLock = new Object();
    
    /** number of chunks processed */
    private volatile int chunksProcessed;
    
    /** chunks expected (set when last chunk has been processed */
    private volatile int chunksExpected;
    
    /** whether the last chunk has been processed */
    private volatile boolean lastChunkReceived;

    private int bucketId;

    private InternalDistributedMember recipient;
    
    public FetchEntriesResponse(InternalDistributedSystem ds,
        final PartitionedRegion pr, final InternalDistributedMember recipient,
        final int bucketId) {
      super(ds, Collections.singleton(recipient));
      this.pr = pr;
      this.bucketId = bucketId;
      this.recipient = recipient;
      this.returnValue = new HashMap<Object, Object>() {
        private static final long serialVersionUID = 0L;

        @Override
        public String toString()
        {
//          int sz; 
//          synchronized(this) {
//            sz = this.size();
//          }
          return "Bucket id = " + bucketId + " from member = "
              + recipient
              + ": " + super.toString();
        }
      };
    }
    
    
    
    
    @Override
    public void process(DistributionMessage msg) {
      //If the reply is a region version vector, store it in our RVV field.
      if(msg instanceof ReplyMessage) {
        ReplyMessage reply = (ReplyMessage) msg;
        Object returnValue = reply.getReturnValue();
        if(returnValue instanceof RegionVersionVector) {
          this.returnRVV = (RegionVersionVector) returnValue;
          synchronized(this.endLock) {
            if(allMessagesReceived(true)) {
              super.process(msg);
            }
          }
          return;
        }
      }
      super.process(msg);
    }




    void processChunk(FetchEntriesReplyMessage msg) {
      // this processing algorighm won't work well if there are multiple recipients.  currently the
      // retry logic for failed recipients is in PartitionedRegion.  If we parallelize the sending
      // of this message, we'll need to handle failover in this processor class and track results
      // differently.

      final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DM);
      
      boolean doneProcessing = false;
      
      if (msg.getException() != null) {
        process(msg);
      }
      else {
        boolean deserializingKey = true;
        try {
          ByteArrayInputStream byteStream = new ByteArrayInputStream(msg.chunk);
          DataInputStream in = new DataInputStream(byteStream);
          final boolean requiresRegionContext = this.pr
              .keyRequiresRegionContext();
          Object key;
          
          while (in.available() > 0) {
            deserializingKey = true;
            key = DataSerializer.readObject(in);
            if (key != null) {
              if (requiresRegionContext) {
                ((KeyWithRegionContext)key).setRegionContext(this.pr);
              }
              deserializingKey = false;
              Object value = DataSerializer.readObject(in);
              VersionTag versionTag = DataSerializer.readObject(in);
              
              //Fix for 47260 - canonicalize the mebmer ids to avoid an OOME
              VersionSource id = versionTag.getMemberID();
              if (id != null) {
                if(canonicalMembers.containsKey(id)) {
                  versionTag.setMemberID(canonicalMembers.get(id));
                } else {
                  canonicalMembers.put(id, id);
                }
              }
              
              synchronized(returnValue) {
                returnValue.put(key, value);
                returnVersions.put(key, versionTag);
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
              chunksExpected = msg.msgNum + 1;
              lastChunkReceived = true;
            }
  
            if (allMessagesReceived(msg.hasRVV)) {
              doneProcessing = true;
            }
            if (isDebugEnabled) {
              logger.trace(LogMarker.DM, "{} chunksProcessed={},lastChunkReceived={},chunksExpected={},done={}",
                  this, chunksProcessed, lastChunkReceived, chunksExpected, doneProcessing);
            }
          }
        }
        catch (Exception e) {
          if (deserializingKey) {
            processException(new ReplyException(LocalizedStrings.FetchEntriesMessage_ERROR_DESERIALIZING_KEYS.toLocalizedString(), e));
          } else {
            processException(new ReplyException(LocalizedStrings.FetchEntriesMessage_ERROR_DESERIALIZING_VALUES.toLocalizedString(), e)); // for bug 41202
          }
          checkIfDone(); // fix for hang in 41202
        }
  
        // if all chunks have been received, wake up the waiting thread
        if (doneProcessing) {
          process(msg);
        }
      }
    }
    
    private boolean allMessagesReceived(boolean hasRVV) {
      synchronized(this.endLock) {
        return lastChunkReceived  &&  (chunksExpected == chunksProcessed) && (!hasRVV || returnRVV != null);
      }
    }
    
    /**
     * @return Set the keys associated with the bucketid of the {@link FetchKeysMessage}
     * @throws ForceReattemptException if the peer is no longer available
     */
    public BucketDump waitForEntries() throws ForceReattemptException {
      try {
        waitForRepliesUninterruptibly();
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          logger.debug("FetchKeysResponse got remote cancellation; forcing reattempt. {}", t.getMessage(), t);
          throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_FETCHKEYSRESPONSE_GOT_REMOTE_CANCELLATION_FORCING_REATTEMPT.toLocalizedString(), t);
        }
        else if (t instanceof ForceReattemptException) {
          // Not sure this is necessary, but it is possible for
          // FetchEntriesMessage to marshal a ForceReattemptException, so...
          throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_PEER_REQUESTS_REATTEMPT.toLocalizedString(), t);
        }
        e.handleAsUnexpected();
      }
      if (!this.lastChunkReceived) {
        throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_NO_REPLIES_RECEIVED.toLocalizedString());
      }
      // Deserialize all CachedDeserializable here so we have access to applications thread context class loader
      Iterator it = this.returnValue.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry)it.next();
        Object value = entry.getValue();
        if (value instanceof CachedDeserializable) {
          entry.setValue(((CachedDeserializable) value).getDeserializedValue(
              null, null));
        }
      }

      return new BucketDump(bucketId, recipient, returnRVV, returnValue, returnVersions);
    }
  }
 }
