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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.VersionTagHolder;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * 
 * @since GemFire 8.0
 */
public final class FetchBulkEntriesMessage extends PartitionMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  private HashSet<Integer> bucketIds;

  private String regex;

  /**
   * Map of bucket-id as key and set of keys as value.
   */
  private HashMap<Integer, HashSet> bucketKeys;
  
  private static final byte ALL_KEYS = (byte)0;

  private static final byte KEY_LIST = (byte)1;

  private static final byte REGEX = (byte)2;

  private byte keys;

  private boolean allowTombstones;

  public FetchBulkEntriesMessage() {
  }

  private FetchBulkEntriesMessage(InternalDistributedMember recipient,
      int regionId, ReplyProcessor21 processor,
      HashMap<Integer, HashSet> bucketKeys, HashSet<Integer> bucketIds,
      String regex, boolean allowTombstones) {
    super(recipient, regionId, processor);
    this.bucketKeys = bucketKeys;
    this.bucketIds = bucketIds;
    this.regex = regex;
    this.keys = bucketKeys != null ? KEY_LIST : ALL_KEYS;
    this.allowTombstones = allowTombstones;
  }

  /**
   * Sends a PartitionedRegion message to fetch all the entries for a bucketId
   * @param recipient the member that the fetch keys message is sent to 
   * @param r  the PartitionedRegion that contains the bucket
   * @param bucketIds the identity of the buckets that contain the entries to be returned
   * @param regex the regular expression to be evaluated for selecting keys
   * @param allowTombstones
   * @return the processor used to read the returned entries
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static FetchBulkEntriesResponse send(
      InternalDistributedMember recipient, PartitionedRegion r,
      HashMap<Integer, HashSet> bucketKeys, HashSet<Integer> bucketIds,
      String regex, boolean allowTombstones) throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "FetchBulkEntriesMessage NULL reply message");
    FetchBulkEntriesResponse p = new FetchBulkEntriesResponse(r.getSystem(), r, recipient);
    FetchBulkEntriesMessage m = new FetchBulkEntriesMessage(recipient, r.getPRId(), p, bucketKeys,
        bucketIds, regex, allowTombstones);

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
      logger.debug("FetchBulkEntriesMessage operateOnRegion: {}", pr.getFullPath());
    }

    FetchBulkEntriesReplyMessage.sendReply(pr, getSender(), getProcessorId(),
        dm, this.bucketKeys, this.bucketIds, regex, this.allowTombstones, startTime);
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketIds);
    buff.append("; recipient=").append(this.getRecipient());
  }

  public int getDSFID() {
    return PR_FETCH_BULK_ENTRIES_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.keys = DataSerializer.readByte(in);
    if (this.keys == KEY_LIST) {
      this.bucketKeys = DataSerializer.readHashMap(in);
    } else if (this.keys == ALL_KEYS) {
      this.bucketIds = DataSerializer.readHashSet(in);
    }
    this.regex = DataSerializer.readString(in);
    this.allowTombstones = DataSerializer.readPrimitiveBoolean(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeByte(this.keys, out);
    if (this.keys == KEY_LIST) {
      DataSerializer.writeHashMap(this.bucketKeys, out);
    } else if (this.keys == ALL_KEYS) {
      DataSerializer.writeHashSet(this.bucketIds, out);
    }
    DataSerializer.writeString(this.regex, out);
    DataSerializer.writePrimitiveBoolean(this.allowTombstones, out);
  }
  
  public static final class FetchBulkEntriesReplyMessage extends ReplyMessage {
    
    /** Whether this message is the last of a series of chunk responses */
    boolean lastInSeries;
    /** Array holding chunk of entries received */
    transient byte[] chunk;
    /** Stream holding chunk of entries to send */
    transient HeapDataOutputStream chunkStream;
    /** Sequence number of this chunk message */
    private int msgNum;

    private HashSet<Integer> failedBucketIds;

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }

    /**
     * Empty constructor to conform to DataSerializable interface 
     */
    public FetchBulkEntriesReplyMessage() {
    }
  
    private FetchBulkEntriesReplyMessage(InternalDistributedMember dest,
        int processorId, HeapDataOutputStream chunk, int msgNum, boolean lastInSeries) {
      setRecipient(dest);
      setProcessorId(processorId);
      this.lastInSeries = lastInSeries;
      this.chunkStream = chunk;
      this.msgNum = msgNum;
    }

    public static void sendReply(PartitionedRegion pr,
        final InternalDistributedMember recipient, final int processorId,
        final DM dm, final HashMap<Integer, HashSet> bucketKeys,
        final HashSet<Integer> bucketIds, String regex,
        boolean allowTombstones, long startTime) throws ForceReattemptException {

      PartitionedRegionDataStore ds = pr.getDataStore();
      if (ds == null) {
        return;
      }
      ArrayList<BucketRegion> maps = new ArrayList<BucketRegion>();
      HashSet<Integer> failedBuckets = new HashSet<Integer>();

      Set<Integer> bucketIdSet = null;
      if (bucketKeys != null) {
        bucketIdSet = bucketKeys.keySet();
      } else { // bucketIds != null
        bucketIdSet = bucketIds;
      }
      for (int id : bucketIdSet) {
        try {
          maps.add(ds.handleRemoteGetEntries(id));
        } catch (ForceReattemptException fre) {
          failedBuckets.add(id);
        }
      }
      
      HeapDataOutputStream mos = new HeapDataOutputStream(
          InitialImageOperation.CHUNK_SIZE_IN_BYTES + 2048,
          recipient.getVersionObject());
      Iterator<BucketRegion> mapsIterator = maps.iterator();
      BucketRegion map = null;
      Iterator it = null;

      boolean keepGoing = true;
      boolean lockAcquired = false;
      boolean writeFooter = false;
      boolean lastMsgSent = false;
      boolean needToWriteBucketInfo = true;
      int msgNum = 0;

      while (mapsIterator.hasNext()) {
        if (map != null && lockAcquired) {
          try {
            map.releaseDestroyLock();
            // instead take a bucketCreationLock.getWriteLock() or pr.BucketLock?
          } catch (CancelException e) {
          } finally {
            lockAcquired = false;
          }
        }
        map = mapsIterator.next();
        if (map.isBucketDestroyed()) {
          failedBuckets.add(map.getId());
          continue;
        }
        try {
          map.acquireDestroyLock();
          lockAcquired = true;
        }
        catch (CancelException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("sendReply: acquireDestroyLock failed due to cache closure, region = {}", map.getFullPath());
          }
        }

        try {
          if (bucketKeys != null) {
            it = bucketKeys.get(map.getId()).iterator();
          } else { // bucketIds != null
            if (regex == null) {
              it = new HashSet(map.keySet(allowTombstones)).iterator();
            } else {
              it = map.getKeysWithInterest(InterestType.REGULAR_EXPRESSION,
                  regex, allowTombstones).iterator();
            }
          }

          while (it.hasNext()) {
            Object key = it.next();
            VersionTagHolder clientEvent = new VersionTagHolder();
            Object value = map.get(key, null, true, true, true, null,
                clientEvent, allowTombstones);

            if (needToWriteBucketInfo) {
              DataSerializer.writePrimitiveInt(map.getId(), mos);
              needToWriteBucketInfo = false;
              writeFooter = true;
            }

            int entrySize = mos.size();
            DataSerializer.writeObject(key, mos);
            VersionTag versionTag = clientEvent.getVersionTag();
            if (versionTag != null) {
              versionTag.replaceNullIDs(map.getVersionMember());
            }
            DataSerializer.writeObject(value, mos);
            DataSerializer.writeObject(versionTag, mos);
            entrySize = mos.size() - entrySize; 

            // If no more space OR no more entries in bucket, write end-of-bucket marker.
            if ((mos.size() + entrySize) >= InitialImageOperation.CHUNK_SIZE_IN_BYTES || !it.hasNext()) {

              DataSerializer.writeObject(null, mos);
              DataSerializer.writePrimitiveBoolean(it.hasNext(), mos);
              needToWriteBucketInfo = true;
              writeFooter = false; // Be safe
            }

            // If no more space in chunk, send it.
            if ((mos.size() + entrySize) >= InitialImageOperation.CHUNK_SIZE_IN_BYTES) {

              // Send as last message if no more data
              boolean lastMsg = !(it.hasNext() || mapsIterator.hasNext());

              ++msgNum;
              FetchBulkEntriesReplyMessage reply = new FetchBulkEntriesReplyMessage(
                  recipient, processorId, mos, msgNum, lastMsg);
              if (lastMsg) {
                reply.failedBucketIds = failedBuckets;
              }
              Set failures = dm.putOutgoing(reply);
              keepGoing = (failures == null) || (failures.size() == 0);
              if (lastMsg && keepGoing) {
                lastMsgSent = true;
              }
              mos.reset();

            } // else still enough space
          } // while (for each key)

          if (!keepGoing) {
            throw new ForceReattemptException("Failed to send response");
          }
        } catch (IOException ioe) {
          throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCHENTRIES_REQUEST.toLocalizedString(), ioe);
        } finally {
          if (lockAcquired) {
            try {
              map.releaseDestroyLock();
            } catch (CancelException e) {
            } finally {
              lockAcquired = false;
            }
          }
        }
      } // while (for each map)

      if (!lastMsgSent) {
        if (mos.size() == 0) {
          try {
            DataSerializer.writePrimitiveInt(-1, mos);
          } catch (IOException ioe) {
            throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCHENTRIES_REQUEST.toLocalizedString(), ioe);
          }
        } else if (writeFooter) {
          try {
            DataSerializer.writeObject(null, mos); // end of entries of current bucket in current response
            DataSerializer.writePrimitiveBoolean(false, mos); // no more entries of current bucket
          } catch (IOException ioe) {
            throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCHENTRIES_REQUEST.toLocalizedString(), ioe);
          }
        }
        ++msgNum;
        FetchBulkEntriesReplyMessage reply = new FetchBulkEntriesReplyMessage(
            recipient, processorId, mos, msgNum, true);
        reply.failedBucketIds = failedBuckets;
        Set failures = dm.putOutgoing(reply);
        if (failures != null && failures.size() > 0) {
          throw new ForceReattemptException("Failed to send response");
        }
      }

      if (lockAcquired) {
        try {
          map.releaseDestroyLock();
        } catch (CancelException e) {
          // ignore
        } finally {
          lockAcquired = false;
        }
      }
    }

    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 p) {
      final long startTime = getTimestamp();
      FetchBulkEntriesResponse processor = (FetchBulkEntriesResponse)p;
  
      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "FetchBulkEntriesReplyMessage processor not found");
        }
        return;
      }
      processor.processChunkResponse(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", processor, this);
      }

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime()
          - startTime);
    }
    
   
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.lastInSeries);
      DataSerializer.writePrimitiveInt(this.msgNum, out);
      DataSerializer.writeObjectAsByteArray(this.chunkStream, out);
      DataSerializer.writeHashSet(this.failedBucketIds, out);
    }
    
    @Override
    public int getDSFID() {
      return PR_FETCH_BULK_ENTRIES_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.lastInSeries = in.readBoolean();
      this.msgNum = DataSerializer.readPrimitiveInt(in);
      this.chunk = DataSerializer.readByteArray(in);
      this.failedBucketIds = DataSerializer.readHashSet(in);
    }
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("FetchBulkEntriesReplyMessage ")
        .append("processorid=").append(this.processorId);
      if (getSender() != null) { 
        sb.append(",sender=").append(this.getSender());
      }
      sb.append(",lastInSeries=").append(lastInSeries);
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
   * com.gemstone.gemfire.internal.cache.partitioned.FetchBulkEntriesMessage}
   * @since GemFire 8.0
   */
  public static class FetchBulkEntriesResponse extends ReplyProcessor21  {

    private final PartitionedRegion pr;

    private final HashMap<Integer, HashMap<Object, Object>> returnValue;
    private final HashMap<Integer, HashMap<Object, VersionTag>> returnVersions = new HashMap();
    private final Map<VersionSource,VersionSource> canonicalMembers = new ConcurrentHashMap<VersionSource,VersionSource>();
    
    /** lock used to synchronize chunk processing */
    private final Object endLock = new Object();
    
    /** number of chunks processed */
    private volatile int chunksProcessed;
    
    /** whether the last chunk has been processed */
    private volatile boolean lastChunkReceived;

    private HashSet<Integer> failedBucketIds;

    private ArrayList<Integer> receivedBuckets = new ArrayList<Integer>();
    
    private int expectedChunks;

    private InternalDistributedMember recipient;
    
    public FetchBulkEntriesResponse(InternalDistributedSystem ds,
        final PartitionedRegion pr, final InternalDistributedMember recipient) {
      super(ds, Collections.singleton(recipient));
      this.pr = pr;
      this.recipient = recipient;
      this.returnValue = new HashMap<Integer, HashMap<Object, Object>>();
    }

    void processChunkResponse(FetchBulkEntriesReplyMessage msg) {
      boolean doneProcessing = false;
      
      if (msg.getException() != null) {
        process(msg);
      }
      else {
        boolean deserializingKey = true;
        try {
          ByteArrayInputStream byteStream = new ByteArrayInputStream(msg.chunk);
          DataInputStream in = new DataInputStream(byteStream);
          Object key;
          int currentId;

          final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DM);
          while (in.available() > 0) {
            currentId = DataSerializer.readPrimitiveInt(in);
            if (currentId == -1) {
              break;
            }
            while (in.available() > 0) {
              deserializingKey = true;
              key = DataSerializer.readObject(in);
              if (key != null) {
                deserializingKey = false;
                Object value = DataSerializer.readObject(in);
                VersionTag versionTag = DataSerializer.readObject(in);

                if (versionTag != null) {
                  // Fix for 47260 - canonicalize the member ids to avoid an OOME
                  if (canonicalMembers.containsKey(versionTag.getMemberID())) {
                    versionTag.setMemberID(canonicalMembers.get(versionTag
                        .getMemberID()));
                  } else {
                    canonicalMembers.put(versionTag.getMemberID(),
                        versionTag.getMemberID());
                  }
                }

                synchronized (returnValue) {
                  HashMap<Object, Object> valueMap = returnValue.get(currentId);
                  HashMap<Object, VersionTag> versionMap = returnVersions
                      .get(currentId);
                  if (valueMap != null) {
                    valueMap.put(key, value);
                  } else {
                    valueMap = new HashMap<Object, Object>();
                    valueMap.put(key, value);
                    returnValue.put(currentId, valueMap);
                  }
                  if (versionMap != null) {
                    versionMap.put(key, versionTag);
                  } else {
                    versionMap = new HashMap<Object, VersionTag>();
                    versionMap.put(key, versionTag);
                    returnVersions.put(currentId, versionMap);
                  }
                }
              } else {
                // null should signal the end of the set of keys
                boolean bucketHasMore = DataSerializer.readPrimitiveBoolean(in);
                synchronized (this.returnValue) {
                  if (!bucketHasMore && currentId != -1) {
                    this.receivedBuckets.add(currentId);
                  }
                }
                break;
              }
            } // inner while
          }  // outer while

          synchronized(this.endLock) {
            this.chunksProcessed = this.chunksProcessed + 1;
  
            if (msg.lastInSeries) {
              this.expectedChunks = msg.msgNum;
              this.failedBucketIds = msg.failedBucketIds;
            }
            if (this.expectedChunks == this.chunksProcessed) {
              doneProcessing = true;
              this.lastChunkReceived = true;
            }
  
            if (isDebugEnabled) {
              logger.trace(LogMarker.DM, "{} chunksProcessed={}, lastChunkReceived={},done={}",
                  this, this.chunksProcessed, this.lastChunkReceived, doneProcessing);
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
      } // else msg.getException() == null
    }

    /**
     * @return Array of BucketDumps
     * @throws ForceReattemptException if the peer is no longer available
     */
    public BucketDump[] waitForEntries() throws ForceReattemptException {
      try {
        waitForRepliesUninterruptibly();
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          logger.debug("FetchBulkEntriesResponse got remote cancellation; forcing reattempt. {}", t.getMessage(), t);
          throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_FETCHKEYSRESPONSE_GOT_REMOTE_CANCELLATION_FORCING_REATTEMPT.toLocalizedString(), t);
        }
        else if (t instanceof ForceReattemptException) {
          // Not sure this is necessary, but it is possible for
          // FetchBulkEntriesMessage to marshal a ForceReattemptException, so...
          throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_PEER_REQUESTS_REATTEMPT.toLocalizedString(), t);
        }
        e.handleAsUnexpected();
      }
      if (!this.lastChunkReceived) {
        throw new ForceReattemptException(LocalizedStrings.FetchEntriesMessage_NO_REPLIES_RECEIVED.toLocalizedString());
      }

      BucketDump[] dumps = new BucketDump[this.receivedBuckets.size()];
      for (int i = 0; i < this.receivedBuckets.size(); i++) {
        int id = this.receivedBuckets.get(i);
        dumps[i] = new BucketDump(id, recipient, null, returnValue.get(id),
            returnVersions.get(id));
      }
      return dumps;
    }
    
    public HashSet<Integer> getFailedBucketIds() {
      return this.failedBucketIds;
    }
  }

  public Version[] getSerializationVersions() {
    return null;
  }

}
