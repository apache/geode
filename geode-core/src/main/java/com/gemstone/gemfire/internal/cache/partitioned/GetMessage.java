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
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.TransactionDataNotColocatedException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.BucketRegion.RawValue;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.DataLocationException;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.KeyInfo;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.VersionTagHolder;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * This message is used as the request for a
 * {@link com.gemstone.gemfire.cache.Region#get(Object)}operation. The reply is
 * sent in a {@link com.gemstone.gemfire.internal.cache.partitioned.GetMessage}. 
 * 
 * Since the {@link com.gemstone.gemfire.cache.Region#get(Object)}operation is
 * used <bold>very </bold> frequently the performance of this class is critical.
 * 
 * @since GemFire 5.0
 */
public final class GetMessage extends PartitionMessageWithDirectReply
  {
  private static final Logger logger = LogService.getLogger();
  
  private Object key;

  /** The callback arg of the operation */
  private Object cbArg;
  
  /** set to true if the message was requeued in the pr executor pool */
  private transient boolean forceUseOfPRExecutor;

  private ClientProxyMembershipID context;
  
  private boolean returnTombstones;

  // reuse some flags
  protected static final int HAS_LOADER = NOTIFICATION_ONLY;
  protected static final int CAN_START_TX = IF_NEW;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public GetMessage() {
  }
  
  private GetMessage(InternalDistributedMember recipient, int regionId,
                     DirectReplyProcessor processor,
                     final Object key, final Object aCallbackArgument, ClientProxyMembershipID context,
                     boolean returnTombstones) {
    super(recipient, regionId, processor);
    this.key = key;
    this.cbArg = aCallbackArgument;
    this.context = context;
    this.returnTombstones = returnTombstones;
  }

  private static final boolean ORDER_PR_GETS = Boolean.getBoolean("gemfire.order-pr-gets");

  @Override
  final public int getProcessorType()
  {
    if (!forceUseOfPRExecutor && !ORDER_PR_GETS && !isDirectAck()) {
      try {
        PartitionedRegion pr = PartitionedRegion.getPRFromId(this.regionId);
        // If the region is persistent the get may need to fault a value
        // in which has to sync the region entry. Note it may need to
        // do this even if it is not overflow (after recovery values are faulted in async).
        // If the region has an LRU then in lruUpdateCallback it will
        // call getLRUEntry which has to sync a region entry.
        // Syncing a region entry can lead to dead-lock (see bug 52078).
        // So if either of these conditions hold this message can not be
        // processed in the p2p msg reader.
        if (pr.getAttributes().getDataPolicy().withPersistence()
            || !pr.getAttributes().getEvictionAttributes().getAlgorithm().isNone()) {
          return DistributionManager.PARTITIONED_REGION_EXECUTOR;
        }
      } catch (PRLocallyDestroyedException ignore) {
      } catch (RuntimeException ignore) {
        // fix for GEODE-216
        // Most likely here would be RegionDestroyedException or CacheClosedException
        // but the cancel criteria code can throw any RuntimeException.
        // In all these cases it is ok to just fall through and return the
        // old executor type.
      }
    }
    if (forceUseOfPRExecutor) {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
    }
    else if (ORDER_PR_GETS) {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
    } else {
      // Make this guy serial so that it will be processed in the p2p msg reader
      // which gives it better performance.
      return DistributionManager.SERIAL_EXECUTOR;
    }
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }
  
  @Override
  protected final boolean operateOnPartitionedRegion(
      final DistributionManager dm, PartitionedRegion r, long startTime)
      throws ForceReattemptException
  {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "GetMessage operateOnRegion: {}", r.getFullPath());
    }

    PartitionedRegionDataStore ds = r.getDataStore();
    
    if (this.getTXUniqId() != TXManagerImpl.NOTX) {
      assert r.getDataView() instanceof TXStateProxy;
    }

    RawValue valueBytes;
    Object val = null;
    try {
    if (ds != null) {
      VersionTagHolder event = new VersionTagHolder();
      try {
        if (r.keyRequiresRegionContext()) {
          ((KeyWithRegionContext)this.key).setRegionContext(r);
        }
        KeyInfo keyInfo = r.getKeyInfo(key, cbArg);
        boolean lockEntry = forceUseOfPRExecutor || isDirectAck();
        
        val = r.getDataView().getSerializedValue(r, keyInfo, !lockEntry, this.context, event, returnTombstones);
        
        if(val == BucketRegion.REQUIRES_ENTRY_LOCK) {
          Assert.assertTrue(!lockEntry);
          this.forceUseOfPRExecutor = true;
          if (logger.isDebugEnabled()) {
            logger.debug("Rescheduling GetMessage due to possible cache-miss");
          }
          schedule(dm);
          return false;
        }
        valueBytes = val instanceof RawValue ? (RawValue)val : new RawValue(val);
      } 
      catch(DistributedSystemDisconnectedException sde) {
        sendReply(getSender(), this.processorId, dm, new ReplyException(new ForceReattemptException(LocalizedStrings.GetMessage_OPERATION_GOT_INTERRUPTED_DUE_TO_SHUTDOWN_IN_PROGRESS_ON_REMOTE_VM.toLocalizedString(), sde)), r, startTime);
        return false;
      }
      catch (PrimaryBucketException pbe) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe), r, startTime);
        return false;
      }
      catch (DataLocationException e) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(e), r, startTime);
        return false;
      }

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.debug("GetMessage sending serialized value {} back via GetReplyMessage using processorId: {}", valueBytes, getProcessorId());
      }
      
      r.getPrStats().endPartitionMessagesProcessing(startTime); 
      GetReplyMessage.send(getSender(), getProcessorId(), valueBytes, getReplySender(dm), event.getVersionTag());
   // Unless there was an exception thrown, this message handles sending the
      // response
      return false;
    }
    else {
      throw new InternalGemFireError(LocalizedStrings.GetMessage_GET_MESSAGE_SENT_TO_WRONG_MEMBER.toLocalizedString());
    }
    }finally {
      OffHeapHelper.release(val);
    }

    
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; key=").append(this.key).append("; callback arg=").append(this.cbArg)
      .append("; context=").append(this.context);
  }

  public int getDSFID() {
    return PR_GET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.cbArg = DataSerializer.readObject(in);
    this.context = DataSerializer.readObject(in);
    this.returnTombstones = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
    DataSerializer.writeObject(this.cbArg, out);
    DataSerializer.writeObject(this.context, out);
    out.writeBoolean(this.returnTombstones);
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    return s;
  }

  @Override
  protected void setBooleans(short s, DataInput in) throws ClassNotFoundException, IOException {
    super.setBooleans(s, in);
  }

  public void setKey(Object key)
  {
    this.key = key;
  }

  /**
   * Sends a PartitionedRegion
   * {@link com.gemstone.gemfire.cache.Region#get(Object)} message   
   * 
   * @param recipient
   *          the member that the get message is sent to
   * @param r
   *          the PartitionedRegion for which get was performed upon
   * @param key
   *          the object to which the value should be feteched
   * @param requestingClient the client cache that requested this item
   * @return the processor used to fetch the returned value associated with the
   *         key
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static GetResponse send(InternalDistributedMember recipient,
                                 PartitionedRegion r,
                                 final Object key,
                                 final Object aCallbackArgument,
                                 ClientProxyMembershipID requestingClient,
                                 boolean returnTombstones)
      throws ForceReattemptException
  {
    Assert.assertTrue(recipient != null,
        "PRDistribuedGetReplyMessage NULL reply message");
    GetResponse p = new GetResponse(r.getSystem(), Collections.singleton(recipient), key);
    GetMessage m = new GetMessage(recipient, r.getPRId(), p,
        key, aCallbackArgument, requestingClient, returnTombstones);
    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(LocalizedStrings.GetMessage_FAILED_SENDING_0.toLocalizedString(m));
    }

    return p;
  }

  /**
   * This message is used for the reply to a
   * {@link com.gemstone.gemfire.cache.Region#get(Object)}operation This is the
   * reply to a {@link GetMessage}.
   * 
   * Since the {@link com.gemstone.gemfire.cache.Region#get(Object)}operation
   * is used <bold>very </bold> frequently the performance of this class is
   * critical.
   * 
   * @since GemFire 5.0
   */
  public static final class GetReplyMessage extends ReplyMessage
   {
    /** 
     * The raw value in the cache which may be serialized to the output stream, if 
     * it is NOT already a byte array 
     */
    private transient RawValue rawVal;

    /**
     * Indicates that the value already a byte array (aka user blob) and does
     * not need de-serialization. Also indicates if the value has been
     * serialized directly as an object rather than as a byte array and whether
     * it is INVALID/LOCAL_INVALID or TOMBSTONE.
     */
    byte valueType;

    // static values for valueType
    static final byte VALUE_IS_SERIALIZED_OBJECT = 0;
    static final byte VALUE_IS_BYTES = 1;
    /** came from partial SQLF merge and reconciling with it but not used yet */
    //static final byte VALUE_IS_OBJECT = 2;
    static final byte VALUE_IS_INVALID = 3;
    static final byte VALUE_IS_TOMBSTONE = 4;
    static final byte VALUE_HAS_VERSION_TAG = 8;

    /*
     * Used on the fromData side to transfer the value bytes to the requesting
     * thread
     */
    public transient byte[] valueInBytes;
    
    /*
     * the version information for the entry
     */
    public VersionTag versionTag;

    public transient Version remoteVersion;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public GetReplyMessage() {
    }

    private GetReplyMessage(int processorId, RawValue val, VersionTag versionTag) {
      setProcessorId(processorId);
      this.rawVal = val;
      final Object rval = val.getRawValue();
      if (rval == Token.TOMBSTONE) {
        this.valueType = VALUE_IS_TOMBSTONE;
      }
      else if (Token.isInvalid(rval)) {
        this.valueType = VALUE_IS_INVALID;
      }
      else if (val.isValueByteArray()) {
        this.valueType = VALUE_IS_BYTES;
      }
      else {
        this.valueType = VALUE_IS_SERIALIZED_OBJECT;
      }
      this.versionTag = versionTag;
    }

    /** GetReplyMessages are always processed in-line */
    @Override
    public boolean getInlineProcess() {
      return true;
    }
    
    /**
     * Return the value from the get operation, serialize it bytes as late as
     * possible to avoid making un-neccesary byte[] copies.  De-serialize those 
     * same bytes as late as possible to avoid using precious threads (aka P2P readers). 
     * @param recipient the origin VM that performed the get
     * @param processorId the processor on which the origin thread is waiting
     * @param val the raw value that will eventually be serialized 
     * @param replySender distribution manager used to send the reply
     * @param versionTag the version of the object
     * @throws ForceReattemptException
     */
    public static void send(InternalDistributedMember recipient, 
        int processorId, RawValue val, ReplySender replySender, VersionTag versionTag)
        throws ForceReattemptException
    {
      Assert.assertTrue(recipient != null,
          "PRDistribuedGetReplyMessage NULL reply message");
      GetReplyMessage m = new GetReplyMessage(processorId, val, versionTag);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, ReplyProcessor21 processor)
    {
      final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DM);
      final long startTime = getTimestamp();
      if (isDebugEnabled) {
        logger.trace(LogMarker.DM, "GetReplyMessage process invoking reply processor with processorId: {}", this.processorId);
      }

      if (processor == null) {
        if (isDebugEnabled) {
          logger.debug("GetReplyMessage processor not found");
        }
        return;
      }
      
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(this.getSender());
      }
      
      processor.process(this);

      if (isDebugEnabled) {
        logger.debug("{} Processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return PR_GET_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      final boolean hasVersionTag = (this.versionTag != null);
      byte flags = this.valueType;
      if (hasVersionTag) {
        flags |= VALUE_HAS_VERSION_TAG;
      }
      out.writeByte(flags);
      if (this.valueType == VALUE_IS_BYTES) {
        DataSerializer.writeByteArray((byte[])this.rawVal.getRawValue(), out);
      }
      else {
        this.rawVal.writeAsByteArray(out);
      }
      if (hasVersionTag) {
        DataSerializer.writeObject(this.versionTag, out);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      byte flags = in.readByte();
      final boolean hasVersionTag;
      if ((hasVersionTag = (flags & VALUE_HAS_VERSION_TAG) != 0)) {
        flags &= ~VALUE_HAS_VERSION_TAG;
      }
      this.valueType = flags;
      this.valueInBytes = DataSerializer.readByteArray(in);
      if (flags != VALUE_IS_BYTES) {
        this.remoteVersion = InternalDataSerializer
            .getVersionForDataStreamOrNull(in);
      }
      if (hasVersionTag) {
        this.versionTag = (VersionTag)DataSerializer.readObject(in);
      }
    }

    @Override
    public String toString()
    {
      StringBuilder sb = new StringBuilder();
      sb.append("GetReplyMessage ").append("processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender());
      if (this.valueType == VALUE_IS_TOMBSTONE) {
        sb.append(" returning tombstone token.");
      }
      else if (this.valueType == VALUE_IS_INVALID) {
        sb.append(" returning invalid token.");
      }
      else if (this.rawVal != null) {
        sb.append(" returning serialized value=").append(this.rawVal);
      }
      else if (this.valueInBytes == null) {
        sb.append(" returning null value");
      } else {
        sb.append(" returning serialized value of len=").append(this.valueInBytes.length);
      }
      if (this.versionTag != null) {
        sb.append (" version=").append(this.versionTag);
      }
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage}
   * 
   * @since GemFire 5.0
   */
  public static class GetResponse extends PartitionResponse {
    private volatile GetReplyMessage getReply;
    private volatile boolean returnValueReceived;
    private volatile long start;
    final Object key;
    private VersionTag versionTag;

    public GetResponse(InternalDistributedSystem ds, Set recipients, Object key) {
      super(ds, recipients, false);
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg)
    {
      if (DistributionStats.enableClockStats) {
        this.start = DistributionStats.getStatTime();
      }
      if (msg instanceof GetReplyMessage) {
        GetReplyMessage reply = (GetReplyMessage)msg;
        // De-serialization needs to occur in the requesting thread, not a P2P thread
        // (or some other limited resource)
        if (reply.valueInBytes != null
            || reply.valueType == GetReplyMessage.VALUE_IS_INVALID
            || reply.valueType == GetReplyMessage.VALUE_IS_TOMBSTONE) {
          this.getReply = reply;
        }
        this.returnValueReceived = true;
        this.versionTag = reply.versionTag;
      }
      super.process(msg);
    }
    
    /**
     * De-seralize the value, if the value isn't already a byte array, this 
     * method should be called in the context of the requesting thread for the
     * best scalability
     * @param preferCD 
     * @see EntryEventImpl#deserialize(byte[])
     * @return the value object
     */
    public Object getValue(boolean preferCD) throws ForceReattemptException
    {
      final GetReplyMessage reply = this.getReply;
      try {
        if (reply != null) {
          switch (reply.valueType) {
            case GetReplyMessage.VALUE_IS_BYTES:
              return reply.valueInBytes;
            case GetReplyMessage.VALUE_IS_INVALID:
              return Token.INVALID;
            case GetReplyMessage.VALUE_IS_TOMBSTONE:
              return Token.TOMBSTONE;
            default:
              if (reply.valueInBytes != null) {
                if (preferCD) {
                  return CachedDeserializableFactory.create(reply.valueInBytes);
                }
                else {
                  return BlobHelper.deserializeBlob(reply.valueInBytes,
                      reply.remoteVersion, null);
                }
              }
              else {
                return null;
              }
          }
        }
        return null;
      }
      catch (IOException e) {
        throw new ForceReattemptException(LocalizedStrings.GetMessage_UNABLE_TO_DESERIALIZE_VALUE_IOEXCEPTION.toLocalizedString(), e);
      }
      catch (ClassNotFoundException e) {
        throw new ForceReattemptException(LocalizedStrings.GetMessage_UNABLE_TO_DESERIALIZE_VALUE_CLASSNOTFOUNDEXCEPTION.toLocalizedString(), e);
      }
    }
    
    /**
     * @return version information for the entry after successfully reading a response
     */
    public VersionTag getVersionTag() {
      return this.versionTag;
    }


    /**
     * @param preferCD 
     * @return Object associated with the key that was sent in the get message
     * @throws ForceReattemptException if the peer is no longer available
     */
    public Object waitForResponse(boolean preferCD) 
        throws ForceReattemptException {
      try {
//        waitForRepliesUninterruptibly();
          waitForCacheException();
          if (DistributionStats.enableClockStats) {
            getDistributionManager().getStats().incReplyHandOffTime(this.start);
          }
      }
      // Neeraj: Adding separate catch block for ENFE because there should not be a reattempt due
      // to this exception from the sender node. ENFE is a type of CacheException(caught below)
      // which wraps all CacheException in ForcedReattemptException(which is not correct). Filing
      // a separate bug for this.(#41717)
      catch (EntryNotFoundException enfe) {
        // rethrow this
        throw enfe;
      }
      catch (ForceReattemptException e) {
        e.checkKey(key);
        final String msg = "GetResponse got ForceReattemptException; rethrowing";
        logger.debug(msg, e);
        throw e;
      } catch (TransactionDataNotColocatedException e) {
        // Throw this up to user!
        throw e;
      }
      if (!this.returnValueReceived) {
        throw new ForceReattemptException(LocalizedStrings.GetMessage_NO_RETURN_VALUE_RECEIVED.toLocalizedString());
      }
      return getValue(preferCD);
    }
  }

}
