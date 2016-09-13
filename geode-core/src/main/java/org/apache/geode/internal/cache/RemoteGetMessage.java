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

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
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
import com.gemstone.gemfire.internal.cache.BucketRegion.RawValue;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * This message is used as the request for a
 * {@link com.gemstone.gemfire.cache.Region#get(Object)}operation. The reply is
 * sent in a {@link com.gemstone.gemfire.internal.cache.RemoteGetMessage.GetReplyMessage}. 
 * 
 * Replicate regions can use this message to send a Get request to another peer.
 * 
 * @since GemFire 6.5
 */
public final class RemoteGetMessage extends RemoteOperationMessageWithDirectReply
  {
  private static final Logger logger = LogService.getLogger();
  
  private Object key;

  /** The callback arg of the operation */
  private Object cbArg;

  private ClientProxyMembershipID context;
  

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteGetMessage() {
  }
  
  private RemoteGetMessage(InternalDistributedMember recipient, String regionPath,
      DirectReplyProcessor processor,
      final Object key, final Object aCallbackArgument, ClientProxyMembershipID context) {
    super(recipient, regionPath, processor);
    this.key = key;
    this.cbArg = aCallbackArgument;
    this.context = context;
  }

  @Override
  final public int getProcessorType()
  {
      return DistributionManager.SERIAL_EXECUTOR;
    }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }
  
  @Override
  protected final boolean operateOnRegion(
      final DistributionManager dm, LocalRegion r, long startTime)
      throws RemoteOperationException
  {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "RemoteGetMessage operateOnRegion: {}", r.getFullPath());
    }

    if (this.getTXUniqId() != TXManagerImpl.NOTX) {
      assert r.getDataView() instanceof TXStateProxy;
    }
    
    if ( ! (r instanceof PartitionedRegion) ) { // prs already wait on initialization
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }

    RawValue valueBytes;
    Object val = null;
      try {
        KeyInfo keyInfo = r.getKeyInfo(key, cbArg);
        val = r.getDataView().getSerializedValue(r, keyInfo, false, this.context, null, false /*for replicate regions*/);
        valueBytes = val instanceof RawValue ? (RawValue)val : new RawValue(val);

        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "GetMessage sending serialized value {} back via GetReplyMessage using processorId: {}",
                       valueBytes, getProcessorId());
        }
      
        //      r.getPrStats().endPartitionMessagesProcessing(startTime); 
        GetReplyMessage.send(getSender(), getProcessorId(), valueBytes, getReplySender(dm));

        // Unless there was an exception thrown, this message handles sending the
        // response
        return false;
      } 
      catch(DistributedSystemDisconnectedException sde) {
        sendReply(getSender(), this.processorId, dm, new ReplyException(new RemoteOperationException(LocalizedStrings.GetMessage_OPERATION_GOT_INTERRUPTED_DUE_TO_SHUTDOWN_IN_PROGRESS_ON_REMOTE_VM.toLocalizedString(), sde)), r, startTime);
        return false;
      }
      catch (PrimaryBucketException pbe) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe), r, startTime);
        return false;
      }
      catch (DataLocationException e) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(e), r, startTime);
        return false;
      }finally {
        OffHeapHelper.release(val);
      }

  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; key=").append(this.key).append("; callback arg=").append(this.cbArg);
  }

  public int getDSFID() {
    return R_GET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.cbArg = DataSerializer.readObject(in);
    this.context = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
    DataSerializer.writeObject(this.cbArg, out);
    DataSerializer.writeObject(this.context, out);
  }

  public void setKey(Object key)
  {
    this.key = key;
  }

  /**
   * Sends a ReplicateRegion
   * {@link com.gemstone.gemfire.cache.Region#get(Object)} message   
   * 
   * @param recipient
   *          the member that the get message is sent to
   * @param r
   *          the ReplicateRegion for which get was performed upon
   * @param key
   *          the object to which the value should be feteched
   * @param requestingClient the client requesting the value
   * @return the processor used to fetch the returned value associated with the
   *         key
   */
  public static RemoteGetResponse send(InternalDistributedMember recipient,
      LocalRegion r, final Object key, final Object aCallbackArgument, ClientProxyMembershipID requestingClient)
      throws RemoteOperationException
  {
    Assert.assertTrue(recipient != null,
        "PRDistribuedGetReplyMessage NULL reply message");
    RemoteGetResponse p = new RemoteGetResponse(r.getSystem(), Collections.singleton(recipient), key);
    RemoteGetMessage m = new RemoteGetMessage(recipient, r.getFullPath(), p,
        key, aCallbackArgument, requestingClient);
    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(LocalizedStrings.GetMessage_FAILED_SENDING_0.toLocalizedString(m));
    }

    return p;
  }

  /**
   * This message is used for the reply to a
   * {@link com.gemstone.gemfire.cache.Region#get(Object)}operation This is the
   * reply to a {@link RemoteGetMessage}.
   * 
   * Since the {@link com.gemstone.gemfire.cache.Region#get(Object)}operation
   * is used <bold>very </bold> frequently the performance of this class is
   * critical.
   * 
   * @since GemFire 6.5
   */
  public static final class GetReplyMessage extends ReplyMessage
   {
    /** 
     * The raw value in the cache which may be serialized to the output stream, if 
     * it is NOT already a byte array 
     */
    private transient RawValue rawVal;

    /** Indicates that the value already a byte array (aka user blob) and does 
     * not need de-serialization
     */
    public boolean valueIsByteArray;
    
    /*
     * Used on the fromData side to transfer the value bytes to the requesting
     * thread
     */
    public transient byte[] valueInBytes;

    public transient Version remoteVersion;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public GetReplyMessage() {
    }

    private GetReplyMessage(int processorId, RawValue val) {
      setProcessorId(processorId);
      this.rawVal = val;
      this.valueIsByteArray = val.isValueByteArray();
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
     */
    public static void send(InternalDistributedMember recipient, 
        int processorId, RawValue val, ReplySender replySender)
        throws RemoteOperationException
    {
      Assert.assertTrue(recipient != null,
          "PRDistribuedGetReplyMessage NULL reply message");
      GetReplyMessage m = new GetReplyMessage(processorId, val);
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
        logger.trace(LogMarker.DM, "GetReplyMessage process invoking reply processor with processorId:{}", this.processorId);
      }

      if (processor == null) {
        if (isDebugEnabled) {
          logger.trace(LogMarker.DM, "GetReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (isDebugEnabled) {
        logger.trace(LogMarker.DM, "{} Processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_GET_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      out.writeByte(this.valueIsByteArray ? 1 : 0);
      this.rawVal.writeAsByteArray(out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.valueIsByteArray = (in.readByte() == 1);
      this.valueInBytes = DataSerializer.readByteArray(in);
      if (!this.valueIsByteArray) {
        this.remoteVersion = InternalDataSerializer
            .getVersionForDataStreamOrNull(in);
      }
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("GetReplyMessage ").append("processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender())
          .append(" returning serialized value=")
          .append(this.rawVal);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.RemoteGetMessage.GetReplyMessage}
   * 
   * @since GemFire 5.0
   */
  public static class RemoteGetResponse extends RemoteOperationResponse {

    private volatile GetReplyMessage getReply;
    private volatile boolean returnValueReceived;
    private volatile long start;
    final Object key;

    public RemoteGetResponse(InternalDistributedSystem ds, Set recipients, Object key) {
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
        if (reply.valueInBytes != null) {
          this.getReply = reply;
        }
        this.returnValueReceived = true;
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
    public Object getValue(boolean preferCD) throws RemoteOperationException
    {
      final GetReplyMessage reply = this.getReply;
      try {
        if (reply != null) {
          if (reply.valueIsByteArray) {
            return reply.valueInBytes;
          }
          else if (preferCD) {
            return CachedDeserializableFactory.create(reply.valueInBytes);
          }
          else {
            return BlobHelper.deserializeBlob(reply.valueInBytes,
                reply.remoteVersion, null);
          }
        }
        return null;
      }
      catch (IOException e) {
        throw new RemoteOperationException(LocalizedStrings.GetMessage_UNABLE_TO_DESERIALIZE_VALUE_IOEXCEPTION.toLocalizedString(), e);
      }
      catch (ClassNotFoundException e) {
        throw new RemoteOperationException(LocalizedStrings.GetMessage_UNABLE_TO_DESERIALIZE_VALUE_CLASSNOTFOUNDEXCEPTION.toLocalizedString(), e);
      }
    }


    /**
     * @return Object associated with the key that was sent in the get message
     */
    public Object waitForResponse(boolean preferCD) 
        throws RemoteOperationException {
      try {
//        waitForRepliesUninterruptibly();
          waitForCacheException();
          if (DistributionStats.enableClockStats) {
            getDistributionManager().getStats().incReplyHandOffTime(this.start);
          }
      }
      catch (RemoteOperationException e) {
        e.checkKey(key);
        final String msg = "RemoteGetResponse got RemoteOperationException; rethrowing";
        logger.debug(msg, e);
        throw e;
      } catch (TransactionDataNotColocatedException e) {
        // Throw this up to user!
        throw e;
      }
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(LocalizedStrings.GetMessage_NO_RETURN_VALUE_RECEIVED.toLocalizedString());
      }
      return getValue(preferCD);
    }
  }

}
