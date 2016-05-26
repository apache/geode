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
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * This message is used be a replicate region to send a contains key/value request
 * to another peer.
 * @since GemFire 6.5
 */
public final class RemoteContainsKeyValueMessage extends RemoteOperationMessageWithDirectReply
  {
  private static final Logger logger = LogService.getLogger();
  
  private boolean valueCheck;

  private Object key;

  protected static final short VALUE_CHECK = UNRESERVED_FLAGS_START;

  public RemoteContainsKeyValueMessage() {
  }

  public RemoteContainsKeyValueMessage(InternalDistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, Object key, boolean valueCheck) {
    super(recipient, regionPath, processor);
    this.valueCheck = valueCheck;
    this.key = key;
  }

  /**
   * Sends a ReplicateRegion message for either
   * {@link com.gemstone.gemfire.cache.Region#containsKey(Object)}or
   * {@link com.gemstone.gemfire.cache.Region#containsValueForKey(Object)}
   * depending on the <code>valueCheck</code> argument
   * 
   * @param recipient
   *          the member that the contains keys/value message is sent to
   * @param r
   *          the LocalRegion
   * @param key
   *          the key to be queried
   * @param valueCheck
   *          true if
   *          {@link com.gemstone.gemfire.cache.Region#containsValueForKey(Object)}
   *          is desired, false if
   *          {@link com.gemstone.gemfire.cache.Region#containsKey(Object)}is
   *          desired
   * @return the processor used to read the returned keys
   */
  public static RemoteContainsKeyValueResponse send(InternalDistributedMember recipient,
      LocalRegion r, Object key, boolean valueCheck)
      throws RemoteOperationException {
    Assert.assertTrue(recipient != null,
        "PRDistribuedRemoteContainsKeyValueMessage NULL reply message");

    RemoteContainsKeyValueResponse p = new RemoteContainsKeyValueResponse(r.getSystem(),
        Collections.singleton(recipient), key);
    RemoteContainsKeyValueMessage m = new RemoteContainsKeyValueMessage(recipient, r.getFullPath(), p, key, valueCheck);

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(LocalizedStrings.FAILED_SENDING_0.toLocalizedString(m));
    }
    return p;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected boolean operateOnRegion(DistributionManager dm,
      LocalRegion r, long startTime) throws CacheException,
      RemoteOperationException
  {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "DistributedRemoteContainsKeyValueMessage operateOnRegion: {}", r.getFullPath());
    }
    
    if ( ! (r instanceof PartitionedRegion) ) {  // prs already wait on initialization
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }

    if (r.keyRequiresRegionContext()) {
      ((KeyWithRegionContext)this.key).setRegionContext(r);
    }
    final boolean replyVal;
        if (this.valueCheck) {
          replyVal = r.containsValueForKey(this.key);
        } else {
          replyVal = r.containsKey(this.key);
        }

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.debug("DistributedRemoteContainsKeyValueMessage sending reply back using processorId: {}",getProcessorId());
      }

      //r.getPrStats().endPartitionMessagesProcessing(startTime); 
      RemoteContainsKeyValueReplyMessage.send(getSender(), getProcessorId(), getReplySender(dm),
          replyVal);

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; valueCheck=").append(this.valueCheck).append("; key=")
        .append(this.key);
  }

  public int getDSFID() {
    return R_CONTAINS_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.valueCheck = (flags & VALUE_CHECK) != 0;
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
  }

  @Override
  protected short computeCompressedShort() {
    short flags = super.computeCompressedShort();
    if (this.valueCheck) flags |= VALUE_CHECK;
    return flags;
  }

  public static final class RemoteContainsKeyValueReplyMessage extends
      ReplyMessage
   {

    /** Propagated exception from remote node to operation initiator */
    private boolean containsKeyValue;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoteContainsKeyValueReplyMessage() {
    }

    private RemoteContainsKeyValueReplyMessage(int processorId,
        boolean containsKeyValue) {
      this.processorId = processorId;
      this.containsKeyValue = containsKeyValue;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender, boolean containsKeyValue)
    {
      Assert.assertTrue(recipient != null,
          "ContainsKeyValueReplyMessage NULL reply message");
      RemoteContainsKeyValueReplyMessage m = new RemoteContainsKeyValueReplyMessage(
          processorId, containsKeyValue);
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
      final long startTime = getTimestamp();

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "ContainsKeyValueReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_CONTAINS_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.containsKeyValue = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      out.writeBoolean(this.containsKeyValue);
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("ContainsKeyValueReplyMessage ").append(
          "processorid=").append(this.processorId).append(" reply to sender ")
          .append(this.getSender()).append(" returning containsKeyValue=")
          .append(doesItContainKeyValue());
      return sb.toString();
    }

    public boolean doesItContainKeyValue()
    {
      return this.containsKeyValue;
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.RemoteContainsKeyValueMessage.RemoteContainsKeyValueReplyMessage}
   * 
   * @since GemFire 6.5
   */
  public static class RemoteContainsKeyValueResponse extends RemoteOperationResponse
   {
    private volatile boolean returnValue;
    private volatile boolean returnValueReceived;
    final Object key;

    public RemoteContainsKeyValueResponse(InternalDistributedSystem ds,
        Set recipients, Object key) {
      super(ds, recipients, false);
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof RemoteContainsKeyValueReplyMessage) {
          RemoteContainsKeyValueReplyMessage reply = (RemoteContainsKeyValueReplyMessage)msg;
          this.returnValue = reply.doesItContainKeyValue();
          this.returnValueReceived = true;
          if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM, "ContainsKeyValueResponse return value is {}", this.returnValue);
          }
        }
      }
      finally {
        super.process(msg);
      }
    }

    /**
     * @return Set the keys associated with the ReplicateRegion of the
     *         {@link RemoteContainsKeyValueMessage}
     * @throws PrimaryBucketException if the instance of the bucket that received this operation was not primary
     */
    public boolean waitForContainsResult() throws PrimaryBucketException,
        RemoteOperationException {
      try {
        waitForCacheException();
      }
      catch (RemoteOperationException rce) {
        rce.checkKey(key);
        throw rce;
      }
      catch (PrimaryBucketException pbe) {
        // Is this necessary?
        throw pbe;
      }
      catch (RegionDestroyedException e) {
        throw e;
      }
      catch (CacheException ce) {
        logger.debug("ContainsKeyValueResponse got remote CacheException; forcing reattempt.", ce);
        throw new RemoteOperationException(LocalizedStrings.RemoteContainsKeyValueMessage_CONTAINSKEYVALUERESPONSE_GOT_REMOTE_CACHEEXCEPTION.toLocalizedString(), ce);
      }
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(LocalizedStrings.RemoteContainsKeyValueMessage_NO_RETURN_VALUE_RECEIVED.toLocalizedString());
      }
      return this.returnValue;
    }
  }

}
