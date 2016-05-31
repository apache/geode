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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.OperationCancelledException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * This message is used as the request for a
 * {@link com.gemstone.gemfire.cache.Region#getEntry(Object)}operation. The
 * reply is sent in a {@link 
 * com.gemstone.gemfire.internal.cache.RemoteFetchEntryMessage.FetchEntryReplyMessage}.
 * 
 * @since GemFire 5.1
 */
public final class RemoteFetchEntryMessage extends RemoteOperationMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  private Object key;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteFetchEntryMessage() {
  }

  private RemoteFetchEntryMessage(InternalDistributedMember recipient, String regionPath,
      ReplyProcessor21 processor, final Object key) {
    super(recipient, regionPath, processor);
    this.key = key;
  }

  /**
   * Sends a LocalRegion
   * {@link com.gemstone.gemfire.cache.Region#getEntry(Object)} message   
   * 
   * @param recipient
   *          the member that the getEntry message is sent to
   * @param r
   *          the Region for which getEntry was performed upon
   * @param key
   *          the object to which the value should be feteched
   * @return the processor used to fetch the returned value associated with the
   *         key
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static FetchEntryResponse send(InternalDistributedMember recipient,
      LocalRegion r, final Object key)
      throws RemoteOperationException
  {
    Assert.assertTrue(recipient != null, "RemoteFetchEntryMessage NULL recipient");
    FetchEntryResponse p = new FetchEntryResponse(r.getSystem(), Collections
        .singleton(recipient), r, key);
    RemoteFetchEntryMessage m = new RemoteFetchEntryMessage(recipient, r.getFullPath(), p, key);

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(LocalizedStrings.RemoteFetchEntryMessage_FAILED_SENDING_0.toLocalizedString(m));
    }

    return p;
  }

//  final public int getProcessorType()
//  {
//    return DistributionManager.PARTITIONED_REGION_EXECUTOR;
//  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected final boolean operateOnRegion(DistributionManager dm,
      LocalRegion r, long startTime) throws RemoteOperationException
  {
    // RemoteFetchEntryMessage is used in refreshing client caches during interest list recovery,
    // so don't be too verbose or hydra tasks may time out

    if ( ! (r instanceof PartitionedRegion) ) {
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }
    EntrySnapshot val;
      try {
        if (r.keyRequiresRegionContext()) {
          ((KeyWithRegionContext)this.key).setRegionContext(r);
        }
        final KeyInfo keyInfo = r.getKeyInfo(key);
        Region.Entry re = r.getDataView().getEntry(keyInfo, r, true);
        if(re==null) {
          r.checkEntryNotFound(key);
        }
        NonLocalRegionEntry nlre = new NonLocalRegionEntry(re, r);
        LocalRegion dataReg = r.getDataRegionForRead(keyInfo);
        val = new EntrySnapshot(nlre,dataReg,r, false);
        //r.getPrStats().endRemoteOperationMessagesProcessing(startTime); 
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), val, dm, null);
      }
      catch (TransactionException tex) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, 
            new ReplyException(tex));
      }
      catch (EntryNotFoundException enfe) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, 
            new ReplyException(LocalizedStrings.RemoteFetchEntryMessage_ENTRY_NOT_FOUND.toLocalizedString(), enfe));
      }
      catch (PrimaryBucketException pbe) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, 
            new ReplyException(pbe));
      }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }


  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; key=").append(this.key);
  }

  public int getDSFID() {
    return R_FETCH_ENTRY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
  }

  public void setKey(Object key)
  {
    this.key = key;
  }

  /**
   * This message is used for the reply to a {@link RemoteFetchEntryMessage}.
   * 
   * @since GemFire 5.0
   */
  public static final class FetchEntryReplyMessage extends ReplyMessage
   {
    /** Propagated exception from remote node to operation initiator */
    private EntrySnapshot value;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public FetchEntryReplyMessage() {
    }

    private FetchEntryReplyMessage(int processorId,
        EntrySnapshot value, ReplyException re) {
      this.processorId = processorId;
      this.value = value;
      setException(re);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient,
        int processorId, EntrySnapshot value, DM dm, ReplyException re)
    {
      Assert.assertTrue(recipient != null,
          "FetchEntryReplyMessage NULL recipient");
      FetchEntryReplyMessage m = new FetchEntryReplyMessage(processorId, value, re);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 processor)
    {
      final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DM);
      
      final long startTime = getTimestamp();
      if (isDebugEnabled) {
        logger.trace(LogMarker.DM, "FetchEntryReplyMessage process invoking reply processor with processorId:{}", this.processorId);
      }

      if (processor == null) {
        if (isDebugEnabled) {
          logger.trace(LogMarker.DM, "FetchEntryReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (isDebugEnabled) {
        logger.trace(LogMarker.DM, "{}  processed  {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    public EntrySnapshot getValue()
    {
      return this.value;
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      if (this.value == null) {
        out.writeBoolean(true); // null entry
      }
      else {
        out.writeBoolean(false); // null entry
        InternalDataSerializer.invokeToData((DataSerializable)this.value, out);
      }
    }

    @Override
    public int getDSFID() {
      return R_FETCH_ENTRY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      boolean nullEntry = in.readBoolean();
      if (!nullEntry) {
        // since the Entry object shares state with the PartitionedRegion,
        // we have to find the region and ask it to create a new Entry instance
        // to be populated from the DataInput
        FetchEntryResponse processor = (FetchEntryResponse)ReplyProcessor21
            .getProcessor(this.processorId);
        if (processor == null) {
          throw new OperationCancelledException("This operation was cancelled (null processor)");
        }
        this.value = new EntrySnapshot(in,processor.region);
      }
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("FetchEntryReplyMessage ").append("processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender()).append(" returning value=").append(
              this.value);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.RemoteFetchEntryMessage.FetchEntryReplyMessage}
   * 
   */
  public static class FetchEntryResponse extends RemoteOperationResponse
   {
    private volatile EntrySnapshot returnValue;

    final LocalRegion region;
    final Object key;

    public FetchEntryResponse(InternalDistributedSystem ds, Set recipients,
        LocalRegion theRegion, Object key) {
      super(ds, recipients);
      this.region = theRegion;
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg)
    {
      try {
        if (msg instanceof FetchEntryReplyMessage) {
          FetchEntryReplyMessage reply = (FetchEntryReplyMessage)msg;
          this.returnValue = reply.getValue();
          if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM, "FetchEntryResponse return value is {}" , this.returnValue);
          }
        }
      }
      finally {
        super.process(msg);
      }
    }

    /**
     * @return Object associated with the key that was sent in the get message
     * @throws EntryNotFoundException
     * @throws RemoteOperationException if the peer is no longer available
     * @throws EntryNotFoundException
     */
    public EntrySnapshot waitForResponse() 
        throws EntryNotFoundException, RemoteOperationException {
      try {
        // waitForRepliesUninterruptibly();
        waitForCacheException();
      }
      catch (RemoteOperationException e) {
        e.checkKey(key);
        final String msg = "FetchEntryResponse got remote RemoteOperationException; rethrowing";
        logger.debug(msg, e);
        throw e;
      }
      catch (EntryNotFoundException e) {
        throw e;
      }
      catch (TransactionException e) {
        throw e;
      }
      catch (RegionDestroyedException e) {
        throw e;
      }
      catch (CacheException ce) {
        logger.debug("FetchEntryResponse got remote CacheException; forcing reattempt.", ce);
        throw new RemoteOperationException(LocalizedStrings.RemoteFetchEntryMessage_FETCHENTRYRESPONSE_GOT_REMOTE_CACHEEXCEPTION_FORCING_REATTEMPT.toLocalizedString(), ce);
      }
      return this.returnValue;
    }
  }

}
