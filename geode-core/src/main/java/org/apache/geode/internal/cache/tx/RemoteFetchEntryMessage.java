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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonLocalRegionEntry;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This message is used as the request for a
 * {@link org.apache.geode.cache.Region#getEntry(Object)}operation. The reply is sent in a
 * {@link org.apache.geode.internal.cache.tx.RemoteFetchEntryMessage.FetchEntryReplyMessage}.
 *
 * @since GemFire 5.1
 */
public class RemoteFetchEntryMessage extends RemoteOperationMessage {
  private static final Logger logger = LogService.getLogger();

  private Object key;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteFetchEntryMessage() {}

  private RemoteFetchEntryMessage(InternalDistributedMember recipient, String regionPath,
      ReplyProcessor21 processor, final Object key) {
    super(recipient, regionPath, processor);
    this.key = key;
  }

  /**
   * Sends a LocalRegion {@link org.apache.geode.cache.Region#getEntry(Object)} message
   *
   * @param recipient the member that the getEntry message is sent to
   * @param r the Region for which getEntry was performed upon
   * @param key the object to which the value should be fetched
   * @return the processor used to fetch the returned value associated with the key
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static FetchEntryResponse send(InternalDistributedMember recipient, LocalRegion r,
      final Object key) throws RemoteOperationException {
    Assert.assertTrue(recipient != null, "RemoteFetchEntryMessage NULL recipient");
    FetchEntryResponse p = new FetchEntryResponse(r.getSystem(), recipient, r, key);
    RemoteFetchEntryMessage m = new RemoteFetchEntryMessage(recipient, r.getFullPath(), p, key);

    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws RemoteOperationException {
    r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    EntrySnapshot val;
    try {
      final KeyInfo keyInfo = r.getKeyInfo(key);
      Region.Entry<?, ?> re = r.getDataView().getEntry(keyInfo, r, true);
      if (re == null) {
        r.checkEntryNotFound(key);
      }
      NonLocalRegionEntry nlre = new NonLocalRegionEntry(re, r);
      LocalRegion dataReg = r.getDataRegionForRead(keyInfo);
      val = new EntrySnapshot(nlre, dataReg, r, false);
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), val, dm, null);
    } catch (TransactionException tex) {
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, new ReplyException(tex));
    } catch (EntryNotFoundException enfe) {
      FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, new ReplyException(
          "entry not found", enfe));
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }


  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(key);
  }

  @Override
  public int getDSFID() {
    return R_FETCH_ENTRY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    key = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(key, out);
  }

  public void setKey(Object key) {
    this.key = key;
  }

  /**
   * This message is used for the reply to a {@link RemoteFetchEntryMessage}.
   *
   * @since GemFire 5.0
   */
  public static class FetchEntryReplyMessage extends ReplyMessage {
    /** Propagated exception from remote node to operation initiator */
    private EntrySnapshot value;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public FetchEntryReplyMessage() {}

    private FetchEntryReplyMessage(int processorId, EntrySnapshot value, ReplyException re) {
      this.processorId = processorId;
      this.value = value;
      setException(re);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        EntrySnapshot value, DistributionManager dm, ReplyException re) {
      Assert.assertTrue(recipient != null, "FetchEntryReplyMessage NULL recipient");
      FetchEntryReplyMessage m = new FetchEntryReplyMessage(processorId, value, re);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 processor) {
      final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DM_VERBOSE);

      final long startTime = getTimestamp();
      if (isDebugEnabled) {
        logger.trace(LogMarker.DM_VERBOSE,
            "FetchEntryReplyMessage process invoking reply processor with processorId:{}",
            processorId);
      }

      if (processor == null) {
        if (isDebugEnabled) {
          logger.trace(LogMarker.DM_VERBOSE, "FetchEntryReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (isDebugEnabled) {
        logger.trace(LogMarker.DM_VERBOSE, "{}  processed  {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    public EntrySnapshot getValue() {
      return value;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      if (value == null) {
        out.writeBoolean(true); // null entry
      } else {
        out.writeBoolean(false); // null entry
        InternalDataSerializer.invokeToData(value, out);
      }
    }

    @Override
    public int getDSFID() {
      return R_FETCH_ENTRY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      boolean nullEntry = in.readBoolean();
      if (!nullEntry) {
        // EntrySnapshot.setRegion is called later
        value = new EntrySnapshot(in, null);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("FetchEntryReplyMessage ").append("processorid=").append(processorId)
          .append(" reply to sender ").append(getSender()).append(" returning value=")
          .append(value);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.tx.RemoteFetchEntryMessage.FetchEntryReplyMessage}
   *
   */
  public static class FetchEntryResponse extends RemoteOperationResponse {
    private volatile EntrySnapshot returnValue;

    final LocalRegion region;
    final Object key;

    public FetchEntryResponse(InternalDistributedSystem ds, InternalDistributedMember recipient,
        LocalRegion theRegion, Object key) {
      super(ds, recipient);
      region = theRegion;
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof FetchEntryReplyMessage) {
          FetchEntryReplyMessage reply = (FetchEntryReplyMessage) msg;
          returnValue = reply.getValue();
          if (returnValue != null) {
            returnValue.setRegion(region);
          }
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "FetchEntryResponse return value is {}",
                returnValue);
          }
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * @return Object associated with the key that was sent in the get message
     * @throws RemoteOperationException if the peer is no longer available
     */
    public EntrySnapshot waitForResponse() throws EntryNotFoundException, RemoteOperationException {
      try {
        waitForRemoteResponse();
      } catch (EntryNotFoundException | TransactionException e) {
        throw e;
      } catch (CacheException ce) {
        logger.debug("FetchEntryResponse failed with remote CacheException", ce);
        throw new RemoteOperationException("FetchEntryResponse failed with remote CacheException",
            ce);
      }
      return returnValue;
    }
  }

}
