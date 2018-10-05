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
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BucketRegion.RawValue;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.DataLocationException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.util.BlobHelper;

/**
 * This message is used as the request for a get operation done in a transaction that is hosted on a
 * remote member. This messsage sends the get to the remote member.
 *
 * @since GemFire 6.5
 */
public class RemoteGetMessage extends RemoteOperationMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private Object key;

  /** The callback arg of the operation */
  private Object cbArg;

  private ClientProxyMembershipID context;


  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteGetMessage() {}

  private RemoteGetMessage(InternalDistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, final Object key, final Object aCallbackArgument,
      ClientProxyMembershipID context) {
    super(recipient, regionPath, processor);
    this.key = key;
    this.cbArg = aCallbackArgument;
    this.context = context;
  }

  @Override
  protected boolean operateOnRegion(final ClusterDistributionManager dm, LocalRegion r,
      long startTime) throws RemoteOperationException {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "RemoteGetMessage operateOnRegion: {}", r.getFullPath());
    }

    if (this.getTXUniqId() != TXManagerImpl.NOTX) {
      assert r.getDataView() instanceof TXStateProxy;
    }

    r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized

    RawValue valueBytes;
    Object val = null;
    try {
      KeyInfo keyInfo = r.getKeyInfo(key, cbArg);
      val = r.getDataView().getSerializedValue(r, keyInfo, false, this.context, null,
          false /* for replicate regions */);
      valueBytes = val instanceof RawValue ? (RawValue) val : new RawValue(val);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "GetMessage sending serialized value {} back via GetReplyMessage using processorId: {}",
            valueBytes, getProcessorId());
      }

      GetReplyMessage.send(getSender(), getProcessorId(), valueBytes, getReplySender(dm));

      // Unless an exception was thrown, this message handles sending the response
      return false;
    } catch (DistributedSystemDisconnectedException sde) {
      sendReply(getSender(), this.processorId, dm,
          new ReplyException(new RemoteOperationException(
              "Operation got interrupted due to shutdown in progress on remote VM.",
              sde)),
          r, startTime);
      return false;
    } catch (DataLocationException e) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(e), r, startTime);
      return false;
    } finally {
      OffHeapHelper.release(val);
    }

  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; key=").append(this.key).append("; callback arg=").append(this.cbArg);
  }

  public int getDSFID() {
    return R_GET_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.cbArg = DataSerializer.readObject(in);
    this.context = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
    DataSerializer.writeObject(this.cbArg, out);
    DataSerializer.writeObject(this.context, out);
  }

  public void setKey(Object key) {
    this.key = key;
  }

  /**
   * Sends a ReplicateRegion {@link org.apache.geode.cache.Region#get(Object)} message
   *
   * @param recipient the member that the get message is sent to
   * @param r the ReplicateRegion for which get was performed upon
   * @param key the object to which the value should be feteched
   * @param requestingClient the client requesting the value
   * @return the processor used to fetch the returned value associated with the key
   */
  public static RemoteGetResponse send(InternalDistributedMember recipient, LocalRegion r,
      final Object key, final Object aCallbackArgument, ClientProxyMembershipID requestingClient)
      throws RemoteOperationException {
    Assert.assertTrue(recipient != null, "RemoteGetMessage NULL recipient");
    RemoteGetResponse p = new RemoteGetResponse(r.getSystem(), recipient);
    RemoteGetMessage m = new RemoteGetMessage(recipient, r.getFullPath(), p, key, aCallbackArgument,
        requestingClient);
    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  /**
   * This message is used for the reply to a
   * {@link org.apache.geode.cache.Region#get(Object)}operation This is the reply to a
   * {@link RemoteGetMessage}.
   *
   * Since the {@link org.apache.geode.cache.Region#get(Object)}operation is used <bold>very </bold>
   * frequently the performance of this class is critical.
   *
   * @since GemFire 6.5
   */
  public static class GetReplyMessage extends ReplyMessage {
    /**
     * The raw value in the cache which may be serialized to the output stream, if it is NOT already
     * a byte array
     */
    private transient RawValue rawVal;

    /**
     * Indicates that the value already a byte array (aka user blob) and does not need
     * de-serialization
     */
    public boolean valueIsByteArray;

    /*
     * Used on the fromData side to transfer the value bytes to the requesting thread
     */
    public transient byte[] valueInBytes;

    public transient Version remoteVersion;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public GetReplyMessage() {}

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
     * Return the value from the get operation, serialize it bytes as late as possible to avoid
     * making un-neccesary byte[] copies. De-serialize those same bytes as late as possible to avoid
     * using precious threads (aka P2P readers).
     *
     * @param recipient the origin VM that performed the get
     * @param processorId the processor on which the origin thread is waiting
     * @param val the raw value that will eventually be serialized
     * @param replySender distribution manager used to send the reply
     */
    public static void send(InternalDistributedMember recipient, int processorId, RawValue val,
        ReplySender replySender) throws RemoteOperationException {
      Assert.assertTrue(recipient != null, "PRDistribuedGetReplyMessage NULL reply message");
      GetReplyMessage m = new GetReplyMessage(processorId, val);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, ReplyProcessor21 processor) {
      final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DM_VERBOSE);

      final long startTime = getTimestamp();
      if (isDebugEnabled) {
        logger.trace(LogMarker.DM_VERBOSE,
            "GetReplyMessage process invoking reply processor with processorId:{}",
            this.processorId);
      }

      if (processor == null) {
        if (isDebugEnabled) {
          logger.trace(LogMarker.DM_VERBOSE, "GetReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (isDebugEnabled) {
        logger.trace(LogMarker.DM_VERBOSE, "{} Processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_GET_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeByte(this.valueIsByteArray ? 1 : 0);
      this.rawVal.writeAsByteArray(out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.valueIsByteArray = (in.readByte() == 1);
      this.valueInBytes = DataSerializer.readByteArray(in);
      if (!this.valueIsByteArray) {
        this.remoteVersion = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      }
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("GetReplyMessage ").append("processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender())
          .append(" returning serialized value=").append(this.rawVal);
      return sb.toString();
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.tx.RemoteGetMessage.GetReplyMessage}
   *
   * @since GemFire 5.0
   */
  public static class RemoteGetResponse extends RemoteOperationResponse {

    private volatile GetReplyMessage getReply;
    private volatile boolean returnValueReceived;
    private volatile long start;

    public RemoteGetResponse(InternalDistributedSystem ds, InternalDistributedMember recipient) {
      super(ds, recipient, false);
    }

    @Override
    public void process(DistributionMessage msg) {
      if (DistributionStats.enableClockStats) {
        this.start = DistributionStats.getStatTime();
      }
      if (msg instanceof GetReplyMessage) {
        GetReplyMessage reply = (GetReplyMessage) msg;
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
     * De-seralize the value, if the value isn't already a byte array, this method should be called
     * in the context of the requesting thread for the best scalability
     *
     * @see EntryEventImpl#deserialize(byte[])
     * @return the value object
     */
    public Object getValue(boolean preferCD) throws RemoteOperationException {
      final GetReplyMessage reply = this.getReply;
      try {
        if (reply != null) {
          if (reply.valueIsByteArray) {
            return reply.valueInBytes;
          } else if (preferCD) {
            return CachedDeserializableFactory.create(reply.valueInBytes,
                getDistributionManager().getCache());
          } else {
            return BlobHelper.deserializeBlob(reply.valueInBytes, reply.remoteVersion, null);
          }
        }
        return null;
      } catch (IOException e) {
        throw new RemoteOperationException(
            "Unable to deserialize value (IOException)",
            e);
      } catch (ClassNotFoundException e) {
        throw new RemoteOperationException(
            "Unable to deserialize value (ClassNotFoundException)",
            e);
      }
    }


    /**
     * @return Object associated with the key that was sent in the get message
     */
    public Object waitForResponse(boolean preferCD) throws RemoteOperationException {
      waitForRemoteResponse();
      if (DistributionStats.enableClockStats) {
        getDistributionManager().getStats().incReplyHandOffTime(this.start);
      }
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(
            "no return value received");
      }
      return getValue(preferCD);
    }
  }

}
