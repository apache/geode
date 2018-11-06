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
package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
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
import org.apache.geode.internal.cache.DataLocationException;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * This message is used as the request for a
 * {@link org.apache.geode.cache.Region#getEntry(Object)}operation. The reply is sent in a
 * {@link org.apache.geode.internal.cache.partitioned.FetchEntryMessage.FetchEntryReplyMessage}.
 *
 * @since GemFire 5.1
 */
public class FetchEntryMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  private Object key;
  private boolean access;

  // reusing an unused flag for HAS_ACCESS
  protected static final int HAS_ACCESS = HAS_FILTER_INFO;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public FetchEntryMessage() {}

  private FetchEntryMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor, final Object key, boolean access) {
    super(recipient, regionId, processor);
    this.key = key;
    this.access = access;
  }

  /**
   * Sends a PartitionedRegion {@link org.apache.geode.cache.Region#getEntry(Object)} message
   *
   * @param recipient the member that the getEntry message is sent to
   * @param r the PartitionedRegion for which getEntry was performed upon
   * @param key the object to which the value should be feteched
   * @return the processor used to fetch the returned value associated with the key
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static FetchEntryResponse send(InternalDistributedMember recipient, PartitionedRegion r,
      final Object key, boolean access) throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "FetchEntryMessage NULL recipient");
    FetchEntryResponse p =
        new FetchEntryResponse(r.getSystem(), Collections.singleton(recipient), r, key);
    FetchEntryMessage m = new FetchEntryMessage(recipient, r.getPRId(), p, key, access);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  public FetchEntryMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws ForceReattemptException {
    // FetchEntryMessage is used in refreshing client caches during interest list recovery,
    // so don't be too verbose or hydra tasks may time out

    PartitionedRegionDataStore ds = r.getDataStore();
    EntrySnapshot val;
    if (ds != null) {
      try {
        KeyInfo keyInfo = r.getKeyInfo(key);
        val = (EntrySnapshot) r.getDataView().getEntryOnRemote(keyInfo, r, true);
        r.getPrStats().endPartitionMessagesProcessing(startTime);
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), val, dm, null);
      } catch (TransactionException tex) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm,
            new ReplyException(tex));
      } catch (PRLocallyDestroyedException pde) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm,
            new ReplyException(new ForceReattemptException(
                "Encountered PRLocallyDestroyed",
                pde)));
      } catch (EntryNotFoundException enfe) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, new ReplyException(
            "entry not found", enfe));
      } catch (PrimaryBucketException pbe) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm,
            new ReplyException(pbe));
      } catch (ForceReattemptException pbe) {
        pbe.checkKey(key);
        // Slightly odd -- we're marshalling the retry to the peer on another host...
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm,
            new ReplyException(pbe));
      } catch (DataLocationException e) {
        FetchEntryReplyMessage.send(getSender(), getProcessorId(), null, dm, new ReplyException(e));
      }
    } else {
      throw new InternalGemFireError(
          "FetchEntryMessage message sent to wrong member");
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(this.key);
  }

  public int getDSFID() {
    return PR_FETCH_ENTRY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
  }

  @Override
  protected void setBooleans(short s, DataInput in) throws IOException, ClassNotFoundException {
    super.setBooleans(s, in);
    this.access = ((s & HAS_ACCESS) != 0);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (this.access)
      s |= HAS_ACCESS;
    return s;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  /**
   * This message is used for the reply to a {@link FetchEntryMessage}.
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

    public FetchEntryReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in);
    }

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
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "FetchEntryReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "FetchEntryReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    public EntrySnapshot getValue() {
      return this.value;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      if (this.value == null) {
        out.writeBoolean(true); // null entry
      } else {
        out.writeBoolean(false); // null entry
        InternalDataSerializer.invokeToData(this.value, out);
      }
    }

    @Override
    public int getDSFID() {
      return PR_FETCH_ENTRY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      boolean nullEntry = in.readBoolean();
      if (!nullEntry) {
        // since the Entry object shares state with the PartitionedRegion,
        // we have to find the region and ask it to create a new Entry instance
        // to be populated from the DataInput
        FetchEntryResponse processor =
            (FetchEntryResponse) ReplyProcessor21.getProcessor(this.processorId);
        if (processor == null) {
          throw new OperationCancelledException("This operation was cancelled (null processor)");
        }
        this.value = new EntrySnapshot(in, processor.partitionedRegion);
      }
    }

    @Override
    public StringBuilder getStringBuilder() {
      StringBuilder sb = super.getStringBuilder();
      if (getException() == null) {
        sb.append(" returning value=").append(this.value);
      }
      return sb;
    }
  }

  /**
   * A processor to capture the value returned by {@link FetchEntryMessage.FetchEntryReplyMessage}
   */
  public static class FetchEntryResponse extends PartitionResponse {
    private volatile EntrySnapshot returnValue;

    final PartitionedRegion partitionedRegion;
    final Object key;

    public FetchEntryResponse(InternalDistributedSystem ds, Set recipients,
        PartitionedRegion theRegion, Object key) {
      super(ds, recipients);
      partitionedRegion = theRegion;
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof FetchEntryReplyMessage) {
          FetchEntryReplyMessage reply = (FetchEntryReplyMessage) msg;
          this.returnValue = reply.getValue();
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "FetchEntryResponse return value is {}",
                this.returnValue);
          }
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * @return Object associated with the key that was sent in the get message
     * @throws ForceReattemptException if the peer is no longer available
     */
    public EntrySnapshot waitForResponse() throws EntryNotFoundException, ForceReattemptException {
      try {
        waitForCacheException();
      } catch (ForceReattemptException e) {
        e.checkKey(key);
        final String msg = "FetchEntryResponse got remote ForceReattemptException; rethrowing";
        logger.debug(msg, e);
        throw e;
      } catch (EntryNotFoundException | TransactionException e) {
        throw e;
      } catch (CacheException ce) {
        logger.debug("FetchEntryResponse got remote CacheException; forcing reattempt.", ce);
        throw new ForceReattemptException(
            "FetchEntryResponse got remote CacheException; forcing reattempt.",
            ce);
      }
      return this.returnValue;
    }
  }

}
