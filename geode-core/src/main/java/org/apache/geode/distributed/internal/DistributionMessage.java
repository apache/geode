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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.deadlock.MessageDependencyMonitor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.sequencelog.MessageLogger;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * <P>
 * A <code>DistributionMessage</code> carries some piece of information to a distribution manager.
 * </P>
 *
 * <P>
 * Messages that don't have strict ordering requirements should extend
 * {@link org.apache.geode.distributed.internal.PooledDistributionMessage}. Messages that must be
 * processed serially in the order they were received can extend
 * {@link org.apache.geode.distributed.internal.SerialDistributionMessage}. To customize the
 * sequentialness/thread requirements of a message, extend DistributionMessage and implement
 * getExecutor().
 * </P>
 */
public abstract class DistributionMessage
    implements Message<InternalDistributedMember>, Cloneable {

  /**
   * WARNING: setting this to true may break dunit tests.
   * <p>
   * see org.apache.geode.cache30.ClearMultiVmCallBkDUnitTest
   */
  private static final boolean INLINE_PROCESS =
      !Boolean.getBoolean("DistributionManager.enqueueOrderedMessages");

  private static final Logger logger = LogService.getLogger();

  @Immutable
  protected static final InternalDistributedMember ALL_RECIPIENTS = null;

  @Immutable
  private static final InternalDistributedMember[] ALL_RECIPIENTS_ARRAY = {null};

  @Immutable
  private static final InternalDistributedMember[] EMPTY_RECIPIENTS_ARRAY =
      new InternalDistributedMember[0];

  @Immutable
  private static final List<InternalDistributedMember> ALL_RECIPIENTS_LIST =
      Collections.singletonList(null);

  // common flags used by operation messages
  /** Keep this compatible with the other GFE layer PROCESSOR_ID flags. */
  protected static final short HAS_PROCESSOR_ID = 0x1;
  /** Flag set when this message carries a transactional member in context. */
  protected static final short HAS_TX_MEMBERID = 0x2;
  /** Flag set when this message carries a transactional context. */
  protected static final short HAS_TX_ID = 0x4;
  /** Flag set when this message is a possible duplicate. */
  protected static final short POS_DUP = 0x8;
  /** Indicate time statistics capturing as part of this message processing */
  protected static final short ENABLE_TIMESTATS = 0x10;
  /** If message sender has set the processor type to be used explicitly. */
  protected static final short HAS_PROCESSOR_TYPE = 0x20;

  /** the unreserved flags start for child classes */
  protected static final short UNRESERVED_FLAGS_START = (HAS_PROCESSOR_TYPE << 1);

  //////////////////// Instance Fields ////////////////////

  /** The sender of this message */
  protected transient InternalDistributedMember sender;

  /** A set of recipients for this message, not serialized */
  private transient InternalDistributedMember[] recipients = null;

  /** A timestamp, in nanos, associated with this message. Not serialized. */
  private transient long timeStamp;

  /** The number of bytes used to read this message, for statistics only */
  private transient int bytesRead = 0;

  /** true if message should be multicast; ignores recipients */
  private transient boolean multicast = false;

  /** true if messageBeingReceived stats need decrementing when done with msg */
  private transient boolean doDecMessagesBeingReceived = false;

  /**
   * This field will be set if we can send a direct ack for this message.
   */
  private transient ReplySender acker = null;

  /**
   * True if the P2P reader that received this message is a SHARED reader.
   */
  private transient boolean sharedReceiver;

  ////////////////////// Constructors //////////////////////

  protected DistributionMessage() {
    this.timeStamp = DistributionStats.getStatTime();
  }

  ////////////////////// Static Helper Methods //////////////////////

  /**
   * Get the next bit mask position while checking that the value should not exceed maximum byte
   * value.
   */
  protected static int getNextByteMask(final int mask) {
    return getNextBitMask(mask, (Byte.MAX_VALUE) + 1);
  }

  /**
   * Get the next bit mask position while checking that the value should not exceed given maximum
   * value.
   */
  protected static int getNextBitMask(int mask, final int maxValue) {
    mask <<= 1;
    if (mask > maxValue) {
      Assert.fail("exhausted bit flags with all available bits: 0x" + Integer.toHexString(mask)
          + ", max: 0x" + Integer.toHexString(maxValue));
    }
    return mask;
  }

  public static byte getNumBits(final int maxValue) {
    byte numBits = 1;
    while ((1 << numBits) <= maxValue) {
      numBits++;
    }
    return numBits;
  }

  ////////////////////// Instance Methods //////////////////////

  public void setDoDecMessagesBeingReceived(boolean v) {
    this.doDecMessagesBeingReceived = v;
  }

  public void setReplySender(ReplySender acker) {
    this.acker = acker;
  }

  public ReplySender getReplySender(DistributionManager dm) {
    if (acker != null) {
      return acker;
    } else {
      return dm;
    }
  }

  public boolean isDirectAck() {
    return acker != null;
  }

  /**
   * If true then this message most be sent on an ordered channel. If false then it can be
   * unordered.
   *
   * @since GemFire 5.5
   */
  public boolean orderedDelivery() {
    final int processorType = getProcessorType();
    switch (processorType) {
      case OperationExecutors.SERIAL_EXECUTOR:
        // no need to use orderedDelivery for PR ops particularly when thread
        // does not own resources
        // case DistributionManager.PARTITIONED_REGION_EXECUTOR:
        return true;
      case OperationExecutors.REGION_FUNCTION_EXECUTION_EXECUTOR:
        // allow nested distributed functions to be executed from within the
        // execution of a function
        return false;
      default:
        InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
        return (ids != null && ids.threadOwnsResources());
    }
  }

  /**
   * Sets the intended recipient of the message. If recipient is Message.ALL_RECIPIENTS
   * then the
   * message will be sent to all distribution managers.
   */
  public void setRecipient(InternalDistributedMember recipient) {
    if (this.recipients != null) {
      throw new IllegalStateException(
          "Recipients can only be set once");
    }
    this.recipients = new InternalDistributedMember[] {recipient};
  }

  /**
   * Causes this message to be send using multicast if v is true.
   *
   * @since GemFire 5.0
   */
  public void setMulticast(boolean v) {
    this.multicast = v;
  }

  /**
   * Return true if this message should be sent using multicast.
   *
   * @since GemFire 5.0
   */
  public boolean getMulticast() {
    return this.multicast;
  }

  /**
   * Return true of this message should be sent via UDP instead of the direct-channel. This is
   * typically only done for messages that are broadcast to the full membership set.
   */
  public boolean sendViaUDP() {
    return false;
  }

  /**
   * Sets the intended recipient of the message. If recipient set contains
   * Message.ALL_RECIPIENTS
   * then the message will be sent to all distribution managers.
   */
  @Override
  public void setRecipients(Collection recipients) {
    this.recipients = (InternalDistributedMember[]) recipients
        .toArray(EMPTY_RECIPIENTS_ARRAY);
  }

  @Override
  public void registerProcessor() {
    // override if direct-ack is supported
  }

  @Override
  public boolean isHighPriority() {
    return false;
  }

  @Override
  public List<InternalDistributedMember> getRecipients() {
    InternalDistributedMember[] recipients = getRecipientsArray();
    if (recipients == null
        || recipients.length == 1 && recipients[0] == ALL_RECIPIENTS) {
      return ALL_RECIPIENTS_LIST;
    }
    return Arrays.asList(recipients);
  }


  public void resetRecipients() {
    this.recipients = null;
    this.multicast = false;
  }


  /**
   * Returns the intended recipient(s) of this message. If the message is intended to delivered to
   * all distribution managers, then the array will contain ALL_RECIPIENTS. If the recipients have
   * not been set null is returned.
   */
  public InternalDistributedMember[] getRecipientsArray() {
    if (this.multicast || this.recipients == null) {
      return ALL_RECIPIENTS_ARRAY;
    }
    return this.recipients;
  }

  /**
   * Returns true if message will be sent to everyone.
   */
  public boolean forAll() {
    return (this.recipients == null) || (this.multicast)
        || ((this.recipients.length > 0) && (this.recipients[0] == ALL_RECIPIENTS));
  }

  public String getRecipientsDescription() {
    if (forAll()) {
      return "recipients: ALL";
    } else {
      StringBuffer sb = new StringBuffer(100);
      sb.append("recipients: <");
      for (int i = 0; i < this.recipients.length; i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append(this.recipients[i]);
      }
      sb.append(">");
      return sb.toString();
    }
  }

  /**
   * Returns the sender of this message. Note that this value is not set until this message is
   * received by a distribution manager.
   */
  public InternalDistributedMember getSender() {
    return this.sender;
  }

  /**
   * Sets the sender of this message. This method is only invoked when the message is
   * <B>received</B> by a <code>DistributionManager</code>.
   */
  @Override
  public void setSender(InternalDistributedMember _sender) {
    this.sender = _sender;
  }

  /**
   * Return the Executor in which to process this message.
   */
  protected Executor getExecutor(ClusterDistributionManager dm) {
    return dm.getExecutors().getExecutor(getProcessorType(), sender);
  }

  public abstract int getProcessorType();

  /**
   * Processes this message. This method is invoked by the receiver of the message.
   *
   * @param dm the distribution manager that is processing the message.
   */
  protected abstract void process(ClusterDistributionManager dm);

  /**
   * Scheduled action to take when on this message when we are ready to process it.
   */
  protected void scheduleAction(final ClusterDistributionManager dm) {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "Processing '{}'", this);
    }
    String reason = dm.getCancelCriterion().cancelInProgress();
    if (reason != null) {
      // throw new ShutdownException(reason);
      if (logger.isDebugEnabled()) {
        logger.debug("ScheduleAction: cancel in progress ({}); skipping<{}>", reason, this);
      }
      return;
    }
    if (MessageLogger.isEnabled()) {
      MessageLogger.logMessage(this, getSender(), dm.getDistributionManagerId());
    }
    MessageDependencyMonitor.processingMessage(this);
    long time = 0;
    if (DistributionStats.enableClockStats) {
      time = DistributionStats.getStatTime();
      dm.getStats().incMessageProcessingScheduleTime(time - getTimestamp());
    }
    setBreadcrumbsInReceiver();
    try {

      DistributionMessageObserver observer = DistributionMessageObserver.getInstance();
      if (observer != null) {
        observer.beforeProcessMessage(dm, this);
      }
      process(dm);
      if (observer != null) {
        observer.afterProcessMessage(dm, this);
      }
    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Cancelled caught processing {}: {}", this, e.getMessage(), e);
      }
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal(String.format("Uncaught exception processing %s", this), t);
    } finally {
      if (doDecMessagesBeingReceived) {
        dm.getStats().decMessagesBeingReceived(this.bytesRead);
      }
      dm.getStats().incProcessedMessages(1L);
      if (DistributionStats.enableClockStats) {
        dm.getStats().incProcessedMessagesTime(time);
      }
      Breadcrumbs.clearBreadcrumb();
      MessageDependencyMonitor.doneProcessing(this);
    }
  }

  /**
   * Schedule this message's process() method in a thread determined by getExecutor()
   */
  protected void schedule(final ClusterDistributionManager dm) {
    boolean inlineProcess = INLINE_PROCESS
        && getProcessorType() == OperationExecutors.SERIAL_EXECUTOR && !isPreciousThread();

    boolean forceInline = this.acker != null || getInlineProcess() || Connection.isDominoThread();

    if (inlineProcess && !forceInline && isSharedReceiver()) {
      // If processing this message notify a serial gateway sender then don't do it inline.
      if (mayNotifySerialGatewaySender(dm)) {
        inlineProcess = false;
      }
    }

    inlineProcess |= forceInline;

    if (inlineProcess) {
      dm.getStats().incNumSerialThreads(1);
      try {
        scheduleAction(dm);
      } finally {
        dm.getStats().incNumSerialThreads(-1);
      }
    } else { // not inline
      try {
        getExecutor(dm).execute(new SizeableRunnable(this.getBytesRead()) {
          @Override
          public void run() {
            scheduleAction(dm);
          }

          @Override
          public String toString() {
            return "Processing {" + DistributionMessage.this.toString() + "}";
          }
        });
      } catch (RejectedExecutionException ex) {
        if (!dm.shutdownInProgress()) { // fix for bug 32395
          logger.warn(String.format("%s schedule() rejected", this.toString()), ex);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.fatal(String.format("Uncaught exception processing %s", this), t);
        // I don't believe this ever happens (DJP May 2007)
        throw new InternalGemFireException(
            "Unexpected error scheduling message",
            t);
      }
    } // not inline
  }

  protected boolean mayNotifySerialGatewaySender(ClusterDistributionManager dm) {
    // subclasses should override this method if processing them may notify a serial gateway sender.
    return false;
  }

  /**
   * returns true if the current thread should not be used for inline processing. i.e., it is a
   * "precious" resource
   */
  public static boolean isPreciousThread() {
    String thrname = Thread.currentThread().getName();
    // return thrname.startsWith("Geode UDP");
    return thrname.startsWith("unicast receiver") || thrname.startsWith("multicast receiver");
  }


  /** most messages should not force in-line processing */
  public boolean getInlineProcess() {
    return false;
  }

  /**
   * sets the breadcrumbs for this message into the current thread's name
   */
  public void setBreadcrumbsInReceiver() {
    if (Breadcrumbs.ENABLED) {
      String sender = null;
      String procId = "";
      long pid = getProcessorId();
      if (pid != 0) {
        procId = " processorId=" + pid;
      }
      if (Thread.currentThread().getName().startsWith(Connection.THREAD_KIND_IDENTIFIER)) {
        sender = procId;
      } else {
        sender = "sender=" + getSender() + procId;
      }
      if (sender.length() > 0) {
        Breadcrumbs.setReceiveSide(sender);
      }
      Object evID = getEventID();
      if (evID != null) {
        Breadcrumbs.setEventId(evID);
      }
    }
  }

  /**
   * sets breadcrumbs in a thread that is sending a message to another member
   */
  public void setBreadcrumbsInSender() {
    if (Breadcrumbs.ENABLED) {
      String procId = "";
      long pid = getProcessorId();
      if (pid != 0) {
        procId = "processorId=" + pid;
      }
      if (this.recipients != null && this.recipients.length <= 10) { // set a limit on recipients
        Breadcrumbs.setSendSide(procId + " recipients=" + Arrays.toString(this.recipients));
      } else {
        if (procId.length() > 0) {
          Breadcrumbs.setSendSide(procId);
        }
      }
      Object evID = getEventID();
      if (evID != null) {
        Breadcrumbs.setEventId(evID);
      }
    }
  }

  public EventID getEventID() {
    return null;
  }

  /**
   * This method resets the state of this message, usually releasing objects and resources it was
   * using. It is invoked after the message has been sent. Note that classes that override this
   * method should always invoke the inherited method (<code>super.reset()</code>).
   */
  public void reset() {
    resetRecipients();
    this.sender = null;
  }

  /**
   * Writes the contents of this <code>DistributionMessage</code> to the given output. Note that
   * classes that override this method should always invoke the inherited method
   * (<code>super.toData()</code>).
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    // context.getSerializer().writeObject(this.recipients, out); // no need to serialize; filled in
    // later
    // ((IpAddress)this.sender).toData(out); // no need to serialize; filled in later
    // out.writeLong(this.timeStamp);
  }

  /**
   * Reads the contents of this <code>DistributionMessage</code> from the given input. Note that
   * classes that override this method should always invoke the inherited method
   * (<code>super.fromData()</code>).
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {

    // this.recipients = (Set)context.getDeserializer().readObject(in); // no to deserialize; filled
    // in later
    // this.sender = DataSerializer.readIpAddress(in); // no to deserialize; filled in later
    // this.timeStamp = (long)in.readLong();
  }

  /**
   * Returns a timestamp, in nanos, associated with this message.
   */
  public long getTimestamp() {
    return timeStamp;
  }

  /**
   * Sets the timestamp of this message to the current time (in nanos).
   *
   * @return the number of elapsed nanos since this message's last timestamp
   */
  public long resetTimestamp() {
    if (DistributionStats.enableClockStats) {
      long now = DistributionStats.getStatTime();
      long result = now - this.timeStamp;
      this.timeStamp = now;
      return result;
    } else {
      return 0;
    }
  }

  public void setBytesRead(int bytesRead) {
    this.bytesRead = bytesRead;
  }

  public int getBytesRead() {
    return bytesRead;
  }

  public void setSharedReceiver(boolean v) {
    this.sharedReceiver = v;
  }

  public boolean isSharedReceiver() {
    return this.sharedReceiver;
  }

  /**
   *
   * @return null if message is not conflatable. Otherwise return a key that can be used to identify
   *         the entry to conflate.
   * @since GemFire 4.2.2
   */
  public ConflationKey getConflationKey() {
    return null; // by default conflate nothing; override in subclasses
  }

  /**
   * @return the ID of the reply processor for this message, or zero if none
   * @since GemFire 5.7
   */
  public int getProcessorId() {
    return 0;
  }

  /**
   * Severe alert processing enables suspect processing at the ack-wait-threshold and issuing of a
   * severe alert at the end of the ack-severe-alert-threshold. Some messages should not support
   * this type of processing (e.g., GII, or DLockRequests)
   *
   * @return whether severe-alert processing may be performed on behalf of this message
   */
  public boolean isSevereAlertCompatible() {
    return false;
  }

  /**
   * Returns true if the message is for internal-use such as a meta-data region.
   *
   * @return true if the message is for internal-use such as a meta-data region
   * @since GemFire 7.0
   */
  public boolean isInternal() {
    return false;
  }

  @Override
  public boolean dropMessageWhenMembershipIsPlayingDead() {
    return containsRegionContentChange();
  }

  /**
   * does this message carry state that will alter the content of one or more cache regions? This is
   * used to track the flight of content changes through communication channels
   */
  public boolean containsRegionContentChange() {
    return false;
  }

  /** returns the class name w/o package information. useful in logging */
  public String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  @Override
  public String toString() {
    String cname = getShortClassName();
    final StringBuilder sb = new StringBuilder(cname);
    sb.append('@').append(Integer.toHexString(System.identityHashCode(this)));
    sb.append(" processorId=").append(getProcessorId());
    sb.append(" sender=").append(getSender());
    return sb.toString();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
