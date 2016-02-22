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
package com.gemstone.gemfire.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.deadlock.MessageDependencyMonitor;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.sequencelog.MessageLogger;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.internal.util.Breadcrumbs;

/**
 * <P>A <code>DistributionMessage</code> carries some piece of
 * information to a distribution manager.  </P>
 *
 * <P>Messages that don't have strict ordering requirements should extend
 * {@link com.gemstone.gemfire.distributed.internal.PooledDistributionMessage}.
 * Messages that must be processed serially in the order they were received
 * can extend
 * {@link com.gemstone.gemfire.distributed.internal.SerialDistributionMessage}.
 * To customize the sequentialness/thread requirements of a message, extend
 * DistributionMessage and implement getExecutor().</P>
 *
 *
 */
public abstract class DistributionMessage
  implements DataSerializableFixedID, Cloneable {
  
  private static final Logger logger = LogService.getLogger();
  
  /** Indicates that a distribution message should be sent to all
   * other distribution managers. */
  public static final InternalDistributedMember ALL_RECIPIENTS = null;

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
  protected static final short UNRESERVED_FLAGS_START =
    (HAS_PROCESSOR_TYPE << 1);

  ////////////////////  Instance Fields  ////////////////////

  /** The sender of this message */
  protected transient InternalDistributedMember sender;

  /** A set of recipients for this message, not serialized*/
  private transient InternalDistributedMember[] recipients = null;

  /** A timestamp, in nanos, associated with this message. Not serialized. */
  private transient long timeStamp;

  /** The number of bytes used to read this message,  for statistics only */
  private transient int bytesRead = 0;

  /** true if message should be multicast; ignores recipients */
  private transient boolean multicast = false;

  /** true if messageBeingReceived stats need decrementing when done with msg */
  private transient boolean doDecMessagesBeingReceived = false;
  
  /**
   * This field will be set if we can send a direct ack for
   * this message.
   */
  private transient ReplySender acker = null;
  
  /**
   * True if the P2P reader that received this message is a SHARED reader.
   */
  private transient boolean sharedReceiver;
  
  //////////////////////  Constructors  //////////////////////

  protected DistributionMessage() {
    this.timeStamp = DistributionStats.getStatTime();
  }

  //////////////////////  Static Helper Methods  //////////////////////

  /**
   * Get the next bit mask position while checking that the value should not
   * exceed maximum byte value.
   */
  protected static int getNextByteMask(final int mask) {
    return getNextBitMask(mask, (Byte.MAX_VALUE) + 1);
  }

  /**
   * Get the next bit mask position while checking that the value should not
   * exceed given maximum value.
   */
  protected static final int getNextBitMask(int mask, final int maxValue) {
    mask <<= 1;
    if (mask > maxValue) {
      Assert.fail("exhausted bit flags with all available bits: 0x"
          + Integer.toHexString(mask) + ", max: 0x"
          + Integer.toHexString(maxValue));
    }
    return mask;
  }

  public static final byte getNumBits(final int maxValue) {
    byte numBits = 1;
    while ((1 << numBits) <= maxValue) {
      numBits++;
    }
    return numBits;
  }

  //////////////////////  Instance Methods  //////////////////////

  public void setDoDecMessagesBeingReceived(boolean v) {
    this.doDecMessagesBeingReceived = v;
  }
  
  public final void setReplySender(ReplySender acker) {
    this.acker = acker;
  }
  
  public ReplySender getReplySender(DM dm) {
    if(acker != null) {
      return acker;
    } else {
      return dm;
    }
  } 
  
  public boolean isDirectAck() {
    return acker != null;
  }

  /**
   * If true then this message most be sent on an ordered channel.
   * If false then it can be unordered.
   * @since 5.5 
   */
  public boolean orderedDelivery() {
    final int processorType = getProcessorType();
    switch (processorType) {
      case DistributionManager.SERIAL_EXECUTOR:
      // no need to use orderedDelivery for PR ops particularly when thread
      // does not own resources
      //case DistributionManager.PARTITIONED_REGION_EXECUTOR:
        return true;
      case DistributionManager.REGION_FUNCTION_EXECUTION_EXECUTOR:
        // allow nested distributed functions to be executed from within the
        // execution of a function; this is required particularly for SQLFabric
        // TODO: this can later be adjusted to use a separate property
        return false;
      default:
        InternalDistributedSystem ids = InternalDistributedSystem
            .getAnyInstance();
        return (ids != null && ids.threadOwnsResources());
    }
  }

  /**
   * Sets the intended recipient of the message.  If recipient is
   * {@link #ALL_RECIPIENTS} then the message will be sent to all
   * distribution managers.
   */
  public void setRecipient(InternalDistributedMember recipient) {
    if (this.recipients != null) {
       throw new IllegalStateException(LocalizedStrings.DistributionMessage_RECIPIENTS_CAN_ONLY_BE_SET_ONCE.toLocalizedString());
    }
    this.recipients = new InternalDistributedMember[] {recipient};
  }

  /**
   * Causes this message to be send using multicast if v is true.
   * @since 5.0
   */
  public void setMulticast(boolean v) {
    this.multicast = v;
  }
  /**
   * Return true if this message should be sent using multicast.
   * @since 5.0
   */
  public boolean getMulticast() {
    return this.multicast;
  }
  /**
   * Return true of this message should be sent via UDP instead of the
   * direct-channel.  This is typically only done for messages that are
   * broadcast to the full membership set.
   */
  public boolean sendViaUDP() {
    return false;
  }
  /**
   * Sets the intended recipient of the message.  If recipient set contains
   * {@link #ALL_RECIPIENTS} then the message will be sent to all
   * distribution managers.
   */
  public void setRecipients(Collection recipients) {
    if (this.recipients != null) {
       throw new IllegalStateException(LocalizedStrings.DistributionMessage_RECIPIENTS_CAN_ONLY_BE_SET_ONCE.toLocalizedString());
    }
    this.recipients = (InternalDistributedMember[])recipients.toArray(new InternalDistributedMember[recipients.size()]);
  }

  public void resetRecipients() {
    this.recipients = null;
    this.multicast = false;
  }

  public Set getSuccessfulRecipients() {
    // note we can't use getRecipients() for plannedRecipients because it will
    // return ALL_RECIPIENTS if multicast
    InternalDistributedMember[] plannedRecipients = this.recipients;
    Set successfulRecipients = new HashSet(Arrays.asList(plannedRecipients));
    return successfulRecipients;
  }
  
  /**
   * Returns the intended recipient(s) of this message.  If the message
   * is intended to delivered to all distribution managers, then
   * the array will contain ALL_RECIPIENTS.
   * If the recipients have not been set null is returned.
   */
  public InternalDistributedMember[] getRecipients() {
    if (this.multicast) {
      return new InternalDistributedMember[] {ALL_RECIPIENTS};
    }else if (this.recipients != null) {
      return this.recipients;
    } else {
      return new InternalDistributedMember[] {ALL_RECIPIENTS};
    }
  }
  /**
   * Returns true if message will be sent to everyone.
   */
  public boolean forAll() {
    return (this.recipients == null)
      || (this.multicast)
      || ((this.recipients.length > 0)
          && (this.recipients[0] == ALL_RECIPIENTS));
  }

  public String getRecipientsDescription() {
    if (this.recipients == null) {
      return "recipients: ALL";
    }
    else if (this.multicast) {
      return "recipients: multicast";
    } else if (this.recipients.length > 0 && this.recipients[0] == ALL_RECIPIENTS) {
      return "recipients: ALL";
    } else {
      StringBuffer sb = new StringBuffer(100);
      sb.append("recipients: <");
      for (int i=0; i < this.recipients.length; i++) {
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
   * Returns the sender of this message.  Note that this value is not
   * set until this message is received by a distribution manager.
   */
  public InternalDistributedMember getSender() {
    return this.sender;
  }

  /**
   * Sets the sender of this message.  This method is only invoked
   * when the message is <B>received</B> by a
   * <code>DistributionManager</code>.
   */
  public void setSender(InternalDistributedMember _sender) {
    this.sender = _sender;
  }

  /**
   * Return the Executor in which to process this message.
   */
  protected Executor getExecutor(DistributionManager dm) {
    return dm.getExecutor(getProcessorType(), sender);
  }
  
//  private Executor getExecutor(DistributionManager dm, Class clazz) {
//    return dm.getExecutor(getProcessorType());
//  }

  public abstract int getProcessorType();

  /**
   * Processes this message.  This method is invoked by the receiver
   * of the message.
   * @param dm the distribution manager that is processing the message.
   */
  protected abstract void process(DistributionManager dm);
  
  /**
   * Scheduled action to take when on this message when we are ready
   * to process it.
   */
  protected final void scheduleAction(final DistributionManager dm) {
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "Processing '{}'", this);
    }
    String reason = dm.getCancelCriterion().cancelInProgress();
    if (reason != null) {
      // throw new ShutdownException(reason);
      if (logger.isDebugEnabled()) {
        logger.debug("ScheduleAction: cancel in progress ({}); skipping<{}>", reason, this);
      }
      return;
    }
    if(MessageLogger.isEnabled()) {
      MessageLogger.logMessage(this, getSender(),  dm.getDistributionManagerId());
    }
    MessageDependencyMonitor.processingMessage(this);
    long time = 0;
    if (DistributionStats.enableClockStats) {
      time = DistributionStats.getStatTime();
      dm.getStats().incMessageProcessingScheduleTime(time-getTimestamp());
    }
    setBreadcrumbsInReceiver();
    try {
      
      DistributionMessageObserver observer = DistributionMessageObserver.getInstance();
      if(observer != null) {
        observer.beforeProcessMessage(dm, this);
      }
      process(dm);
      if(observer != null) { 
        observer.afterProcessMessage(dm, this);
      }
    }
    catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Cancelled caught processing {}: {}", this, e.getMessage(), e);
      }
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionMessage_UNCAUGHT_EXCEPTION_PROCESSING__0, this), t);
    }
    finally {
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
   * Schedule this message's process() method in a thread determined
   * by getExecutor()
   */
  protected final void schedule(final DistributionManager dm) {
    boolean inlineProcess = DistributionManager.INLINE_PROCESS
      && getProcessorType() == DistributionManager.SERIAL_EXECUTOR
      && !isPreciousThread();
    
    boolean forceInline = this.acker != null || getInlineProcess() || Connection.isDominoThread();
    
    if (inlineProcess && !forceInline && isSharedReceiver()) {
      // If processing this message may need to add
      // to more than one serial gateway then don't
      // do it inline.
      if (mayAddToMultipleSerialGateways(dm)) {
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
          public void run() {
            scheduleAction(dm);
          }

          @Override
          public String toString() {
            return "Processing {" + DistributionMessage.this.toString() + "}";
          }
        });
      }
      catch (RejectedExecutionException ex) {
        if (!dm.shutdownInProgress()) { // fix for bug 32395
          logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionMessage_0__SCHEDULE_REJECTED, this.toString()), ex);
        }
      }
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionMessage_UNCAUGHT_EXCEPTION_PROCESSING__0, this), t);
        // I don't believe this ever happens (DJP May 2007)
        throw new InternalGemFireException(LocalizedStrings.DistributionMessage_UNEXPECTED_ERROR_SCHEDULING_MESSAGE.toLocalizedString(), t);
      }
    } // not inline
  }

  protected boolean mayAddToMultipleSerialGateways(DistributionManager dm) {
    // subclasses should override this method if processing
    // them may add to multiple serial gateways.
    return false;
  }

  /**
   * returns true if the current thread should not be used for inline
   * processing.  i.e., it is a "precious" resource
   */
  public static boolean isPreciousThread() {
    String thrname = Thread.currentThread().getName();
    //return thrname.startsWith("Geode UDP");
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
      if (Thread.currentThread().getName().startsWith("P2P Message Reader")) {
        sender = procId;
      }
      else {
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
        Breadcrumbs.setSendSide(procId 
            + " recipients="+Arrays.toString(this.recipients));
      }
      else {
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
   * This method resets the state of this message, usually releasing
   * objects and resources it was using.  It is invoked after the
   * message has been sent.  Note that classes that override this
   * method should always invoke the inherited method
   * (<code>super.reset()</code>).
   */
  public void reset() {
    resetRecipients();
    this.sender = null;
  }

  /**
   * Writes the contents of this <code>DistributionMessage</code> to
   * the given output.
   * Note that classes that
   * override this method should always invoke the inherited method
   * (<code>super.toData()</code>).
   */
  public void toData(DataOutput out) throws IOException {
//     DataSerializer.writeObject(this.recipients, out); // no need to serialize; filled in later
    //((IpAddress)this.sender).toData(out); // no need to serialize; filled in later
    //out.writeLong(this.timeStamp);
  }

  /**
   * Reads the contents of this <code>DistributionMessage</code> from
   * the given input.
   * Note that classes that override this
   * method should always invoke the inherited method
   * (<code>super.fromData()</code>).
   */
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {

//     this.recipients = (Set)DataSerializer.readObject(in); // no to deserialize; filled in later
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

  public void setBytesRead(int bytesRead)
  {
    this.bytesRead = bytesRead;
  }

  public int getBytesRead()
  {
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
   * @return null if message is not conflatable. Otherwise return
   * a key that can be used to identify the entry to conflate.
   * @since 4.2.2
   */
  public ConflationKey getConflationKey() {
    return null; // by default conflate nothing; override in subclasses
  }

  /**
   * @return the ID of the reply processor for this message, or zero if none
   * @since 5.7
   */
  public int getProcessorId() {
    return 0;
  }
  
  /**
   * Severe alert processing enables suspect processing at the ack-wait-threshold
   * and issuing of a severe alert at the end of the ack-severe-alert-threshold.
   * Some messages should not support this type of processing
   * (e.g., GII, or DLockRequests)
   * @return whether severe-alert processing may be performed on behalf
   * of this message
   */
  public boolean isSevereAlertCompatible() {
    return false;
  }
  
  /**
   * Returns true if the message is for internal-use such as a meta-data region.
   * 
   * @return true if the message is for internal-use such as a meta-data region
   * @since 7.0
   */
  public boolean isInternal() {
    return false;
  }
  
  /**
   * does this message carry state that will alter the content of
   * one or more cache regions?  This is used to track the
   * flight of content changes through communication channels
   */
  public boolean containsRegionContentChange() {
    return false;
  }

  /** returns the class name w/o package information.  useful in logging */
  public String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length()+1);
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

  public Version[] getSerializationVersions() {
    return null;
  }
}
