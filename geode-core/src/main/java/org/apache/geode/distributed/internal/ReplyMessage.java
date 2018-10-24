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

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.logging.LogService;

/**
 * A message that acknowledges that an operation completed successfully, or threw a CacheException.
 * Note that even though this message has a <code>processorId</code>, it is not a
 * {@link MessageWithReply} because it is sent in <b>reply</b> to another message.
 *
 *
 */
public class ReplyMessage extends HighPriorityDistributionMessage {
  private static final Logger logger = LogService.getLogger();

  /** The shared obj id of the ReplyProcessor */
  protected int processorId;

  protected boolean ignored = false;

  protected boolean closed = false;

  private boolean returnValueIsException;

  private Object returnValue;

  private transient boolean sendViaJGroups;

  protected transient boolean internal;

  public void setProcessorId(int id) {
    this.processorId = id;
  }

  @Override
  public boolean sendViaUDP() {
    return this.sendViaJGroups;
  }

  public void setException(ReplyException ex) {
    this.returnValue = ex;
    this.returnValueIsException = true;
  }

  public void setReturnValue(Object o) {
    this.returnValue = o;
    this.returnValueIsException = false;
  }

  /** ReplyMessages are always processed in-line, though subclasses are not */
  @Override
  public boolean getInlineProcess() {
    return this.getClass().equals(ReplyMessage.class);
  }

  /** Send an ack */
  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, ReplySender dm) {
    send(recipient, processorId, exception, dm, false);
  }

  /** Send an ack */
  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, ReplySender dm, boolean internal) {
    Assert.assertTrue(recipient != null, "Sending a ReplyMessage to ALL");
    ReplyMessage m = new ReplyMessage();

    m.processorId = processorId;
    if (exception != null) {
      m.returnValue = exception;
      m.returnValueIsException = true;
    }
    if (exception != null && logger.isDebugEnabled()) {
      Throwable cause = exception.getCause();
      if (cause instanceof EntryNotFoundException) {
        logger.debug("Replying with entry-not-found: {}", exception.getCause().getMessage());
      } else if (cause instanceof ConcurrentCacheModificationException) {
        logger.debug("Replying with concurrent-modification-exception");
      } else if (cause instanceof CancelException) {
        // no need to log this - it will show up in normal debug-level logs when the reply is sent
      } else {
        logger.debug("Replying with exception: " + m, exception);
      }
    }
    m.setRecipient(recipient);
    dm.putOutgoing(m);
  }

  /** Send an ack */
  public static void send(InternalDistributedMember recipient, int processorId, Object returnValue,
      ReplySender dm) {
    Assert.assertTrue(recipient != null, "Sending a ReplyMessage to ALL");
    ReplyMessage m = new ReplyMessage();

    m.processorId = processorId;
    if (returnValue != null) {
      m.returnValue = returnValue;
      m.returnValueIsException = false;
    }
    m.setRecipient(recipient);
    dm.putOutgoing(m);
  }

  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, ReplySender dm, boolean ignored, boolean closed,
      boolean sendViaJGroups) {
    send(recipient, processorId, exception, dm, ignored, false, false, false);
  }

  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, ReplySender dm, boolean ignored, boolean closed,
      boolean sendViaJGroups, boolean internal) {
    Assert.assertTrue(recipient != null, "Sending a ReplyMessage to ALL");
    ReplyMessage m = new ReplyMessage();

    m.processorId = processorId;
    m.ignored = ignored;
    if (exception != null) {
      m.returnValue = exception;
      m.returnValueIsException = true;
    }
    m.closed = closed;
    m.sendViaJGroups = sendViaJGroups;
    if (logger.isDebugEnabled()) {
      if (exception != null && ignored) {
        if (exception.getCause() instanceof InvalidDeltaException) {
          logger.debug("Replying with invalid-delta: {}", exception.getCause().getMessage());
        } else {
          logger.debug("Replying with ignored=true and exception: {}", m, exception);
        }
      } else if (exception != null) {
        if (exception.getCause() != null
            && (exception.getCause() instanceof EntryNotFoundException)) {
          logger.debug("Replying with entry-not-found: {}", exception.getCause().getMessage());
        } else if (exception.getCause() != null
            && (exception.getCause() instanceof ConcurrentCacheModificationException)) {
          logger.debug("Replying with concurrent-modification-exception");
        } else {
          logger.debug("Replying with exception: {}", m, exception);
        }
      } else if (ignored) {
        logger.debug("Replying with ignored=true: {}", m);
      }
    }

    m.setRecipient(recipient);
    dm.putOutgoing(m);
  }



  /**
   * Processes this message. This method is invoked by the receiver of the message if the message is
   * not direct ack. If the message is a direct ack, the process(dm, ReplyProcessor) method is
   * invoked instead.
   *
   * @param dm the distribution manager that is processing the message.
   */
  @Override
  protected void process(final ClusterDistributionManager dm) {
    dmProcess(dm);
  }

  public void dmProcess(final DistributionManager dm) {
    final long startTime = getTimestamp();
    ReplyProcessor21 processor = ReplyProcessor21.getProcessor(processorId);
    try {
      this.process(dm, processor);

      if (DistributionStats.enableClockStats) {
        dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
      }
    } catch (RuntimeException ex) {
      if (processor != null) {
        processor.cancel(getSender(), ex);
      }
      throw ex;
    }
  }

  public void process(final DistributionManager dm, ReplyProcessor21 processor) {
    if (processor == null)
      return;
    processor.process(ReplyMessage.this);
  }

  public Object getReturnValue() {
    if (this.returnValueIsException) {
      return null;
    } else {
      return this.returnValue;
    }
  }

  public ReplyException getException() {
    if (this.returnValueIsException) {
      ReplyException exception = (ReplyException) this.returnValue;
      if (exception != null) {
        InternalDistributedMember sendr = getSender();
        if (sendr != null) {
          exception.setSenderIfNull(sendr);
        }
      }
      return exception;
    } else {
      return null;
    }
  }

  public boolean getIgnored() {
    return this.ignored;
  }

  public boolean getClosed() {
    return this.closed;
  }

  ////////////////////// Utility Methods //////////////////////

  public int getDSFID() {
    return REPLY_MESSAGE;
  }

  // 8 bits in a byte

  // keeping this consistent with HAS_PROCESSOR_ID in PartitionMessage etc
  public static final byte PROCESSOR_ID_FLAG = 0x01;
  public static final byte IGNORED_FLAG = 0x02;
  public static final byte EXCEPTION_FLAG = 0x04;
  public static final byte CLOSED_FLAG = 0x08;
  public static final byte HAS_TX_CHANGES = 0x10;
  public static final byte TIME_STATS_SET = 0x20;
  public static final byte OBJECT_FLAG = 0x40;
  public static final byte INTERNAL_FLAG = (byte) 0x80;

  private static boolean testFlag(byte status, byte flag) {
    return (status & flag) != 0;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);

    byte status = 0;
    if (this.ignored) {
      status |= IGNORED_FLAG;
    }
    if (this.returnValueIsException) {
      status |= EXCEPTION_FLAG;
    } else if (this.returnValue != null) {
      status |= OBJECT_FLAG;
    }
    if (this.processorId != 0) {
      status |= PROCESSOR_ID_FLAG;
    }
    if (this.closed) {
      status |= CLOSED_FLAG;
    }
    if (this.internal) {
      status |= INTERNAL_FLAG;
    }
    out.writeByte(status);
    if (this.processorId != 0) {
      out.writeInt(processorId);
    }
    if (this.returnValueIsException || this.returnValue != null) {
      DataSerializer.writeObject(this.returnValue, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    byte status = in.readByte();
    this.ignored = testFlag(status, IGNORED_FLAG);
    this.closed = testFlag(status, CLOSED_FLAG);
    if (testFlag(status, PROCESSOR_ID_FLAG)) {
      this.processorId = in.readInt();
    }
    if (testFlag(status, EXCEPTION_FLAG)) {
      this.returnValue = DataSerializer.readObject(in);
      this.returnValueIsException = true;
    } else if (testFlag(status, OBJECT_FLAG)) {
      this.returnValue = DataSerializer.readObject(in);
    }
    this.internal = testFlag(status, INTERNAL_FLAG);
  }

  protected StringBuilder getStringBuilder() {
    StringBuilder sb = new StringBuilder();
    sb.append(getShortClassName());
    sb.append(" processorId=");
    sb.append(this.processorId);
    sb.append(" from ");
    sb.append(this.getSender());
    ReplyException ex = getException();
    if (ex != null) {
      if (ex.getCause() != null && ex.getCause() instanceof InvalidDeltaException) {
        sb.append(" with request for full value");
      } else {
        sb.append(" with exception ");
        sb.append(ex);
      }
    }
    return sb;
  }

  public boolean isInternal() {
    return this.internal;
  }

  @Override
  public String toString() {
    return getStringBuilder().toString();
  }
}
