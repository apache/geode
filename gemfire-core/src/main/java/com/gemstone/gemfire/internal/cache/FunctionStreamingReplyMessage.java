/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.NotSerializableException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

public class FunctionStreamingReplyMessage extends ReplyMessage{
  private static final Logger logger = LogService.getLogger();
  
  /** the number of this message */
  protected int msgNum;
  
  /** whether this message is the last one in this series */
  protected boolean lastMsg;
  
  protected Object result;
  
  /**
   * @param msgNum message number in this series (0-based)
   * @param lastMsg if this is the last message in this series
   */
  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, DM dm, Object result, int msgNum,
      boolean lastMsg) {
    FunctionStreamingReplyMessage m = new FunctionStreamingReplyMessage() ;
    m.processorId = processorId;
    if (exception != null) {
      m.setException(exception);
      if (logger.isDebugEnabled()) {
        logger.debug("Replying with exception: {}", m, exception);
      }
    }
    m.setRecipient(recipient);
    m.msgNum = msgNum;
    m.lastMsg = lastMsg;
    m.result = result;
    dm.putOutgoing(m);
  } 
   
  public int getMessageNumber() {
    return this.msgNum;
  }
  
  public boolean isLastMessage() {
    return this.lastMsg;
  }
  
  public Object getResult() {
    return this.result;
  }
  
  @Override
  public int getDSFID() {
    return FUNCTION_STREAMING_REPLY_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.msgNum = in.readInt();
    this.lastMsg = in.readBoolean();
    this.processorId = in.readInt();
    try {
      this.result = DataSerializer.readObject(in);
    }
    catch (Exception e) { // bug fix 40670
      // Seems odd to throw a NonSerializableEx when it has already been
      // serialized and we are failing because we can't deserialize.
      NotSerializableException ioEx = new NotSerializableException();
      ioEx.initCause(e);
      throw ioEx;
    }
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.msgNum);
    out.writeBoolean(this.lastMsg); 
    out.writeInt(this.processorId);
    
    //soubhik. fix for ticket 40670
    try {
      DataSerializer.writeObject(this.result, out);
    } 
    catch(Exception ex) {
      if (ex instanceof CancelException) {
        throw new DistributedSystemDisconnectedException(ex);
      }
      NotSerializableException ioEx = new NotSerializableException(this.result
          .getClass().getName());
      ioEx.initCause(ex);
      throw ioEx;
    }
  }

  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    buff.append(getClass().getName());
    buff.append("(processorId=");
    buff.append(this.processorId);
    buff.append(" from ");
    buff.append(this.getSender());
    ReplyException ex = this.getException();
    if (ex != null) {
      buff.append(" with exception ");
      buff.append(ex);
    }
    buff.append(";msgNum ");
    buff.append(this.msgNum);
    buff.append(";lastMsg=");
    buff.append(this.lastMsg);
    buff.append(")");
    return buff.toString();
  }
}
