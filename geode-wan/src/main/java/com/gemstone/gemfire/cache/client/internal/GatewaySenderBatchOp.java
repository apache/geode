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
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.wan.BatchException70;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventRemoteDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventRemoteDispatcher.GatewayAck;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Logger;

public class GatewaySenderBatchOp {
  
  private static final Logger logger = LogService.getLogger();
  
  /**
   * Send a list of gateway events to a server to execute
   * using connections from the given pool
   * to communicate with the server.
   * @param con the connection to send the message on.
   * @param pool the pool to use to communicate with the server.
   * @param events list of gateway events
   * @param batchId the ID of this batch
   * @param removeFromQueueOnException true if the events should be processed even after some exception
   */
  public static void executeOn(Connection con, ExecutablePool pool, List events, int batchId, boolean removeFromQueueOnException, boolean isRetry)
  {
    AbstractOp op = null;
    //System.out.println("Version: "+con.getWanSiteVersion());
    if (Version.GFE_651.compareTo(con.getWanSiteVersion()) >= 0) {
      op = new GatewaySenderGFEBatchOpImpl(events, batchId, removeFromQueueOnException, con.getDistributedSystemId(), isRetry);
    } else {
      // Default should create a batch of server version (ACCEPTOR.VERSION)
      op = new GatewaySenderGFEBatchOpImpl(events, batchId, removeFromQueueOnException, con.getDistributedSystemId(), isRetry);     
    }
    pool.executeOn(con, op, true/*timeoutFatal*/);
  }
  
  
  public static Object executeOn(Connection con, ExecutablePool pool)
  {
    AbstractOp op = null;
    //System.out.println("Version: "+con.getWanSiteVersion());
    // [sumedh] both cases are now same; why switch-case?
    if (Version.GFE_651.compareTo(con.getWanSiteVersion()) >= 0) {
      op = new GatewaySenderGFEBatchOpImpl();
    } else {
      // Default should create a batch of server version (ACCEPTOR.VERSION)
      op = new GatewaySenderGFEBatchOpImpl();
    }
    return pool.executeOn(con, op, true/*timeoutFatal*/);
  }
                                                               
  private GatewaySenderBatchOp() {
    // no instances allowed
  }
  
  static class GatewaySenderGFEBatchOpImpl extends AbstractOp {
    
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GatewaySenderGFEBatchOpImpl(List events, int batchId, boolean removeFromQueueOnException, int dsId, boolean isRetry)  {
      super(MessageType.GATEWAY_RECEIVER_COMMAND, calcPartCount(events));
      if (isRetry) {
        getMessage().setIsRetry();
      }
      getMessage().addIntPart(events.size());
      getMessage().addIntPart(batchId);
      getMessage().addIntPart(dsId);
      getMessage().addBytesPart(
          new byte[] { removeFromQueueOnException ? (byte)1 : (byte)0 });
      // Add each event
      for (Iterator i = events.iterator(); i.hasNext();) {
        GatewaySenderEventImpl event = (GatewaySenderEventImpl)i.next();
        // Add action
        int action = event.getAction();
        getMessage().addIntPart(action);
        { // Add posDup flag
          byte posDupByte = (byte)(event.getPossibleDuplicate()?0x01:0x00);
          getMessage().addBytesPart(new byte[] {posDupByte});
        }
        if (action >= 0 && action <= 3) {
          // 0 = create
          // 1 = update
          // 2 = destroy
          String regionName = event.getRegionPath();
          EventID eventId = event.getEventId();
          Object key = event.getKey();
          Object callbackArg = event.getSenderCallbackArgument();

          // Add region name
          getMessage().addStringPart(regionName);
          // Add event id
          getMessage().addObjPart(eventId);
          // Add key
          getMessage().addStringOrObjPart(key);
          if (action < 2 /* it is 0 or 1 */) {
            byte[] value = event.getSerializedValue();
            byte valueIsObject = event.getValueIsObject();;
            // Add value (which is already a serialized byte[])
            getMessage().addRawPart(value, (valueIsObject == 0x01));
          }
          // Add callback arg if necessary
          if (callbackArg == null) {
            getMessage().addBytesPart(new byte[] {0x00});
          } else {
            getMessage().addBytesPart(new byte[] {0x01});
            getMessage().addObjPart(callbackArg);
          }
          getMessage().addLongPart(event.getVersionTimeStamp());
        }
      }
    }

    public GatewaySenderGFEBatchOpImpl() {
      super(MessageType.GATEWAY_RECEIVER_COMMAND, 0);
    }

    @Override
    public Object attempt(Connection cnx) throws Exception {
      if (getMessage().getNumberOfParts() == 0) {
        return attemptRead(cnx);
      }
      this.failed = true;
      this.timedOut = false;
      long start = startAttempt(cnx.getStats());
      try {
        try {
          attemptSend(cnx);
          this.failed = false;
        } finally {
          endSendAttempt(cnx.getStats(), start);
        }
      } finally {
        endAttempt(cnx.getStats(), start);
      }
      return this.failed;
    }
    
    private Object attemptRead(Connection cnx) throws Exception {
      this.failed = true;
      try {
        Object result = attemptReadResponse(cnx);
        this.failed = false;
        return result;
      } catch (SocketTimeoutException ste) {
        this.failed = false;
        this.timedOut = true;
        throw ste;
      } catch (Exception e) {
        throw e;
      }
    }
    
    
    /**
     * Attempts to read a response to this operation by reading it from the
     * given connection, and returning it.
     * @param cnx the connection to read the response from
     * @return the result of the operation
     *         or <code>null</code> if the operation has no result.
     * @throws Exception if the execute failed
     */
    protected Object attemptReadResponse(Connection cnx) throws Exception {
      Message msg = createResponseMessage();
      if (msg != null) {
        msg.setComms(cnx.getSocket(), cnx.getInputStream(),
            cnx.getOutputStream(),
            ((ConnectionImpl)cnx).getCommBufferForAsyncRead(), cnx.getStats());
        if (msg instanceof ChunkedMessage) {
          try {
            return processResponse(msg, cnx);
          } finally {
            msg.unsetComms();
            // TODO (ashetkar) Handle the case when we fail to read the
            // connection id.
            processSecureBytes(cnx, msg);
          }
        }

        try {
          msg.recv();
        } finally {
          msg.unsetComms();
          processSecureBytes(cnx, msg);
        }
        return processResponse(msg, cnx);
      }
      
      return null;
    }
    
    
    private static int calcPartCount(List events) {
      int numberOfParts = 4; // for the number of events and the batchId
      for (Iterator i = events.iterator(); i.hasNext();) {
        GatewaySenderEventImpl event = (GatewaySenderEventImpl)i.next();
        numberOfParts += event.getNumberOfParts();
      }
      return numberOfParts;
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      GatewayAck ack = null;
      try {
        // Read the header which describes the type of message following
        switch (msg.getMessageType()) {
        case MessageType.REPLY:
          // Read the chunk
          int batchId = msg.getPart(0).getInt();
          int numEvents = msg.getPart(1).getInt();
          ack = new GatewayAck(batchId, numEvents);
          break;
        case MessageType.EXCEPTION:
          Part part0 = msg.getPart(0);

          Object obj = part0.getObject();
          if (obj instanceof List) {
            List<BatchException70> l = (List<BatchException70>)part0.getObject();

            if (logger.isDebugEnabled()) {
              logger.debug("We got an exception from the GatewayReceiver. MessageType : {} obj :{}", msg.getMessageType(), obj);
            }
            // don't throw Exception but set it in the Ack
            BatchException70 be = new BatchException70(l);
            ack = new GatewayAck(be, l.get(0).getBatchId());

          } else if (obj instanceof Throwable) {
            String s = ": While reading Ack from receiver "
                + ((Throwable)obj).getMessage();
            throw new ServerOperationException(s, (Throwable)obj);
          }
          break;
        default:
          throw new InternalGemFireError(
              LocalizedStrings.Op_UNKNOWN_MESSAGE_TYPE_0
                  .toLocalizedString(Integer.valueOf(msg.getMessageType())));
        }
      } finally {
        msg.clear();
      }
      return ack;
    }
    
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGatewayBatch();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGatewayBatchSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGatewayBatch(start, hasTimedOut(), hasFailed());
    }
    
    @Override
    public boolean isGatewaySenderOp() {
      return true;
    }
  }
}
