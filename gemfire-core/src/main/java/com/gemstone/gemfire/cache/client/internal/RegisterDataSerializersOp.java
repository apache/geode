/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.internal.InternalDataSerializer.SerializerAttributesHolder;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.util.BlobHelper;

public class RegisterDataSerializersOp {

  public static void execute(ExecutablePool pool,
      DataSerializer[] dataSerializers, EventID eventId) {
    AbstractOp op = new RegisterDataSerializersOpImpl(dataSerializers,
        eventId);
    pool.execute(op);
  }
  
  public static void execute(ExecutablePool pool,
      SerializerAttributesHolder[] holders, EventID eventId) {
    AbstractOp op = new RegisterDataSerializersOpImpl(holders,
        eventId);
    pool.execute(op);
  }
  
  private RegisterDataSerializersOp() {
    // no instances allowed
  }
  
  private static class RegisterDataSerializersOpImpl extends AbstractOp {

    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public RegisterDataSerializersOpImpl(DataSerializer[] dataSerializers,
        EventID eventId) {
      super(MessageType.REGISTER_DATASERIALIZERS, dataSerializers.length * 2 + 1);
      for(int i = 0; i < dataSerializers.length; i++) {
        DataSerializer dataSerializer = dataSerializers[i];
         // strip '.class' off these class names
        String className = dataSerializer.getClass().toString().substring(6);
        try {
          getMessage().addBytesPart(BlobHelper.serializeToBlob(className));
        } catch (IOException ex) {
          throw new SerializationException("failed serializing object", ex);
        }
        getMessage().addIntPart(dataSerializer.getId());
      }
      getMessage().addBytesPart(eventId.calcBytes());
      // // CALLBACK FOR TESTING PURPOSE ONLY ////
      if (PoolImpl.IS_INSTANTIATOR_CALLBACK) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeSendingToServer(eventId);
      }
   }
    
    /**
     * @throws SerializationException
     *           Thrown when serialization fails.
     */
    public RegisterDataSerializersOpImpl(SerializerAttributesHolder[] holders,
        EventID eventId) {
      super(MessageType.REGISTER_DATASERIALIZERS, holders.length * 2 + 1);
      for (int i = 0; i < holders.length; i++) {
        try {
          getMessage().addBytesPart(
              BlobHelper.serializeToBlob(holders[i].getClassName()));
        } catch (IOException ex) {
          throw new SerializationException("failed serializing object", ex);
        }
        getMessage().addIntPart(holders[i].getId());
      }
      getMessage().addBytesPart(eventId.calcBytes());
      // // CALLBACK FOR TESTING PURPOSE ONLY ////
      if (PoolImpl.IS_INSTANTIATOR_CALLBACK) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeSendingToServer(eventId);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "registerDataSerializers");
      return null;
    }
    
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startRegisterDataSerializers();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endRegisterDataSerializersSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endRegisterDataSerializers(start, hasTimedOut(), hasFailed());
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
  }
}
