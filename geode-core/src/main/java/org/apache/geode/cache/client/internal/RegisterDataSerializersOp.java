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
package org.apache.geode.cache.client.internal;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.DataSerializer;
import org.apache.geode.SerializationException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.InternalDataSerializer.SerializerAttributesHolder;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.util.BlobHelper;

public class RegisterDataSerializersOp {

  public static void execute(ExecutablePool pool, DataSerializer[] dataSerializers,
      EventID eventId) {
    AbstractOp op = new RegisterDataSerializersOpImpl(dataSerializers, eventId);
    pool.execute(op);
  }

  public static void execute(ExecutablePool pool, SerializerAttributesHolder[] holders,
      EventID eventId) {
    AbstractOp op = new RegisterDataSerializersOpImpl(holders, eventId);
    pool.execute(op);
  }

  private RegisterDataSerializersOp() {
    // no instances allowed
  }

  @VisibleForTesting
  public static class RegisterDataSerializersOpImpl extends AbstractOp {

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public RegisterDataSerializersOpImpl(DataSerializer[] dataSerializers, EventID eventId) {
      super(MessageType.REGISTER_DATASERIALIZERS, dataSerializers.length * 2 + 1);
      for (int i = 0; i < dataSerializers.length; i++) {
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
     * @throws SerializationException Thrown when serialization fails.
     */
    public RegisterDataSerializersOpImpl(SerializerAttributesHolder[] holders, EventID eventId) {
      super(MessageType.REGISTER_DATASERIALIZERS, holders.length * 2 + 1);
      for (int i = 0; i < holders.length; i++) {
        try {
          getMessage().addBytesPart(BlobHelper.serializeToBlob(holders[i].getClassName()));
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
    protected Object processResponse(final @NotNull Message msg) throws Exception {
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

  }
}
