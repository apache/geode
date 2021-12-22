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

import org.apache.geode.Instantiator;
import org.apache.geode.SerializationException;
import org.apache.geode.internal.InternalInstantiator.InstantiatorAttributesHolder;
import org.apache.geode.internal.cache.ClientServerObserver;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.util.BlobHelper;

/**
 * Register a bunch of instantiators on a server
 *
 * @since GemFire 5.7
 */
public class RegisterInstantiatorsOp {
  /**
   * Register a bunch of instantiators on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param instantiators the instantiators to register
   * @param eventId the id of this event
   */
  public static void execute(ExecutablePool pool, Instantiator[] instantiators, EventID eventId) {
    AbstractOp op = new RegisterInstantiatorsOpImpl(instantiators, eventId);
    pool.execute(op, Integer.MAX_VALUE);
  }

  /**
   * Register a bunch of instantiators on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param holders the {@link InstantiatorAttributesHolder}s containing info about the
   *        instantiators to register
   * @param eventId the id of this event
   */
  public static void execute(ExecutablePool pool, Object[] holders, EventID eventId) {
    AbstractOp op = new RegisterInstantiatorsOpImpl(holders, eventId);
    pool.execute(op, Integer.MAX_VALUE);
  }

  private RegisterInstantiatorsOp() {
    // no instances allowed
  }

  private static class RegisterInstantiatorsOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public RegisterInstantiatorsOpImpl(Instantiator[] instantiators, EventID eventId) {
      super(MessageType.REGISTER_INSTANTIATORS, instantiators.length * 3 + 1);
      for (Instantiator instantiator : instantiators) {
        // strip '.class' off these class names
        String className = instantiator.getClass().toString().substring(6);
        String instantiatedClassName = instantiator.getInstantiatedClass().toString().substring(6);
        try {
          getMessage().addBytesPart(BlobHelper.serializeToBlob(className));
          getMessage().addBytesPart(BlobHelper.serializeToBlob(instantiatedClassName));
        } catch (IOException ex) {
          throw new SerializationException("failed serializing object", ex);
        }
        getMessage().addIntPart(instantiator.getId());
      }
      getMessage().addBytesPart(eventId.calcBytes());
      // // // CALLBACK FOR TESTING PURPOSE ONLY ////
      if (PoolImpl.IS_INSTANTIATOR_CALLBACK) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeSendingToServer(eventId);
      }
    }

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public RegisterInstantiatorsOpImpl(Object[] holders, EventID eventId) {
      super(MessageType.REGISTER_INSTANTIATORS, holders.length * 3 + 1);
      for (Object obj : holders) {
        String instantiatorClassName = null;
        String instantiatedClassName = null;
        int id = 0;
        if (obj instanceof Instantiator) {
          instantiatorClassName = ((Instantiator) obj).getClass().getName();
          instantiatedClassName = ((Instantiator) obj).getInstantiatedClass().getName();
          id = ((Instantiator) obj).getId();
        } else {
          instantiatorClassName = ((InstantiatorAttributesHolder) obj).getInstantiatorClassName();
          instantiatedClassName = ((InstantiatorAttributesHolder) obj).getInstantiatedClassName();
          id = ((InstantiatorAttributesHolder) obj).getId();
        }
        try {
          getMessage().addBytesPart(BlobHelper.serializeToBlob(instantiatorClassName));
          getMessage().addBytesPart(BlobHelper.serializeToBlob(instantiatedClassName));
        } catch (IOException ex) {
          throw new SerializationException("failed serializing object", ex);
        }
        getMessage().addIntPart(id);
      }
      getMessage().addBytesPart(eventId.calcBytes());
      // // // CALLBACK FOR TESTING PURPOSE ONLY ////
      if (PoolImpl.IS_INSTANTIATOR_CALLBACK) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeSendingToServer(eventId);
      }
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      processAck(msg, "registerInstantiators");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startRegisterInstantiators();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endRegisterInstantiatorsSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endRegisterInstantiators(start, hasTimedOut(), hasFailed());
    }

  }
}
