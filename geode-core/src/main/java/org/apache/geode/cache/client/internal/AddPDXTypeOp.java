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

import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.pdx.internal.PdxType;

/**
 * Add a PdxType to a server.
 *
 * @since GemFire 6.6
 */
public class AddPDXTypeOp {
  /**
   * Register a bunch of instantiators on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   */
  public static void execute(ExecutablePool pool, int id, PdxType type) {
    AbstractOp op = new AddPDXTypeOpImpl(id, type);
    pool.execute(op);
  }

  private AddPDXTypeOp() {
    // no instances allowed
  }

  private static class AddPDXTypeOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public AddPDXTypeOpImpl(int id, PdxType type) {
      super(MessageType.ADD_PDX_TYPE, 2);
      getMessage().addObjPart(type);
      getMessage().addIntPart(id);
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      processAck(msg, "addPDXType");
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startAddPdxType();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endAddPdxTypeSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endAddPdxType(start, hasTimedOut(), hasFailed());
    }

    // Don't send the transaction id for this message type.
    @Override
    protected boolean participateInTransaction() {
      return false;
    }

  }
}
