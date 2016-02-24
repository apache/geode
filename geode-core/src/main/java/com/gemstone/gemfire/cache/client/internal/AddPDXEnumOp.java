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

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.pdx.internal.EnumInfo;

/**
 * Push a PDX Enum id to other servers.
 * @since 6.6.2
 */
public class AddPDXEnumOp {
  /**
   * Register a bunch of instantiators on a server
   * using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   */
  public static void execute(ExecutablePool pool, int id,
                             EnumInfo ei)
  {
    AbstractOp op = new AddPdxEnumOpImpl(id, ei);
    pool.execute(op);;
  }
                                                               
  private AddPDXEnumOp() {
    // no instances allowed
  }
  
  private static class AddPdxEnumOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public AddPdxEnumOpImpl(int id, EnumInfo ei) {
      super(MessageType.ADD_PDX_ENUM, 2);
      getMessage().addObjPart(ei);
      getMessage().addIntPart(id);
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "addPDXEnum");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startAddPdxType(); /* use the addPdxType stats instead of adding more stats */
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endAddPdxTypeSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endAddPdxType(start, hasTimedOut(), hasFailed());
    }
    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }
    @Override
    protected boolean needsUserId() {
      return false;
    }
    //Don't send the transaction id for this message type.
    @Override
    protected boolean participateInTransaction() {
      return false;
    }
    
    //TODO - no idea what this mumbo jumbo means, but it's on
    //most of the other messages like this.
    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
      getMessage().send(false);
    }
  }
}
